"""Simple round-robin REST load balancer for the direct architecture.

This provides a single REST entry point without depending on NGINX for local
or AWS runs. Requests are proxied to the configured backend REST servers using
HTTP keep-alive pools.
"""
import os
import sys
import threading
from typing import Iterable, List, Optional

import requests
from flask import Flask, Response, jsonify, request
from requests.adapters import HTTPAdapter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.config import REST_BACKEND_URLS

logger = get_logger(__name__)
app = Flask(__name__)


class RoundRobinLoadBalancer:
    """Proxy requests to backend REST servers using round-robin selection."""

    def __init__(self, backends: Iterable[str], timeout: float = 10.0):
        self.backends: List[str] = [backend.rstrip("/") for backend in backends]
        if not self.backends:
            raise ValueError("At least one backend must be provided")

        self.timeout = timeout
        self._index = 0
        self._index_lock = threading.Lock()
        self._thread_local = threading.local()

    def _get_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=0)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._thread_local.session = session
        return session

    def _next_backend(self) -> str:
        with self._index_lock:
            backend = self.backends[self._index % len(self.backends)]
            self._index += 1
            return backend

    def _forward_headers(self) -> dict:
        excluded = {
            "connection",
            "content-length",
            "host",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailers",
            "transfer-encoding",
            "upgrade",
        }
        headers = {}
        for key, value in request.headers.items():
            if key.lower() not in excluded:
                headers[key] = value
        headers["X-Forwarded-For"] = request.remote_addr or ""
        headers["X-Forwarded-Proto"] = request.scheme
        return headers

    def _build_response(self, upstream_response: requests.Response) -> Response:
        excluded = {
            "connection",
            "content-encoding",
            "content-length",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailers",
            "transfer-encoding",
            "upgrade",
        }
        response = Response(
            upstream_response.content,
            status=upstream_response.status_code,
        )
        for key, value in upstream_response.headers.items():
            if key.lower() not in excluded:
                response.headers[key] = value
        return response

    def proxy(self, path: str = "") -> Response:
        tried = []
        session = self._get_session()
        headers = self._forward_headers()
        body = request.get_data()

        for _ in range(len(self.backends)):
            backend = self._next_backend()
            tried.append(backend)
            target_url = f"{backend}/{path.lstrip('/')}" if path else backend
            if request.query_string:
                target_url = f"{target_url}?{request.query_string.decode('utf-8')}"

            try:
                upstream_response = session.request(
                    method=request.method,
                    url=target_url,
                    data=body,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=False,
                )
                if upstream_response.status_code >= 500:
                    logger.warning(
                        "Backend %s returned %s for %s %s, retrying next backend",
                        backend,
                        upstream_response.status_code,
                        request.method,
                        request.path,
                    )
                    continue
                return self._build_response(upstream_response)
            except requests.RequestException as exc:
                logger.warning("Proxy error to %s: %s", backend, exc)

        return jsonify({"error": "Backend unavailable", "tried": tried}), 502

    def health(self):
        statuses = []
        healthy = False
        session = self._get_session()

        for backend in self.backends:
            backend_url = f"{backend}/health"
            try:
                response = session.get(backend_url, timeout=min(2.0, self.timeout))
                backend_ok = response.status_code == 200
                healthy = healthy or backend_ok
                statuses.append({"backend": backend, "healthy": backend_ok})
            except requests.RequestException:
                statuses.append({"backend": backend, "healthy": False})

        return healthy, statuses


load_balancer: Optional[RoundRobinLoadBalancer] = None


@app.route("/health", methods=["GET"])
def health():
    """Load balancer health endpoint."""
    if load_balancer is None:
        return jsonify({"status": "error", "message": "Load balancer not initialized"}), 500

    healthy, statuses = load_balancer.health()
    return (
        jsonify({"status": "ok" if healthy else "degraded", "backends": statuses}),
        200 if healthy else 503,
    )


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
def proxy(path: str):
    """Proxy all requests to the configured backend pool."""
    if load_balancer is None:
        return jsonify({"error": "Load balancer not initialized"}), 500
    return load_balancer.proxy(path)


def main(host: str = "0.0.0.0", port: int = 8000, backends: Optional[Iterable[str]] = None, debug: bool = False):
    """Start the load balancer."""
    global load_balancer

    if backends is None:
        backends = REST_BACKEND_URLS

    load_balancer = RoundRobinLoadBalancer(backends)
    logger.info("Starting REST load balancer on %s:%s", host, port)
    logger.info("Backend pool: %s", ", ".join(backends))

    if debug:
        app.run(host=host, port=port, debug=True, threaded=True)
        return

    try:
        from waitress import serve

        serve(app, host=host, port=port, threads=32)
    except ImportError:
        logger.warning("Waitress not available, falling back to Flask development server")
        app.run(host=host, port=port, debug=False, threaded=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="REST load balancer for the ticket system")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument(
        "--backends",
        nargs="+",
        default=REST_BACKEND_URLS,
        help="Backend base URLs",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    args = parser.parse_args()
    main(host=args.host, port=args.port, backends=args.backends, debug=args.debug)
