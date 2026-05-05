"""REST Client for Direct Communication Architecture"""
import sys
import os
import time
import threading
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple
from requests.adapters import HTTPAdapter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.metrics import MetricsCollector
from src.common.config import REST_CLIENT_URL
from src.benchmarks.parser import BenchmarkParser

logger = get_logger(__name__)

class RestClient:
    """Client for REST-based ticket system."""
    
    def __init__(self, base_url=REST_CLIENT_URL, timeout=60, max_retries=5):
        """
        Initialize REST client.
        
        Args:
            base_url: Base URL of the load balancer/server (single entry point)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max(1, max_retries)
        self._thread_local = threading.local()
        
        # Verify connectivity
        if not self._check_health():
            logger.warning(f"Server at {base_url} may not be responding")

    def _get_session(self) -> requests.Session:
        """Return a thread-local session so pooled connections are not shared across threads."""
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._thread_local.session = session
        return session

    def _check_health(self) -> bool:
        """Check if server is healthy."""
        try:
            resp = self._get_session().get(f"{self.base_url}/health", timeout=5)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def _request_with_retries(self, method: str, url: str, **kwargs):
        """Retry transient transport or server-side failures.

        Requests are safe to retry because the direct architecture stores
        results idempotently using client_id + request_id.
        """
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._get_session().request(method=method, url=url, **kwargs)
                if response.status_code < 500:
                    return response

                last_exception = RuntimeError(f"HTTP {response.status_code}")
                logger.debug(
                    "Transient server error %s on %s %s (attempt %s/%s)",
                    response.status_code,
                    method,
                    url,
                    attempt,
                    self.max_retries,
                )
            except requests.RequestException as exc:
                last_exception = exc
                logger.debug(
                    "Transient request error on %s %s (attempt %s/%s): %s",
                    method,
                    url,
                    attempt,
                    self.max_retries,
                    exc,
                )

            if attempt < self.max_retries:
                time.sleep(min(0.05 * attempt, 0.25))

        if last_exception:
            raise last_exception
        raise RuntimeError("Request failed without exception")

    def buy_unnumbered(self, client_id: str, request_id: int) -> Tuple[bool, float, Optional[float]]:
        """
        Buy an unnumbered ticket.
        
        Args:
            client_id: Client identifier
            request_id: Request identifier
        
        Returns:
            Tuple of (success, latency_in_seconds, server_latency_in_seconds)
        """
        start_time = time.time()
        try:
            payload = {
                "client_id": str(client_id),
                "request_id": int(request_id)
            }
            
            resp = self._request_with_retries(
                "POST",
                f"{self.base_url}/api/buy/unnumbered",
                json=payload,
                timeout=self.timeout,
            )
            
            latency = time.time() - start_time
            
            data = resp.json() if resp.content else {}
            server_latency = data.get("server_latency")
            if resp.status_code == 200:
                success = data.get("success", False)
                return success, latency, server_latency
            else:
                return False, latency, server_latency
        
        except Exception as e:
            latency = time.time() - start_time
            logger.debug(f"Request failed: {e}")
            return False, latency, None

    def buy_numbered(self, seat_id: int, client_id: str, request_id: int) -> Tuple[bool, float, Optional[float]]:
        """
        Buy a specific numbered seat.
        
        Args:
            seat_id: Seat number (1-20000)
            client_id: Client identifier
            request_id: Request identifier
        
        Returns:
            Tuple of (success, latency_in_seconds, server_latency_in_seconds)
        """
        start_time = time.time()
        try:
            payload = {
                "client_id": str(client_id),
                "request_id": int(request_id)
            }
            
            resp = self._request_with_retries(
                "POST",
                f"{self.base_url}/api/buy/numbered/{seat_id}",
                json=payload,
                timeout=self.timeout,
            )
            
            latency = time.time() - start_time
            
            data = resp.json() if resp.content else {}
            server_latency = data.get("server_latency")
            if resp.status_code == 200:
                success = data.get("success", False)
                return success, latency, server_latency
            else:
                return False, latency, server_latency
        
        except Exception as e:
            latency = time.time() - start_time
            logger.debug(f"Request failed: {e}")
            return False, latency, None

    def get_stats(self):
        """Get current system statistics."""
        try:
            resp = self._request_with_retries(
                "GET",
                f"{self.base_url}/api/stats",
                timeout=self.timeout,
            )
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return None

    def reset(self):
        """Reset system (admin endpoint)."""
        try:
            resp = self._request_with_retries(
                "POST",
                f"{self.base_url}/api/admin/reset",
                timeout=self.timeout,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Failed to reset: {e}")
            return False


class RestBenchmarkRunner:
    """Run benchmark against REST architecture."""
    
    def __init__(self, base_url=REST_CLIENT_URL, num_workers=1):
        """
        Initialize benchmark runner.
        
        Args:
            base_url: Server base URL
            num_workers: Number of concurrent client threads
        """
        self.client = RestClient(base_url)
        self.num_workers = num_workers
        self.metrics = MetricsCollector()

    def run_benchmark(self, benchmark_file: str) -> MetricsCollector:
        """
        Run benchmark from file.
        
        Args:
            benchmark_file: Path to benchmark file
        
        Returns:
            MetricsCollector with results
        """
        logger.info(f"Starting REST benchmark from {benchmark_file}")
        logger.info(f"Using {self.num_workers} concurrent workers")
        
        # Parse operations
        parser = BenchmarkParser(benchmark_file)
        operations = parser.parse()
        
        if not operations:
            logger.error("No operations parsed")
            return self.metrics
        
        logger.info(f"Parsed {len(operations)} operations")
        logger.info(f"Operation type: {parser.operation_type}")
        
        # Reset system
        logger.info("Resetting system...")
        if not self.client.reset():
            raise RuntimeError("REST reset failed before benchmark start")
        time.sleep(0.5)
        
        # Run benchmark
        self.metrics.start()
        
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            total_ops = len(operations)
            processed = 0

            if parser.operation_type == "unnumbered":
                # Stream operations to avoid queuing all futures in memory
                def gen_unnumbered():
                    for op in operations:
                        yield op["client_id"], op["request_id"]

                for _ in executor.map(lambda args: self._execute_unnumbered(args[0], args[1]), gen_unnumbered()):
                    processed += 1
                    if processed % 1000 == 0:
                        logger.info(f"Processed {processed}/{total_ops} operations")
            else:
                def gen_numbered():
                    for op in operations:
                        yield op["seat_id"], op["client_id"], op["request_id"]

                for _ in executor.map(lambda args: self._execute_numbered(args[0], args[1], args[2]), gen_numbered()):
                    processed += 1
                    if processed % 1000 == 0:
                        logger.info(f"Processed {processed}/{total_ops} operations")

        self.metrics.end()

        # Get final stats
        final_stats = self.client.get_stats()
        logger.info(f"Final Redis stats: {final_stats}")

        # Print summary
        self.metrics.print_summary()

        return self.metrics

    def _execute_unnumbered(self, client_id: str, request_id: int):
        """Execute unnumbered ticket purchase."""
        success, latency, server_latency = self.client.buy_unnumbered(client_id, request_id)
        self.metrics.record_operation("unnumbered", success, latency, server_latency)

    def _execute_numbered(self, seat_id: int, client_id: str, request_id: int):
        """Execute numbered ticket purchase."""
        success, latency, server_latency = self.client.buy_numbered(seat_id, client_id, request_id)
        self.metrics.record_operation("numbered", success, latency, server_latency)


def main():
    """Run benchmark."""
    import argparse
    
    parser = argparse.ArgumentParser(description="REST Benchmark Client")
    parser.add_argument("benchmark_file", help="Benchmark file path")
    parser.add_argument("--url", default=REST_CLIENT_URL, help="Server base URL")
    parser.add_argument("--workers", type=int, default=1, help="Number of concurrent workers")
    
    args = parser.parse_args()
    
    runner = RestBenchmarkRunner(args.url, args.workers)
    metrics = runner.run_benchmark(args.benchmark_file)
    
    return 0 if metrics.total_operations > 0 else 1

if __name__ == "__main__":
    sys.exit(main())
