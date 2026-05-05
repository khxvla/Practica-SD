"""Configuration module for the ticket system."""
import os


def _split_urls(raw_value: str) -> list[str]:
    """Parse a comma-separated URL list into normalized backend URLs."""
    return [url.strip().rstrip("/") for url in raw_value.split(",") if url.strip()]


# System parameters
TOTAL_TICKETS = 20000

# AWS delivery topology:
# - worker-server: private 10.0.1.118, public 52.91.229.23
#   Hosts Redis, direct REST servers, direct load balancer and indirect workers.
# - rabbitmq-server: private 10.0.1.106, public 34.201.110.76
#   Hosts the RabbitMQ broker.
# Benchmarks run from client instances inside the same VPC, so private IPs are
# the stable defaults for cross-instance communication.
WORKER_SERVER_PRIVATE_IP = "10.0.1.118"
RABBITMQ_SERVER_PRIVATE_IP = "10.0.1.106"

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", WORKER_SERVER_PRIVATE_IP)
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", RABBITMQ_SERVER_PRIVATE_IP)
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

# REST configuration
REST_HOST = os.getenv("REST_HOST", "0.0.0.0")
REST_PORT = int(os.getenv("REST_PORT", 5000))
REST_BENCHMARK_URL = os.getenv(
    "REST_BENCHMARK_URL",
    f"http://{WORKER_SERVER_PRIVATE_IP}:8000",
).rstrip("/")
REST_CLIENT_URL = os.getenv("REST_CLIENT_URL", REST_BENCHMARK_URL).rstrip("/")
REST_BACKEND_URLS = _split_urls(
    os.getenv(
        "REST_BACKEND_URLS",
        "http://127.0.0.1:5001,http://127.0.0.1:5002,http://127.0.0.1:5003",
    )
)

# Benchmark configuration
BENCHMARK_DIR = os.getenv("BENCHMARK_DIR", "benchmarks")

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
