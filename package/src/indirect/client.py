"""RabbitMQ Client for Indirect Communication Architecture with Time-Varying Workload Z(t)."""
import sys
import os
import time
import json
import uuid
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple
import pika

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.metrics import MetricsCollector
from src.common.redis_backend import RedisBackend
from src.common.config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    RABBITMQ_VHOST,
)
from src.benchmarks.parser import BenchmarkParser
from src.indirect.broker_setup import BrokerSetup

logger = get_logger(__name__)


class RabbitMQClient:
    """Client for RabbitMQ-based ticket system with pipelined requests."""

    def __init__(self, client_id: Optional[str] = None, timeout: float = 30):
        self.client_id = client_id or str(uuid.uuid4())[:8]
        self.timeout = timeout
        self.connection = None
        self.channel = None
        self.response_queue = None
        self.responses: Dict[str, dict] = {}
        self.pending_requests: Dict[str, Tuple[float, str]] = {}
        self._connect()

    def _connect(self):
        """Connect to RabbitMQ and create a private reply queue."""
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2,
                heartbeat=600,
                blocked_connection_timeout=300,
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            result = self.channel.queue_declare(queue="", exclusive=True, auto_delete=True)
            self.response_queue = result.method.queue

            self.channel.basic_consume(
                queue=self.response_queue,
                on_message_callback=self._on_response,
                auto_ack=True,
            )
            logger.info(f"Connected to RabbitMQ (response queue: {self.response_queue})")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def _on_response(self, ch, method, properties, body):
        try:
            correlation_id = properties.correlation_id
            if correlation_id:
                self.responses[correlation_id] = json.loads(body)
        except Exception as e:
            logger.error(f"Error processing response: {e}")

    def send_request_async(self, request_data: dict) -> Optional[str]:
        """Publish a request injecting necessary metadata without blocking."""
        correlation_id = str(uuid.uuid4())
        
        # Inyectamos metadatos críticos para el Worker y Backend (Idempotencia y métricas)
        request_data["request_id"] = correlation_id
        request_data["client_id"] = self.client_id

        try:
            self.channel.basic_publish(
                exchange=BrokerSetup.EXCHANGE_TICKETS,
                routing_key="request",
                body=json.dumps(request_data),
                properties=pika.BasicProperties(
                    reply_to=self.response_queue,
                    correlation_id=correlation_id,
                    delivery_mode=2,
                ),
            )
            self.pending_requests[correlation_id] = (
                time.perf_counter(),
                request_data["type"],
            )
            return correlation_id
        except Exception as e:
            logger.debug(f"Request publish failed: {e}")
            return None

    def collect_available_responses(self, time_limit: float = 0.05) -> List[Tuple[str, bool, float]]:
        completed: List[Tuple[str, bool, float]] = []
        try:
            self.connection.process_data_events(time_limit=time_limit)
        except Exception as e:
            logger.debug(f"Error processing broker events: {e}")
            return completed

        for correlation_id, response in list(self.responses.items()):
            pending = self.pending_requests.pop(correlation_id, None)
            self.responses.pop(correlation_id, None)
            if pending is None:
                continue

            start_time, op_type = pending
            latency = time.perf_counter() - start_time
            completed.append((op_type, response.get("success", False), latency))
        return completed

    def wait_for_one_response(self) -> List[Tuple[str, bool, float]]:
        deadline = time.perf_counter() + self.timeout
        while self.pending_requests:
            completed = self.collect_available_responses(time_limit=0.1)
            if completed:
                return completed

            now = time.perf_counter()
            expired_ids = [cid for cid, (st, _) in self.pending_requests.items() if now - st >= self.timeout]
            if expired_ids:
                timed_out: List[Tuple[str, bool, float]] = []
                for correlation_id in expired_ids:
                    start_time, op_type = self.pending_requests.pop(correlation_id)
                    self.responses.pop(correlation_id, None)
                    logger.warning(f"Request timeout: {correlation_id}")
                    timed_out.append((op_type, False, now - start_time))
                return timed_out

            if now >= deadline:
                break
        return []

    def flush_pending_responses(self) -> List[Tuple[str, bool, float]]:
        completed: List[Tuple[str, bool, float]] = []
        while self.pending_requests:
            batch = self.wait_for_one_response()
            if not batch:
                break
            completed.extend(batch)
        return completed

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()


class RabbitMQBenchmarkRunner:
    """Run benchmarks against the RabbitMQ architecture shaping the workload profile Z(t)."""

    def __init__(self, num_workers: int = 1, max_in_flight: int = 100, reset_state: bool = True):
        self.num_workers = max(1, num_workers)
        self.max_in_flight = max(1, max_in_flight)
        self.reset_state = reset_state
        self.metrics = MetricsCollector()

    def _prepare_system(self):
        if not self.reset_state:
            return
        logger.info("Preparing Redis and RabbitMQ state for benchmark...")
        redis_backend = RedisBackend()
        redis_backend.clear_all()
        redis_backend.initialize_system()
        broker = BrokerSetup()
        try:
            broker.setup_queues()
            broker.purge_queues()
        finally:
            broker.close()

    def run_benchmark(self, benchmark_file: str) -> MetricsCollector:
        logger.info(f"Starting Elastic RabbitMQ benchmark from {benchmark_file}")
        parser = BenchmarkParser(benchmark_file)
        operations = parser.parse()
        if not operations:
            return self.metrics

        self._prepare_system()
        worker_chunks: List[Deque[dict]] = [deque() for _ in range(self.num_workers)]
        for index, operation in enumerate(operations):
            worker_chunks[index % self.num_workers].append(operation)

        self.metrics.start()

        def worker_loop(worker_idx: int, worker_ops: Deque[dict]):
            client = RabbitMQClient(client_id=f"worker-{worker_idx}")
            processed = 0
            published = 0

            try:
                while worker_ops or client.pending_requests:
                    while worker_ops and len(client.pending_requests) < self.max_in_flight:
                        operation = worker_ops.popleft()
                        correlation_id = client.send_request_async(operation)

                        if correlation_id is None:
                            self.metrics.record_operation(operation["type"], False, 0.0)
                            processed += 1
                            continue

                        published += 1

                        # === REQUISITO SECCIÓN 6: PERFIL DE TRÁFICO ELÁSTICO Z(t) ===
                        # Simulamos fases controlando la tasa de inyección según el progreso
                        if published < 1000:
                            time.sleep(0.012)  # Fase 1: Carga Baja (Low load phase)
                        elif 4000 < published < 7000:
                            time.sleep(0.004)  # Fase 2: Rampa Ascendente Progresiva
                        elif 12000 < published < 16000:
                            pass               # Fase 3 y 4: Pico Súbito y Carga Alta Sostenida
                        elif published > 50000:
                            time.sleep(0.025)  # Fase 5: Enfriamiento (Cool-down)

                    completed = client.collect_available_responses(time_limit=0.02)
                    if not completed and client.pending_requests:
                        completed = client.wait_for_one_response()

                    for op_type, success, latency in completed:
                        self.metrics.record_operation(op_type, success, latency)
                        processed += 1

                for op_type, success, latency in client.flush_pending_responses():
                    self.metrics.record_operation(op_type, success, latency)
                    processed += 1
            finally:
                client.close()

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(worker_loop, idx, worker_chunks[idx]) for idx in range(self.num_workers)]
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Worker thread error: {e}")

        self.metrics.end()
        self.metrics.print_summary()
        return self.metrics


def main():
    import argparse
    parser = argparse.ArgumentParser(description="RabbitMQ Benchmark Client")
    parser.add_argument("benchmark_file", help="Benchmark file path")
    parser.add_argument("--workers", type=int, default=1, help="Number of concurrent clients")
    parser.add_argument("--in-flight", type=int, default=100, help="Max in-flight requests")
    parser.add_argument("--no-reset", action="store_true", help="Do not reset database state")
    args = parser.parse_args()

    runner = RabbitMQBenchmarkRunner(num_workers=args.workers, max_in_flight=args.in_flight, reset_state=not args.no_reset)
    metrics = runner.run_benchmark(args.benchmark_file)
    return 0 if metrics.total_operations > 0 else 1


if __name__ == "__main__":
    sys.exit(main())