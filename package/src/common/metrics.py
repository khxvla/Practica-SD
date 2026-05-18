"""Metrics collection and analysis."""
import threading
import time
from collections import defaultdict
from src.common.logger import get_logger

logger = get_logger(__name__)

class MetricsCollector:
    """Collect and analyze benchmark metrics."""
    
    def __init__(self):
        """Initialize metrics."""
        self.start_time = None
        self.end_time = None
        self.operations = []  # List of dicts with operation data
        self.request_counts = defaultdict(int)
        self.success_counts = defaultdict(int)
        self.latencies = []
        self._lock = threading.Lock()

    def start(self):
        """Mark start time."""
        self.start_time = time.time()

    def end(self):
        """Mark end time."""
        self.end_time = time.time()

    def record_operation(self, op_type, success, latency):
        """
        Record a single operation.
        
        Args:
            op_type: 'unnumbered' or 'numbered'
            success: True/False
            latency: time in seconds
        """
        with self._lock:
            self.operations.append({
                "type": op_type,
                "success": success,
                "latency": latency,
                "timestamp": time.time()
            })
            self.request_counts[op_type] += 1
            if success:
                self.success_counts[op_type] += 1
            self.latencies.append(latency)

    @property
    def total_time(self):
        """Total execution time in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0

    @property
    def total_operations(self):
        """Total number of operations."""
        return len(self.operations)

    @property
    def throughput(self):
        """Operations per second."""
        if self.total_time > 0:
            return self.total_operations / self.total_time
        return 0

    @property
    def success_rate(self):
        """Percentage of successful operations."""
        if self.total_operations == 0:
            return 0
        successful = sum(1 for op in self.operations if op["success"])
        return (successful / self.total_operations) * 100

    def get_stats_by_type(self, op_type):
        """Get statistics for a specific operation type."""
        ops = [op for op in self.operations if op["type"] == op_type]
        if not ops:
            return None
        
        successful = sum(1 for op in ops if op["success"])
        latencies = [op["latency"] for op in ops]
        latencies.sort()
        
        return {
            "type": op_type,
            "total": len(ops),
            "successful": successful,
            "failed": len(ops) - successful,
            "success_rate": (successful / len(ops)) * 100 if ops else 0,
            "avg_latency": sum(latencies) / len(latencies) if latencies else 0,
            "min_latency": min(latencies) if latencies else 0,
            "max_latency": max(latencies) if latencies else 0,
            "p50_latency": latencies[len(latencies) // 2] if latencies else 0,
            "p95_latency": latencies[int(len(latencies) * 0.95)] if latencies else 0,
            "p99_latency": latencies[int(len(latencies) * 0.99)] if latencies else 0,
        }

    def print_summary(self):
        """Print summary statistics."""
        logger.info("=" * 60)
        logger.info("BENCHMARK SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total time: {self.total_time:.2f}s")
        logger.info(f"Total operations: {self.total_operations}")
        logger.info(f"Throughput: {self.throughput:.2f} ops/sec")
        logger.info(f"Success rate: {self.success_rate:.2f}%")
        
        for op_type in sorted(self.request_counts.keys()):
            stats = self.get_stats_by_type(op_type)
            if stats:
                logger.info(f"\n{op_type.upper()} Tickets:")
                logger.info(f"  Total: {stats['total']}")
                logger.info(f"  Successful: {stats['successful']}")
                logger.info(f"  Failed: {stats['failed']}")
                logger.info(f"  Success rate: {stats['success_rate']:.2f}%")
                logger.info(f"  Avg latency: {stats['avg_latency']:.4f}s")
                logger.info(f"  P50 latency: {stats['p50_latency']:.4f}s")
                logger.info(f"  P95 latency: {stats['p95_latency']:.4f}s")
                logger.info(f"  P99 latency: {stats['p99_latency']:.4f}s")
        logger.info("=" * 60)

    def to_dict(self):
        """Export metrics as dictionary."""
        return {
            "total_time": self.total_time,
            "total_operations": self.total_operations,
            "throughput": self.throughput,
            "success_rate": self.success_rate,
            "by_type": {
                op_type: self.get_stats_by_type(op_type)
                for op_type in self.request_counts
            }
        }
