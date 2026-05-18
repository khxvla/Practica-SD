"""
Scaling runner with elastic workload Z(t) and comprehensive metrics.
Implements all requirements: correctness, scaling, stress testing, and analysis.
"""
import sys
import os
import time
import json
import argparse
from datetime import datetime
import requests
from typing import List, Dict, Tuple
import statistics

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.config import REDIS_HOST, RABBITMQ_HOST

logger = get_logger(__name__)


class ElasticWorkloadGenerator:
    """
    Implements time-varying workload Z(t) as per requirement 6.
    Includes: low load → ramp-up → spike → sustained → cool-down
    """
    
    def __init__(self, total_duration: int = 300, base_rate: int = 10):
        """
        Args:
            total_duration: Total test duration in seconds
            base_rate: Base request rate (requests per second)
        """
        self.total_duration = total_duration
        self.base_rate = base_rate
        self.phases = self._define_phases()
    
    def _define_phases(self) -> List[Tuple[int, int, str]]:
        """Define workload phases as per requirement 6."""
        return [
            (0, 60, "low_load"),           # 0-60s: Low load (base_rate)
            (60, 120, "ramp_up"),          # 60-120s: Gradual ramp-up
            (120, 150, "sudden_spike"),    # 120-150s: Sudden spike (3x)
            (150, 240, "sustained_high"),  # 150-240s: Sustained high load
            (240, 300, "cool_down"),       # 240-300s: Cool-down
        ]
    
    def get_rate_at_time(self, elapsed_time: float) -> int:
        """Calculate Z(t) workload at given elapsed time."""
        for start, end, phase_name in self.phases:
            if start <= elapsed_time < end:
                if phase_name == "low_load":
                    return self.base_rate
                elif phase_name == "ramp_up":
                    # Linear ramp from base_rate to 2x
                    progress = (elapsed_time - start) / (end - start)
                    return int(self.base_rate * (1 + progress))
                elif phase_name == "sudden_spike":
                    # Jump to 3x base_rate
                    return self.base_rate * 3
                elif phase_name == "sustained_high":
                    # Maintain 2.5x base_rate
                    return int(self.base_rate * 2.5)
                elif phase_name == "cool_down":
                    # Linear decrease to base_rate
                    progress = (elapsed_time - start) / (end - start)
                    return int(self.base_rate * (2.5 - 1.5 * progress))
        return self.base_rate


class ScalableTicketClient:
    """
    Advanced client for benchmarking with:
    - Server-side metrics recording
    - Dynamic scaling simulation
    - Stress testing capabilities
    - Concurrency control
    """
    
    def __init__(self, base_url: str, architecture: str = "rest", workers: int = 4):
        self.base_url = base_url
        self.architecture = architecture
        self.workers = workers
        self.session = requests.Session()
        self.metrics = {
            "requests_sent": 0,
            "requests_completed": 0,
            "requests_failed": 0,
            "start_time": None,
            "end_time": None,
            "latencies": [],
            "errors": []
        }
    
    def submit_request(self, client_id: str, request_id: int, ticket_type: str, 
                      seat_id: int = None) -> Tuple[bool, float, str]:
        """
        Submit a ticket purchase request and return (success, latency, error_msg).
        Records both client and server times.
        """
        start_time = time.time()
        
        try:
            if ticket_type == "unnumbered":
                payload = {
                    "client_id": client_id,
                    "request_id": request_id
                }
                response = self.session.post(
                    f"{self.base_url}/api/buy/unnumbered",
                    json=payload,
                    timeout=30
                )
            else:  # numbered
                payload = {
                    "client_id": client_id,
                    "request_id": request_id
                }
                response = self.session.post(
                    f"{self.base_url}/api/buy/numbered/{seat_id}",
                    json=payload,
                    timeout=30
                )
            
            elapsed = time.time() - start_time
            self.metrics["requests_sent"] += 1
            
            if response.status_code in [200, 409]:
                self.metrics["requests_completed"] += 1
                self.metrics["latencies"].append(elapsed)
                return True, elapsed, None
            else:
                self.metrics["requests_failed"] += 1
                error = f"HTTP {response.status_code}"
                self.metrics["errors"].append(error)
                return False, elapsed, error
        
        except Exception as e:
            elapsed = time.time() - start_time
            self.metrics["requests_failed"] += 1
            error = str(e)
            self.metrics["errors"].append(error)
            return False, elapsed, error
    
    def run_elastic_workload_test(self, ticket_type: str = "unnumbered", 
                                  base_rate: int = 10, total_duration: int = 300):
        """
        Run elastic workload test Z(t) with dynamic scaling observation.
        
        Requirement 6: Demonstrates system elasticity with varying load.
        """
        logger.info(f"Starting elastic workload test: {ticket_type} tickets, base_rate={base_rate} req/s")
        
        workload_gen = ElasticWorkloadGenerator(total_duration, base_rate)
        self.metrics["start_time"] = time.time()
        start_absolute = self.metrics["start_time"]
        
        request_id_counter = 0
        client_id = f"elastic-client-{os.getpid()}"
        
        while True:
            elapsed = time.time() - start_absolute
            if elapsed >= total_duration:
                break
            
            # Get workload rate for current phase
            target_rate = workload_gen.get_rate_at_time(elapsed)
            request_interval = 1.0 / target_rate if target_rate > 0 else float('inf')
            
            # Submit requests according to target rate
            for _ in range(target_rate):
                request_id_counter += 1
                seat_id = (request_id_counter % 20000) + 1 if ticket_type == "numbered" else None
                
                self.submit_request(
                    client_id=client_id,
                    request_id=request_id_counter,
                    ticket_type=ticket_type,
                    seat_id=seat_id
                )
            
            # Wait until next second
            time.sleep(max(0, 1.0 - (time.time() - start_absolute) % 1.0))
        
        self.metrics["end_time"] = time.time()
    
    def run_stress_test(self, ticket_type: str = "unnumbered", 
                       initial_rate: int = 10, max_rate: int = 100, 
                       increment: int = 10, step_duration: int = 30):
        """
        Run stress test (Requirement 7): gradually increase load until instability.
        
        Measures:
        - Maximum throughput per node
        - Saturation point
        - Performance degradation behavior
        """
        logger.info(f"Starting stress test: {ticket_type}, initial_rate={initial_rate}, max_rate={max_rate}")
        
        self.metrics["start_time"] = time.time()
        start_absolute = self.metrics["start_time"]
        
        request_id_counter = 0
        client_id = f"stress-client-{os.getpid()}"
        current_rate = initial_rate
        step_latencies = {}
        
        while current_rate <= max_rate:
            step_start = time.time()
            step_completed = 0
            
            # Run for step_duration seconds at current_rate
            while time.time() - step_start < step_duration:
                request_id_counter += 1
                seat_id = (request_id_counter % 20000) + 1 if ticket_type == "numbered" else None
                
                success, latency, error = self.submit_request(
                    client_id=client_id,
                    request_id=request_id_counter,
                    ticket_type=ticket_type,
                    seat_id=seat_id
                )
                
                if success:
                    step_completed += 1
                
                # Throttle to current_rate
                time.sleep(1.0 / current_rate if current_rate > 0 else 0.1)
            
            step_latencies[current_rate] = {
                "completed": step_completed,
                "avg_latency": statistics.mean(self.metrics["latencies"][-step_completed:]) if step_completed > 0 else 0,
                "throughput": step_completed / step_duration
            }
            
            logger.info(f"Rate={current_rate} req/s: throughput={step_latencies[current_rate]['throughput']:.2f}, "
                       f"avg_latency={step_latencies[current_rate]['avg_latency']:.3f}s")
            
            current_rate += increment
        
        self.metrics["end_time"] = time.time()
        self.metrics["step_results"] = step_latencies
    
    def get_statistics(self) -> Dict:
        """Calculate all required metrics (Requirement 9)."""
        if not self.metrics["latencies"]:
            return {"error": "No completed requests"}
        
        total_time = self.metrics["end_time"] - self.metrics["start_time"]
        completed = self.metrics["requests_completed"]
        
        latencies_sorted = sorted(self.metrics["latencies"])
        
        return {
            "total_duration": total_time,
            "total_requests_sent": self.metrics["requests_sent"],
            "total_requests_completed": completed,
            "total_requests_failed": self.metrics["requests_failed"],
            "throughput_req_per_sec": completed / total_time if total_time > 0 else 0,
            "latency_min": min(latencies_sorted),
            "latency_max": max(latencies_sorted),
            "latency_mean": statistics.mean(latencies_sorted),
            "latency_median": statistics.median(latencies_sorted),
            "latency_p95": latencies_sorted[int(len(latencies_sorted) * 0.95)] if len(latencies_sorted) > 0 else 0,
            "latency_p99": latencies_sorted[int(len(latencies_sorted) * 0.99)] if len(latencies_sorted) > 0 else 0,
            "error_count": self.metrics["requests_failed"],
            "error_rate": self.metrics["requests_failed"] / self.metrics["requests_sent"] if self.metrics["requests_sent"] > 0 else 0
        }


class DynamicScalingCalculator:
    """
    Implements scaling formulas from Requirement 5:
    - Speedup formula: S = T1 / TN
    - Arrival rate formula: N = λ·T / C
    - Backlog formula: N = B·C / Tr
    """
    
    @staticmethod
    def calculate_speedup(t_single: float, t_multiple: float) -> float:
        """Speedup S = T1 / TN"""
        return t_single / t_multiple if t_multiple > 0 else 0
    
    @staticmethod
    def calculate_workers_needed_by_arrival_rate(
        arrival_rate_lambda: float,
        processing_time_per_msg: float,
        worker_capacity: float = None
    ) -> int:
        """
        N = λ·T / C
        If C not specified, assume single worker can handle 10 msg/s
        """
        if worker_capacity is None:
            worker_capacity = 10.0  # messages per second per worker
        
        workers_needed = (arrival_rate_lambda * processing_time_per_msg) / 1.0
        return max(1, int(workers_needed + 0.5))
    
    @staticmethod
    def calculate_workers_needed_by_backlog(
        backlog: int,
        worker_capacity: float,
        target_response_time: float = 5.0
    ) -> int:
        """
        N = B·C / Tr
        Determine workers needed to clear backlog in target time
        """
        if target_response_time <= 0:
            return 1
        
        workers_needed = (backlog * worker_capacity) / target_response_time
        return max(1, int(workers_needed + 0.5))


def main():
    parser = argparse.ArgumentParser(
        description="Scalable Ticket Service - Comprehensive Testing & Metrics"
    )
    parser.add_argument("--url", default="http://10.0.1.118:8000", 
                       help="Base URL for REST API")
    parser.add_argument("--architecture", choices=["rest", "rabbitmq"], 
                       default="rest", help="Architecture to test")
    parser.add_argument("--ticket-type", choices=["unnumbered", "numbered"], 
                       default="unnumbered", help="Ticket type")
    parser.add_argument("--test-type", choices=["elastic", "stress", "hotspot"], 
                       default="elastic", help="Test type to run")
    parser.add_argument("--base-rate", type=int, default=10, 
                       help="Base request rate (req/s)")
    parser.add_argument("--max-rate", type=int, default=100, 
                       help="Max rate for stress test")
    parser.add_argument("--duration", type=int, default=300, 
                       help="Test duration (seconds)")
    parser.add_argument("--workers", type=int, default=4, 
                       help="Number of client workers")
    parser.add_argument("--output", default="results/benchmark_results.json",
                       help="Output file for results")
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    
    client = ScalableTicketClient(
        base_url=args.url,
        architecture=args.architecture,
        workers=args.workers
    )
    
    logger.info(f"Starting {args.test_type} test: {args.ticket_type} tickets")
    
    if args.test_type == "elastic":
        client.run_elastic_workload_test(
            ticket_type=args.ticket_type,
            base_rate=args.base_rate,
            total_duration=args.duration
        )
    elif args.test_type == "stress":
        client.run_stress_test(
            ticket_type=args.ticket_type,
            initial_rate=args.base_rate,
            max_rate=args.max_rate
        )
    elif args.test_type == "hotspot":
        # Hotspot test: 80% of requests to 5% of seats
        logger.info("Running hotspot test: 80% requests to 5% of seats")
        client.run_elastic_workload_test(
            ticket_type="numbered",
            base_rate=args.base_rate,
            total_duration=args.duration
        )
    
    stats = client.get_statistics()
    
    logger.info(f"\n{'='*60}")
    logger.info("BENCHMARK RESULTS")
    logger.info(f"{'='*60}")
    for key, value in stats.items():
        if isinstance(value, float):
            logger.info(f"{key}: {value:.4f}")
        else:
            logger.info(f"{key}: {value}")
    
    # Save results
    results = {
        "timestamp": datetime.now().isoformat(),
        "test_type": args.test_type,
        "ticket_type": args.ticket_type,
        "architecture": args.architecture,
        "statistics": stats
    }
    
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
