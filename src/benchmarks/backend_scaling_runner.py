"""Record benchmark runs where the x-axis is the number of backend replicas.

This script is designed for the AWS deployment required by the assignment.
You manually change the number of active backend replicas between runs:

- REST: number of `server.py` processes behind the load balancer
- RabbitMQ: number of active `worker.py` processes

The client-side concurrency stays fixed for every run in the same series.
Each invocation appends or replaces one data point in a persistent results file
and regenerates the plots.
"""
import json
import os
import sys
from typing import List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.config import REST_BENCHMARK_URL
from src.benchmarks.analyzer import BenchmarkAnalyzer
from src.benchmarks.plotter import PlotGenerator

logger = get_logger(__name__)


class BackendScalingSuite:
    """Run one backend-scaling data point and append it to a named series."""

    def __init__(
        self,
        backend_count: int,
        client_workers: int,
        rest_url: str,
        unnumbered_benchmark: str,
        numbered_benchmark: str,
        label: str,
    ):
        safe_label = label.strip() or "backend_scaling"
        self.backend_count = max(1, backend_count)
        self.client_workers = max(1, client_workers)
        self.rest_url = rest_url.rstrip("/")
        self.unnumbered_benchmark = unnumbered_benchmark
        self.numbered_benchmark = numbered_benchmark
        self.analyzer = BenchmarkAnalyzer(label=safe_label, scale_label="Backend Replicas")

        self.results_path = os.path.join(
            "results",
            "reports",
            f"backend_scaling{self.analyzer.label_suffix}.json",
        )
        self.summary_path = os.path.join(
            "results",
            "reports",
            f"backend_scaling{self.analyzer.label_suffix}.txt",
        )

        self._load_existing_results()

    def _load_existing_results(self):
        """Load previously recorded runs for the same label, if they exist."""
        if not os.path.exists(self.results_path):
            return

        with open(self.results_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)

        self.analyzer.results = payload.get("results", {})
        stored_scale_label = payload.get("scale_label")
        if stored_scale_label:
            self.analyzer.scale_label = stored_scale_label

        logger.info("Loaded existing backend-scaling results from %s", self.results_path)

    def _selected_benchmarks(self, architecture: str, ticket_type: str) -> List[Tuple[str, str]]:
        benchmarks: List[Tuple[str, str]] = []

        if architecture in ("rest", "both"):
            if ticket_type in ("unnumbered", "both"):
                benchmarks.append(("rest", self.unnumbered_benchmark))
            if ticket_type in ("numbered", "both"):
                benchmarks.append(("rest", self.numbered_benchmark))

        if architecture in ("rabbitmq", "both"):
            if ticket_type in ("unnumbered", "both"):
                benchmarks.append(("rabbitmq", self.unnumbered_benchmark))
            if ticket_type in ("numbered", "both"):
                benchmarks.append(("rabbitmq", self.numbered_benchmark))

        return benchmarks

    def _run_rest_benchmark(self, benchmark_file: str) -> dict:
        from src.direct.client import RestBenchmarkRunner

        logger.info(
            "Running REST benchmark with backend replicas=%s and client concurrency=%s",
            self.backend_count,
            self.client_workers,
        )
        runner = RestBenchmarkRunner(base_url=self.rest_url, num_workers=self.client_workers)
        return runner.run_benchmark(benchmark_file).to_dict()

    def _run_rabbitmq_benchmark(self, benchmark_file: str) -> dict:
        from src.indirect.client import RabbitMQBenchmarkRunner

        logger.info(
            "Running RabbitMQ benchmark with backend replicas=%s and client concurrency=%s",
            self.backend_count,
            self.client_workers,
        )
        runner = RabbitMQBenchmarkRunner(num_workers=self.client_workers)
        return runner.run_benchmark(benchmark_file).to_dict()

    def run(self, architecture: str, ticket_type: str):
        benchmarks = self._selected_benchmarks(architecture, ticket_type)

        if not benchmarks:
            raise ValueError("No benchmark combinations selected")

        logger.info("=" * 80)
        logger.info("STARTING BACKEND-SCALING DATA POINT")
        logger.info("=" * 80)
        logger.info("Architecture selection: %s", architecture)
        logger.info("Ticket type selection: %s", ticket_type)
        logger.info("Backend replicas: %s", self.backend_count)
        logger.info("Fixed client concurrency: %s", self.client_workers)
        if architecture in ("rest", "both"):
            logger.info("REST URL: %s", self.rest_url)
        logger.info("=" * 80)

        for selected_architecture, benchmark_file in benchmarks:
            logger.info(
                "Recording %s with backend replicas=%s from %s",
                selected_architecture.upper(),
                self.backend_count,
                benchmark_file,
            )

            if selected_architecture == "rest":
                metrics = self._run_rest_benchmark(benchmark_file)
            else:
                metrics = self._run_rabbitmq_benchmark(benchmark_file)

            detected_ticket_type = "unnumbered" if "unnumbered" in benchmark_file else "numbered"
            self.analyzer.upsert_result(
                selected_architecture,
                detected_ticket_type,
                self.backend_count,
                metrics,
            )

        self.analyzer.save_results_json(filepath=self.results_path)
        self.analyzer.save_summary(filepath=self.summary_path)

        plotter = PlotGenerator(self.analyzer, label=self.analyzer.label)
        plots = plotter.generate_all_plots()

        logger.info("Backend-scaling results saved to %s", self.results_path)
        logger.info("Backend-scaling summary saved to %s", self.summary_path)
        logger.info("Generated %s plot files", len(plots))


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Record a backend-scaling benchmark point and regenerate plots",
    )
    parser.add_argument(
        "--architecture",
        choices=["rest", "rabbitmq", "both"],
        required=True,
        help="Which architecture this data point represents",
    )
    parser.add_argument(
        "--ticket-type",
        choices=["unnumbered", "numbered", "both"],
        default="both",
        help="Which ticket types to run for this data point",
    )
    parser.add_argument(
        "--backend-count",
        type=int,
        required=True,
        help="Number of active backend replicas for this run",
    )
    parser.add_argument(
        "--client-workers",
        type=int,
        default=4,
        help="Fixed client concurrency to use while measuring backend scaling",
    )
    parser.add_argument(
        "--rest-url",
        default=REST_BENCHMARK_URL,
        help="REST load balancer URL for direct benchmarks",
    )
    parser.add_argument(
        "--unnumbered-benchmark",
        default="benchmarks/benchmark_unnumbered_20000.txt",
        help="Path to the unnumbered benchmark file",
    )
    parser.add_argument(
        "--numbered-benchmark",
        default="benchmarks/benchmark_numbered_60000.txt",
        help="Path to the numbered benchmark file",
    )
    parser.add_argument(
        "--label",
        default="backend_scaling",
        help="Series label used to accumulate JSON/plots across multiple runs",
    )

    args = parser.parse_args()

    suite = BackendScalingSuite(
        backend_count=args.backend_count,
        client_workers=args.client_workers,
        rest_url=args.rest_url,
        unnumbered_benchmark=args.unnumbered_benchmark,
        numbered_benchmark=args.numbered_benchmark,
        label=args.label,
    )
    suite.run(args.architecture, args.ticket_type)


if __name__ == "__main__":
    main()
