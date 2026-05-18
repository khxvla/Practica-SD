"""Benchmark analyzer and metrics processor."""
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger

logger = get_logger(__name__)

class BenchmarkAnalyzer:
    """Analyze and compare benchmark results."""
    
    def __init__(self, label: str = "", scale_label: str = "Workers"):
        """Initialize analyzer."""
        self.results = {}  # architecture -> list of runs
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_label = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in label.strip())
        self.label = safe_label
        self.label_suffix = f"_{safe_label}" if safe_label else ""
        self.scale_label = scale_label

    def add_result(self, architecture: str, ticket_type: str, num_workers: int, metrics: dict):
        """
        Add benchmark result.
        
        Args:
            architecture: 'rest' or 'rabbitmq'
            ticket_type: 'unnumbered' or 'numbered'
            num_workers: Value represented on the scalability x-axis
            metrics: MetricsCollector.to_dict()
        """
        key = f"{architecture}_{ticket_type}"
        if key not in self.results:
            self.results[key] = []
        
        result = {
            "timestamp": datetime.now().isoformat(),
            "architecture": architecture,
            "ticket_type": ticket_type,
            "num_workers": num_workers,
            **metrics
        }
        self.results[key].append(result)
        logger.info(f"Added result: {key} with {num_workers} workers")

    def upsert_result(self, architecture: str, ticket_type: str, num_workers: int, metrics: dict):
        """Replace any existing result with the same x-axis value, then add the new one."""
        key = f"{architecture}_{ticket_type}"
        if key not in self.results:
            self.results[key] = []

        self.results[key] = [
            existing for existing in self.results[key]
            if existing.get("num_workers") != num_workers
        ]
        self.add_result(architecture, ticket_type, num_workers, metrics)

    def get_scalability_analysis(self, architecture: str, ticket_type: str) -> Dict:
        """
        Analyze scalability: throughput vs num_workers.
        
        Args:
            architecture: 'rest' or 'rabbitmq'
            ticket_type: 'unnumbered' or 'numbered'
        
        Returns:
            Analysis dictionary
        """
        key = f"{architecture}_{ticket_type}"
        if key not in self.results:
            return {}
        
        runs = sorted(self.results[key], key=lambda x: x['num_workers'])
        
        workers = [r['num_workers'] for r in runs]
        throughputs = [r['throughput'] for r in runs]
        latencies = [r['by_type'][ticket_type]['avg_latency'] 
                     if r['by_type'].get(ticket_type) else 0 for r in runs]
        success_rates = [r['by_type'][ticket_type]['success_rate']
                        if r['by_type'].get(ticket_type) else 0 for r in runs]
        
        # Calculate scalability factor
        if len(throughputs) > 1:
            baseline = throughputs[0]
            scalability_factors = [t / baseline if baseline > 0 else 0 for t in throughputs]
        else:
            scalability_factors = [1] if throughputs else []
        
        return {
            "architecture": architecture,
            "ticket_type": ticket_type,
            "num_workers": workers,
            "throughputs": throughputs,
            "latencies": latencies,
            "success_rates": success_rates,
            "scalability_factors": scalability_factors
        }

    def compare_architectures(self, ticket_type: str) -> Dict:
        """
        Compare REST vs RabbitMQ.
        
        Args:
            ticket_type: 'unnumbered' or 'numbered'
        
        Returns:
            Comparison dictionary
        """
        rest_analysis = self.get_scalability_analysis("rest", ticket_type)
        rabbit_analysis = self.get_scalability_analysis("rabbitmq", ticket_type)
        
        comparison = {
            "ticket_type": ticket_type,
            "rest": rest_analysis,
            "rabbitmq": rabbit_analysis
        }
        
        # Calculate differences
        if (
            rest_analysis.get('throughputs')
            and rabbit_analysis.get('throughputs')
            and rest_analysis.get('num_workers') == rabbit_analysis.get('num_workers')
        ):
            rest_tp = rest_analysis['throughputs']
            rabbit_tp = rabbit_analysis['throughputs']
            
            # At same worker count
            differences = [(r - rb) / rb * 100 if rb > 0 else 0 
                          for r, rb in zip(rest_tp, rabbit_tp)]
            comparison['throughput_difference_percent'] = differences
        
        return comparison

    def generate_summary(self) -> str:
        """Generate text summary of all results."""
        summary = []
        summary.append("=" * 80)
        summary.append("BENCHMARK ANALYSIS SUMMARY")
        summary.append(f"Timestamp: {datetime.now().isoformat()}")
        if self.label:
            summary.append(f"Label: {self.label}")
        summary.append("=" * 80)
        
        for ticket_type in ["unnumbered", "numbered"]:
            summary.append(f"\n{'=' * 80}")
            summary.append(f"TICKET TYPE: {ticket_type.upper()}")
            summary.append("=" * 80)
            
            for arch in ["rest", "rabbitmq"]:
                analysis = self.get_scalability_analysis(arch, ticket_type)
                if not analysis.get('num_workers'):
                    continue
                
                summary.append(f"\n{arch.upper()} Architecture:")
                summary.append(f"  {self.scale_label}: {analysis['num_workers']}")
                summary.append(f"  Throughput (ops/sec): {analysis['throughputs']}")
                summary.append(f"  Avg Latency (s): {analysis['latencies']}")
                summary.append(f"  Success Rate (%): {analysis['success_rates']}")
                summary.append(f"  Scalability Factors: {analysis['scalability_factors']}")
            
            # Comparison
            comparison = self.compare_architectures(ticket_type)
            if comparison['rest'] and comparison['rabbitmq']:
                summary.append(f"\nComparison (REST vs RabbitMQ):")
                if 'throughput_difference_percent' in comparison:
                    diffs = comparison['throughput_difference_percent']
                    summary.append(f"  REST Throughput Advantage (%): {diffs}")
        
        summary.append("\n" + "=" * 80)
        return "\n".join(summary)

    def save_results_json(self, filepath: str = None):
        """Save results to JSON file."""
        if filepath is None:
            filepath = f"results/reports/benchmark_results{self.label_suffix}_{self.timestamp}.json"
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        output = {
            "timestamp": self.timestamp,
            "scale_label": self.scale_label,
            "results": self.results,
            "analyses": {
                f"{arch}_{type}": self.get_scalability_analysis(arch, type)
                for arch in ["rest", "rabbitmq"]
                for type in ["unnumbered", "numbered"]
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(output, f, indent=2)
        
        logger.info(f"Results saved to {filepath}")
        return filepath

    def save_summary(self, filepath: str = None):
        """Save text summary."""
        if filepath is None:
            filepath = f"results/reports/benchmark_summary{self.label_suffix}_{self.timestamp}.txt"
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        summary = self.generate_summary()
        with open(filepath, 'w') as f:
            f.write(summary)
        
        logger.info(f"Summary saved to {filepath}")
        return filepath
