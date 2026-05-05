"""Generate performance plots from benchmark results."""
import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger

logger = get_logger(__name__)

try:
    mpl_config_dir = os.path.join(os.getcwd(), ".matplotlib")
    os.makedirs(mpl_config_dir, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", mpl_config_dir)

    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    logger.warning("matplotlib not available, plots will be skipped")

class PlotGenerator:
    """Generate performance plots."""
    
    def __init__(self, analyzer, label: str = ""):
        """
        Initialize plot generator.
        
        Args:
            analyzer: BenchmarkAnalyzer instance
        """
        self.analyzer = analyzer
        self.output_dir = "results/plots"
        os.makedirs(self.output_dir, exist_ok=True)
        safe_label = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in label.strip())
        if not safe_label and getattr(analyzer, "label", ""):
            safe_label = analyzer.label
        self.label = safe_label
        self.label_suffix = f"_{safe_label}" if safe_label else ""
        self.x_axis_label = getattr(analyzer, "scale_label", "Workers")

    def plot_scalability(self, architecture: str, ticket_type: str, save: bool = True):
        """
        Plot throughput vs the configured x-axis value.
        
        Args:
            architecture: 'rest' or 'rabbitmq'
            ticket_type: 'unnumbered' or 'numbered'
            save: Save to file
        
        Returns:
            Filepath or None
        """
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("matplotlib not available")
            return None
        
        analysis = self.analyzer.get_scalability_analysis(architecture, ticket_type)
        
        if not analysis.get('num_workers'):
            logger.warning(f"No data for {architecture} {ticket_type}")
            return None
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f'{architecture.upper()} - {ticket_type.upper()} Tickets', fontsize=16)
        
        # Throughput vs scale axis
        ax = axes[0, 0]
        ax.plot(analysis['num_workers'], analysis['throughputs'], 'o-', linewidth=2, markersize=8)
        ax.set_xlabel(self.x_axis_label)
        ax.set_ylabel('Throughput (ops/sec)')
        ax.set_title(f'Throughput vs {self.x_axis_label}')
        ax.grid(True, alpha=0.3)
        
        # Latency vs workers
        ax = axes[0, 1]
        ax.plot(analysis['num_workers'], analysis['latencies'], 's-', linewidth=2, markersize=8, color='orange')
        ax.set_xlabel(self.x_axis_label)
        ax.set_ylabel('Avg Latency (seconds)')
        ax.set_title(f'Latency vs {self.x_axis_label}')
        ax.grid(True, alpha=0.3)
        
        # Scalability factor
        ax = axes[1, 0]
        ax.plot(analysis['num_workers'], analysis['scalability_factors'], '^-', linewidth=2, markersize=8, color='green')
        ax.axhline(y=1, color='r', linestyle='--', label='Baseline (1x)')
        ax.set_xlabel(self.x_axis_label)
        ax.set_ylabel('Scalability Factor')
        ax.set_title('Scalability Factor (Throughput / Baseline)')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Success rate
        ax = axes[1, 1]
        ax.plot(analysis['num_workers'], analysis['success_rates'], 'd-', linewidth=2, markersize=8, color='purple')
        min_success = min(analysis['success_rates'])
        max_success = max(analysis['success_rates'])
        if min_success == max_success:
            padding = 2 if min_success < 98 else 0.5
        else:
            padding = max(1.0, (max_success - min_success) * 0.2)
        lower_bound = max(0, min_success - padding)
        upper_bound = min(101, max_success + padding)
        ax.set_ylim([lower_bound, upper_bound])
        ax.set_xlabel(self.x_axis_label)
        ax.set_ylabel('Success Rate (%)')
        ax.set_title(f'Success Rate vs {self.x_axis_label}')
        ax.grid(True, alpha=0.3)
        
        if save:
            filepath = os.path.join(
                self.output_dir,
                f"{architecture}_{ticket_type}_scalability{self.label_suffix}.png",
            )
            plt.savefig(filepath, dpi=150, bbox_inches='tight')
            logger.info(f"Plot saved: {filepath}")
            plt.close()
            return filepath
        else:
            return fig

    def plot_comparison(self, ticket_type: str, save: bool = True):
        """
        Compare REST vs RabbitMQ.
        
        Args:
            ticket_type: 'unnumbered' or 'numbered'
            save: Save to file
        
        Returns:
            Filepath or None
        """
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("matplotlib not available")
            return None
        
        rest_analysis = self.analyzer.get_scalability_analysis("rest", ticket_type)
        rabbit_analysis = self.analyzer.get_scalability_analysis("rabbitmq", ticket_type)
        
        if not (rest_analysis.get('num_workers') and rabbit_analysis.get('num_workers')):
            logger.warning(f"No data for comparison: {ticket_type}")
            return None
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        fig.suptitle(f'Comparison: REST vs RabbitMQ - {ticket_type.upper()} Tickets', fontsize=16)
        
        # Throughput comparison
        ax = axes[0]
        ax.plot(
            rest_analysis['num_workers'],
            rest_analysis['throughputs'],
            'o-',
            label='REST',
            linewidth=2,
            markersize=8,
        )
        ax.plot(
            rabbit_analysis['num_workers'],
            rabbit_analysis['throughputs'],
            's-',
            label='RabbitMQ',
            linewidth=2,
            markersize=8,
        )
        ax.set_xlabel(self.x_axis_label)
        ax.set_ylabel('Throughput (ops/sec)')
        ax.set_title('Throughput Comparison')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Latency comparison
        ax = axes[1]
        ax.plot(
            rest_analysis['num_workers'],
            rest_analysis['latencies'],
            'o-',
            label='REST',
            linewidth=2,
            markersize=8,
        )
        ax.plot(
            rabbit_analysis['num_workers'],
            rabbit_analysis['latencies'],
            's-',
            label='RabbitMQ',
            linewidth=2,
            markersize=8,
        )
        ax.set_xlabel(self.x_axis_label)
        ax.set_ylabel('Avg Latency (seconds)')
        ax.set_title('Latency Comparison')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Scalability factor comparison
        ax = axes[2]
        ax.plot(
            rest_analysis['num_workers'],
            rest_analysis['scalability_factors'],
            'o-',
            label='REST',
            linewidth=2,
            markersize=8,
        )
        ax.plot(
            rabbit_analysis['num_workers'],
            rabbit_analysis['scalability_factors'],
            's-',
            label='RabbitMQ',
            linewidth=2,
            markersize=8,
        )
        ax.axhline(y=1, color='r', linestyle='--', alpha=0.5)
        ax.set_xlabel(self.x_axis_label)
        ax.set_ylabel('Scalability Factor')
        ax.set_title('Scalability Comparison')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        if save:
            filepath = os.path.join(
                self.output_dir,
                f"comparison_{ticket_type}{self.label_suffix}.png",
            )
            plt.savefig(filepath, dpi=150, bbox_inches='tight')
            logger.info(f"Comparison plot saved: {filepath}")
            plt.close()
            return filepath
        else:
            return fig

    def plot_latency_breakdown_comparison(self, ticket_type: str, save: bool = True):
        """
        Compare client, server and overhead latency for REST vs RabbitMQ.

        Client latency is the buyer-visible end-to-end latency. Server latency is
        the backend/worker processing time reported by the server side. Overhead
        is the difference between both values.
        """
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("matplotlib not available")
            return None

        rest_analysis = self.analyzer.get_scalability_analysis("rest", ticket_type)
        rabbit_analysis = self.analyzer.get_scalability_analysis("rabbitmq", ticket_type)

        if not (rest_analysis.get("num_workers") and rabbit_analysis.get("num_workers")):
            logger.warning(f"No latency breakdown data for comparison: {ticket_type}")
            return None

        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        fig.suptitle(
            f"Latency Breakdown: REST vs RabbitMQ - {ticket_type.upper()} Tickets",
            fontsize=16,
        )

        series = [
            ("latencies", "Client Latency", "Avg Client Latency (seconds)"),
            ("server_latencies", "Server Latency", "Avg Server Latency (seconds)"),
            ("overhead_latencies", "Transport/Queue Overhead", "Avg Overhead Latency (seconds)"),
        ]

        for ax, (metric_key, title, ylabel) in zip(axes, series):
            ax.plot(
                rest_analysis["num_workers"],
                rest_analysis.get(metric_key, []),
                "o-",
                label="REST",
                linewidth=2,
                markersize=8,
            )
            ax.plot(
                rabbit_analysis["num_workers"],
                rabbit_analysis.get(metric_key, []),
                "s-",
                label="RabbitMQ",
                linewidth=2,
                markersize=8,
            )
            ax.set_xlabel(self.x_axis_label)
            ax.set_ylabel(ylabel)
            ax.set_title(title)
            ax.legend()
            ax.grid(True, alpha=0.3)

        if save:
            filepath = os.path.join(
                self.output_dir,
                f"latency_breakdown_{ticket_type}{self.label_suffix}.png",
            )
            plt.savefig(filepath, dpi=150, bbox_inches="tight")
            logger.info(f"Latency breakdown plot saved: {filepath}")
            plt.close()
            return filepath
        return fig

    def generate_all_plots(self):
        """Generate all available plots."""
        logger.info("Generating plots...")
        
        plots_created = []
        
        # Scalability plots for each combination
        for arch in ["rest", "rabbitmq"]:
            for ticket_type in ["unnumbered", "numbered"]:
                plot = self.plot_scalability(arch, ticket_type)
                if plot:
                    plots_created.append(plot)
        
        # Comparison plots
        for ticket_type in ["unnumbered", "numbered"]:
            plot = self.plot_comparison(ticket_type)
            if plot:
                plots_created.append(plot)
            plot = self.plot_latency_breakdown_comparison(ticket_type)
            if plot:
                plots_created.append(plot)
        
        logger.info(f"Generated {len(plots_created)} plots")
        return plots_created
