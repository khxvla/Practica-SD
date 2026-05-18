#!/usr/bin/env python3
"""
Elastic Launcher Script - Dynamically spawn/terminate workers based on queue depth.
Implements scaling formulas from Requirement 5.
"""

import sys
import os
import time
import subprocess
import argparse
import json
from threading import Thread
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.config import RABBITMQ_HOST
import pika

logger = get_logger(__name__)


class ElasticLauncher:
    """Launches/terminates workers dynamically based on system load."""
    
    def __init__(self, target_workers: int = 3, max_workers: int = 10):
        self.target_workers = target_workers
        self.max_workers = max_workers
        self.active_workers = {}  # {worker_id: process}
        self.rabbitmq_host = RABBITMQ_HOST
        self.queue_depth_history = deque(maxlen=10)
    
    def get_queue_depth(self) -> int:
        """Check RabbitMQ queue depth (number of waiting messages)."""
        try:
            credentials = pika.PlainCredentials("guest", "guest")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.rabbitmq_host, credentials=credentials)
            )
            channel = connection.channel()
            
            # Check QUEUE_REQUESTS depth
            method = channel.queue_declare(
                queue="QUEUE_REQUESTS", 
                passive=True  # Don't create if missing
            )
            
            connection.close()
            return method.method.message_count
        except Exception as e:
            logger.warning(f"Failed to get queue depth: {e}")
            return 0
    
    def calculate_target_workers(self, queue_depth: int, arrival_rate: float) -> int:
        """
        Apply scaling formulas from Requirement 5.
        
        N = max(
            backlog_formula(B),           # B·C / Tr
            arrival_rate_formula(λ)      # λ·T / C
        )
        """
        # Backlog-based (B = queue_depth, C = 10 msg/s, Tr = 5s target)
        workers_backlog = max(1, int((queue_depth * 10) / 5.0 + 0.5))
        
        # Arrival-rate based (λ = arrival_rate, T = 0.1s, C = 1.0)
        workers_arrival = max(1, int((arrival_rate * 0.1) / 1.0 + 0.5))
        
        target = max(workers_backlog, workers_arrival)
        return min(target, self.max_workers)
    
    def spawn_worker(self, worker_id: str) -> bool:
        """Spawn a new worker process."""
        if worker_id in self.active_workers:
            return False
        
        try:
            logger.info(f"Spawning worker: {worker_id}")
            
            process = subprocess.Popen(
                ["python3", "src/indirect/worker.py", f"--worker-id={worker_id}", "--prefetch=100"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            self.active_workers[worker_id] = process
            logger.info(f"✅ Worker {worker_id} spawned (PID: {process.pid})")
            return True
        
        except Exception as e:
            logger.error(f"Failed to spawn worker {worker_id}: {e}")
            return False
    
    def terminate_worker(self, worker_id: str) -> bool:
        """Send QUIT message to worker for graceful shutdown."""
        if worker_id not in self.active_workers:
            return False
        
        try:
            logger.info(f"Terminating worker: {worker_id}")
            
            # Send QUIT message to RabbitMQ
            credentials = pika.PlainCredentials("guest", "guest")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.rabbitmq_host, credentials=credentials)
            )
            channel = connection.channel()
            
            quit_msg = json.dumps({"type": "QUIT", "action": "QUIT"})
            channel.basic_publish(
                exchange='',
                routing_key="QUEUE_REQUESTS",
                body=quit_msg.encode()
            )
            
            connection.close()
            
            # Give worker 10s to exit gracefully
            process = self.active_workers[worker_id]
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning(f"Worker {worker_id} did not exit, killing...")
                process.kill()
            
            del self.active_workers[worker_id]
            logger.info(f"✅ Worker {worker_id} terminated")
            return True
        
        except Exception as e:
            logger.error(f"Error terminating worker {worker_id}: {e}")
            return False
    
    def run_elastic_loop(self, poll_interval: int = 5):
        """
        Main loop: Monitor queue depth and scale workers elastically.
        """
        logger.info(f"Starting elastic launcher (poll_interval={poll_interval}s)")
        
        # Start initial workers
        for i in range(self.target_workers):
            self.spawn_worker(f"worker-{i+1}")
        
        iteration = 0
        while True:
            iteration += 1
            queue_depth = self.get_queue_depth()
            arrival_rate = len(self.queue_depth_history) / poll_interval if self.queue_depth_history else 0
            
            self.queue_depth_history.append(queue_depth)
            
            target = self.calculate_target_workers(queue_depth, arrival_rate)
            current = len(self.active_workers)
            
            logger.info(
                f"[Iter {iteration}] Queue: {queue_depth:4d} | "
                f"Workers: {current} → {target} | "
                f"Arrival: {arrival_rate:.2f} msg/s"
            )
            
            # Scale up
            if current < target:
                for i in range(target - current):
                    worker_id = f"worker-{current + i + 1}"
                    self.spawn_worker(worker_id)
            
            # Scale down
            elif current > target:
                workers_to_remove = list(self.active_workers.keys())[-( current - target):]
                for worker_id in workers_to_remove:
                    self.terminate_worker(worker_id)
            
            # Check worker health
            for worker_id, process in list(self.active_workers.items()):
                if process.poll() is not None:
                    logger.warning(f"Worker {worker_id} exited unexpectedly, respawning...")
                    del self.active_workers[worker_id]
                    self.spawn_worker(worker_id)
            
            time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="Elastic Worker Launcher")
    parser.add_argument("--workers", type=int, default=3, help="Initial worker count")
    parser.add_argument("--max-workers", type=int, default=10, help="Max workers allowed")
    parser.add_argument("--poll-interval", type=int, default=5, help="Poll interval (seconds)")
    
    args = parser.parse_args()
    
    launcher = ElasticLauncher(target_workers=args.workers, max_workers=args.max_workers)
    
    try:
        launcher.run_elastic_loop(poll_interval=args.poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down elastic launcher...")
        for worker_id in list(launcher.active_workers.keys()):
            launcher.terminate_worker(worker_id)


if __name__ == "__main__":
    main()
