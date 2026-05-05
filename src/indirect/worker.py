"""RabbitMQ Worker for Indirect Communication Architecture"""
import sys
import os
import signal
import json
import time
import pika
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.redis_backend import RedisBackend
from src.common.config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_VHOST
from src.indirect.broker_setup import BrokerSetup

logger = get_logger(__name__)

class TicketWorker:
    """Worker process that consumes ticket requests from RabbitMQ."""
    
    def __init__(self, worker_id: str = "worker-1", prefetch_count: int = 100):
        """
        Initialize worker.
        
        Args:
            worker_id: Unique worker identifier
        """
        self.worker_id = worker_id
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self.redis_backend: Optional[RedisBackend] = None
        self.prefetch_count = max(1, prefetch_count)
        self.should_stop = False
        self.processed_count = 0
        self.error_count = 0
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown."""
        logger.info(f"[{self.worker_id}] Received signal {signum}, initiating graceful shutdown...")
        self.should_stop = True

    def _connect_rabbitmq(self):
        """Connect to RabbitMQ."""
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            logger.info(f"[{self.worker_id}] Connected to RabbitMQ")
            
            # Setup queues
            self.channel.queue_declare(queue=BrokerSetup.QUEUE_REQUESTS, durable=True)
            self.channel.queue_declare(queue=BrokerSetup.QUEUE_RESPONSES, durable=True)
            
            # Allow each worker to keep a small batch locally to reduce broker round trips.
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            
        except Exception as e:
            logger.error(f"[{self.worker_id}] Failed to connect to RabbitMQ: {e}")
            raise

    def _connect_redis(self):
        """Connect to Redis."""
        try:
            self.redis_backend = RedisBackend()
            logger.info(f"[{self.worker_id}] Connected to Redis")
        except Exception as e:
            logger.error(f"[{self.worker_id}] Failed to connect to Redis: {e}")
            raise

    def _process_request(self, ch, method, properties, body):
        """
        Process a ticket request.
        
        Args:
            ch: Channel
            method: Method frame
            properties: Properties
            body: Message body
        """
        handler_start = time.perf_counter()
        try:
            # Parse request
            request = json.loads(body)
            
            request_type = request.get("type")
            client_id = request.get("client_id")
            request_id = request.get("request_id")
            
            success = False
            
            # Process based on type
            if request_type == "unnumbered":
                success = self.redis_backend.buy_unnumbered_idempotent(client_id, request_id)
            
            elif request_type == "numbered":
                seat_id = request.get("seat_id")
                if seat_id is not None:
                    success = self.redis_backend.buy_numbered_idempotent(seat_id, client_id, request_id)
            
            else:
                logger.warning(f"[{self.worker_id}] Unknown request type: {request_type}")
            
            # Prepare response
            response = {
                "success": success,
                "type": request_type,
                "client_id": client_id,
                "request_id": request_id,
                "worker_id": self.worker_id,
                "server_latency": time.perf_counter() - handler_start,
            }
            
            # Send response (if reply_to is specified)
            if properties.reply_to:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    body=json.dumps(response),
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    )
                )
            
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            self.processed_count += 1
            
            if self.processed_count % 1000 == 0:
                logger.info(f"[{self.worker_id}] Processed {self.processed_count} requests, errors: {self.error_count}")
        
        except Exception as e:
            logger.error(f"[{self.worker_id}] Error processing request: {e}")
            self.error_count += 1
            # Negative acknowledgment, requeue the message
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def run(self):
        """Start the worker."""
        logger.info(f"[{self.worker_id}] Starting worker...")
        
        try:
            # Connect to services
            self._connect_rabbitmq()
            self._connect_redis()
            
            logger.info(f"[{self.worker_id}] ✓ Worker initialized, waiting for requests...")
            
            # Register callback
            self.channel.basic_consume(
                queue=BrokerSetup.QUEUE_REQUESTS,
                on_message_callback=self._process_request
            )
            
            # Start consuming
            self.channel.start_consuming()
        
        except KeyboardInterrupt:
            logger.info(f"[{self.worker_id}] Interrupted by user")
        except Exception as e:
            logger.error(f"[{self.worker_id}] Worker error: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the worker."""
        logger.info(f"[{self.worker_id}] Stopping worker...")
        
        if self.channel:
            try:
                self.channel.stop_consuming()
            except:
                pass
        
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
        
        logger.info(f"[{self.worker_id}] Processed {self.processed_count} requests, {self.error_count} errors")
        logger.info(f"[{self.worker_id}] ✓ Worker stopped")

def main():
    """Run worker."""
    import argparse
    
    parser = argparse.ArgumentParser(description="RabbitMQ Ticket Worker")
    parser.add_argument("--worker-id", default="worker-1", help="Worker identifier")
    parser.add_argument(
        "--prefetch",
        type=int,
        default=100,
        help="QoS prefetch count for this worker",
    )
    
    args = parser.parse_args()
    
    worker = TicketWorker(worker_id=args.worker_id, prefetch_count=args.prefetch)
    worker.run()

if __name__ == "__main__":
    main()
