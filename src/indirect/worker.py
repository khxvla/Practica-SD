"""RabbitMQ Worker for Indirect Communication Architecture with Realism Delay and QUIT Support."""
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
    """Worker process that consumes ticket requests from RabbitMQ elastically."""
    
    def __init__(self, worker_id: str = "worker-1", prefetch_count: int = 100):
        """Initialize worker."""
        self.worker_id = worker_id
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self.redis_backend: Optional[RedisBackend] = None
        self.prefetch_count = max(1, prefetch_count)
        self.should_stop = False
        self.processed_count = 0
        self.error_count = 0
        
        # Signal handlers for graceful shutdown (EC2 environments)
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
                retry_delay=2,
                heartbeat=60
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            logger.info(f"[{self.worker_id}] Connected to RabbitMQ")
            
            # Setup queues usando vuestro BrokerSetup corporativo
            self.channel.queue_declare(queue=BrokerSetup.QUEUE_REQUESTS, durable=True)
            self.channel.queue_declare(queue=BrokerSetup.QUEUE_RESPONSES, durable=True)
            
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

    def _process_single_message(self) -> bool:
        """
        Fetches and processes a single message using non-blocking basic_get.
        Returns True if a message was processed, False if queue is empty or QUIT received.
        """
        # Usamos basic_get en lugar de basic_consume para permitir un control elástico
        method_frame, properties, body = self.channel.basic_get(
            queue=BrokerSetup.QUEUE_REQUESTS, 
            auto_ack=False
        )
        
        if not method_frame:
            # Si no hay mensajes, informamos al bucle elástico para que pueda apagar el worker (Scale-down)
            return False
            
        try:
            request = json.loads(body.decode('utf-8'))
            request_type = request.get("type")
            
            # --- CORREO DEL PROFESOR: CONTROL DE APAGADO LIMPIÓ MEDIANTE MENSAJE QUIT ---
            if request_type == "QUIT" or request.get("action") == "QUIT":
                logger.info(f"[{self.worker_id}] 🛑 Señal QUIT recibida en la cola. Deteniendo consumidor elásticamente.")
                self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                self.should_stop = True
                return False

            # --- SECCIÓN 4 DEL ENUNCIADO: REQUISITO DE REALISMO (DELAY 100ms) ---
            # Modela la latencia externa de procesamiento de pasarelas de pago bancarias
            time.sleep(0.100)

            client_id = request.get("client_id")
            request_id = request.get("request_id")
            success = False
            
            # Procesamiento concurrente e idempotente usando vuestro RedisBackend
            if request_type == "unnumbered":
                success = self.redis_backend.buy_unnumbered_idempotent(client_id, request_id)
            elif request_type == "numbered":
                seat_id = request.get("seat_id")
                if seat_id is not None:
                    success = self.redis_backend.buy_numbered_idempotent(seat_id, client_id, request_id)
            else:
                logger.warning(f"[{self.worker_id}] Unknown request type: {request_type}")
            
            # --- SECCIÓN 9: REGISTRO DE MÉTRICA EN EL SERVIDOR (PERSISTENT LOG) ---
            # Guardamos el instante exacto de finalización real en el backend
            try:
                self.redis_backend.record_server_metric(
                    request_id=request_id,
                    success=success,
                    timestamp=time.time()
                )
            except Exception as metric_err:
                logger.warning(f"[{self.worker_id}] Failed to record server metric for req {request_id}: {metric_err}")

            # Preparar payload de respuesta
            response = {
                "success": success,
                "type": request_type,
                "client_id": client_id,
                "request_id": request_id,
                "worker_id": self.worker_id
            }
            
            # Devolver respuesta asíncrona al buzón privado del cliente (RPC Reply Topology)
            if properties and properties.reply_to:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    body=json.dumps(response),
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    )
                )
            
            # Confirmar procesamiento correcto a RabbitMQ
            self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            self.processed_count += 1
            
            if self.processed_count % 1000 == 0:
                logger.info(f"[{self.worker_id}] Processed {self.processed_count} requests, errors: {self.error_count}")
            
            return True

        except Exception as e:
            logger.error(f"[{self.worker_id}] Error processing request: {e}")
            self.error_count += 1

            # Req 10 — DLQ routing: track retry count via message headers.
            # After MAX_RETRIES failures, nack with requeue=False so RabbitMQ
            # routes the message to ticket_dlq instead of looping indefinitely.
            MAX_RETRIES = 3
            retry_count = 0
            if properties and properties.headers:
                retry_count = int(properties.headers.get("x-retry-count", 0))

            if retry_count < MAX_RETRIES:
                # Re-publish with incremented retry count so the header persists
                updated_headers = dict(properties.headers or {})
                updated_headers["x-retry-count"] = retry_count + 1
                try:
                    self.channel.basic_publish(
                        exchange="",
                        routing_key=BrokerSetup.QUEUE_REQUESTS,
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            headers=updated_headers,
                            reply_to=properties.reply_to if properties else None,
                            correlation_id=properties.correlation_id if properties else None,
                        ),
                    )
                except Exception:
                    pass
                self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                logger.warning(f"[{self.worker_id}] Retry {retry_count + 1}/{MAX_RETRIES} for req")
            else:
                # Max retries exceeded → route to DLQ
                logger.error(f"[{self.worker_id}] Max retries reached, routing to DLQ")
                self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

            return True

    def run(self):
        """Start the worker loop dynamically."""
        logger.info(f"[{self.worker_id}] Starting elastic stateless worker...")
        
        try:
            self._connect_rabbitmq()
            self._connect_redis()
            
            logger.info(f"[{self.worker_id}] ✓ Worker initialized, polling requests...")
            
            # Bucle elástico controlado: procesa mientras queden mensajes y no reciba orden de apagar
            while not self.should_stop:
                has_processed = self._process_single_message()
                if not has_processed:
                    # Si la cola está vacía, el worker stateless se retira para evitar sobreprovisionamiento
                    logger.info(f"[{self.worker_id}] No quedan mensajes o se recibió QUIT. Retirando instancia de forma elástica.")
                    break
        
        except KeyboardInterrupt:
            logger.info(f"[{self.worker_id}] Interrupted by user")
        except Exception as e:
            logger.error(f"[{self.worker_id}] Worker execution crash: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the worker and release network resources safely."""
        logger.info(f"[{self.worker_id}] Stopping worker resources...")
        
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except:
                pass
        
        logger.info(f"[{self.worker_id}] Processed {self.processed_count} requests, {self.error_count} errors")
        logger.info(f"[{self.worker_id}] ✓ Worker stopped cleanly")

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