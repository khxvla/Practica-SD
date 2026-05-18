"""RabbitMQ broker setup helpers for the indirect architecture."""
import sys
import os
from typing import Optional

import pika

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.config import (
    RABBITMQ_HOST,
    RABBITMQ_PASSWORD,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    RABBITMQ_VHOST,
)
from src.common.logger import get_logger

logger = get_logger(__name__)


class BrokerSetup:
    """Create and manage the RabbitMQ topology used by clients and workers."""

    EXCHANGE_TICKETS = "ticket_exchange"
    QUEUE_REQUESTS = "ticket_requests"
    QUEUE_RESPONSES = "ticket_responses"
    QUEUE_DLQ = "ticket_dlq"  # Dead-Letter Queue (Req 10: fault tolerance)
    ROUTING_KEY_REQUEST = "request"

    def __init__(self):
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self._connect()

    def _connect(self):
        """Open a RabbitMQ connection for administrative operations."""
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
        logger.info("Connected to RabbitMQ for broker setup")

    def setup_queues(self):
        """Declare the exchange, queues and bindings used by the app."""
        self.channel.exchange_declare(
            exchange=self.EXCHANGE_TICKETS,
            exchange_type="direct",
            durable=True,
        )

        # Dead-Letter Queue (Req 10): messages rejected >3 times are routed here
        # instead of being silently dropped or causing infinite requeue loops.
        self.channel.queue_declare(queue=self.QUEUE_DLQ, durable=True)

        # Main request queue — links to DLQ on rejection
        self.channel.queue_declare(
            queue=self.QUEUE_REQUESTS,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self.QUEUE_DLQ,
                "x-message-ttl": 300_000,   # 5 min max in queue
            },
        )
        self.channel.queue_declare(queue=self.QUEUE_RESPONSES, durable=True)
        self.channel.queue_bind(
            exchange=self.EXCHANGE_TICKETS,
            queue=self.QUEUE_REQUESTS,
            routing_key=self.ROUTING_KEY_REQUEST,
        )
        logger.info(
            "Broker topology ready: exchange=%s, request_queue=%s, response_queue=%s, dlq=%s",
            self.EXCHANGE_TICKETS,
            self.QUEUE_REQUESTS,
            self.QUEUE_RESPONSES,
            self.QUEUE_DLQ,
        )

    def purge_queues(self):
        """Remove stale messages from durable queues before a benchmark run."""
        request_result = self.channel.queue_purge(queue=self.QUEUE_REQUESTS)
        response_result = self.channel.queue_purge(queue=self.QUEUE_RESPONSES)
        logger.info(
            "Purged queues: %s=%s messages, %s=%s messages",
            self.QUEUE_REQUESTS,
            getattr(request_result.method, "message_count", 0),
            self.QUEUE_RESPONSES,
            getattr(response_result.method, "message_count", 0),
        )

    def close(self):
        """Close the setup connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()


def main():
    """CLI entry point for broker initialization."""
    broker = BrokerSetup()
    try:
        broker.setup_queues()
        logger.info("Broker setup complete")
    finally:
        broker.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
