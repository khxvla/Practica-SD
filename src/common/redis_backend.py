"""Redis backend for consistency and coordination."""
import redis
import json
from src.common.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD, TOTAL_TICKETS
from src.common.logger import get_logger

logger = get_logger(__name__)

class RedisBackend:
    """Redis client wrapper with ticket system methods."""
    
    def __init__(self):
        """Initialize Redis connection."""
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        # Test connection
        try:
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def initialize_system(self):
        """Initialize system state in Redis."""
        # Unnumbered counter
        self.redis_client.set("tickets:unnumbered:count", 0)
        logger.info("Initialized unnumbered tickets counter")
        
        # Numbered tickets - set of available seat IDs (batched SADD for performance)
        batch_size = 1000
        for i in range(1, TOTAL_TICKETS + 1, batch_size):
            batch = list(range(i, min(i + batch_size, TOTAL_TICKETS + 1)))
            self.redis_client.sadd("tickets:numbered:available", *batch)
        logger.info(f"Initialized {TOTAL_TICKETS} available numbered seats")

    def reset_system(self):
        """Reset all ticket counters (for testing)."""
        self.redis_client.delete("tickets:unnumbered:count")
        self.redis_client.delete("tickets:numbered:available")
        self.redis_client.delete("tickets:numbered:sold")
        logger.info("Reset all counters")

    def _request_result_key(self, client_id, request_id):
        """Build the Redis key used for idempotent request tracking."""
        return f"tickets:requests:{client_id}:{request_id}"

    # Unnumbered ticket operations
    def buy_unnumbered(self):
        """
        Attempt to buy an unnumbered ticket.
        Returns True if successful, False if sold out.
        """
        current = self.redis_client.incr("tickets:unnumbered:count")
        if current <= 20000:
            return True
        else:
            # Decrement back if we went over
            self.redis_client.decr("tickets:unnumbered:count")
            return False

    def buy_unnumbered_idempotent(self, client_id, request_id):
        """
        Idempotent unnumbered purchase keyed by client_id + request_id.

        This keeps the direct and indirect architectures safe to retry when a
        response is lost but the request may already have been applied.
        """
        request_key = self._request_result_key(client_id, request_id)

        while True:
            with self.redis_client.pipeline() as pipe:
                try:
                    pipe.watch(request_key, "tickets:unnumbered:count")

                    existing = pipe.get(request_key)
                    if existing is not None:
                        pipe.unwatch()
                        return existing == "1"

                    current_value = pipe.get("tickets:unnumbered:count")
                    current_count = int(current_value) if current_value else 0
                    success = current_count < TOTAL_TICKETS

                    pipe.multi()
                    if success:
                        pipe.incr("tickets:unnumbered:count")
                    pipe.set(request_key, "1" if success else "0")
                    pipe.execute()
                    return success
                except redis.WatchError:
                    continue

    def get_unnumbered_count(self):
        """Get current count of sold unnumbered tickets."""
        count = self.redis_client.get("tickets:unnumbered:count")
        return int(count) if count else 0

    # Numbered ticket operations
    def buy_numbered(self, seat_id):
        """
        Attempt to buy a specific numbered seat.
        Returns True if successful, False if already sold.
        Uses SET for availability tracking.
        """
        try:
            seat_id = int(seat_id)
            if seat_id < 1 or seat_id > 20000:
                return False
        except (ValueError, TypeError):
            return False
        
        # Try to remove from available set
        result = self.redis_client.srem("tickets:numbered:available", seat_id)
        if result == 1:
            # Successfully removed from available
            self.redis_client.sadd("tickets:numbered:sold", seat_id)
            return True
        else:
            # Seat already sold
            return False

    def buy_numbered_idempotent(self, seat_id, client_id, request_id):
        """
        Idempotent numbered purchase keyed by client_id + request_id.
        """
        try:
            seat_id = int(seat_id)
            if seat_id < 1 or seat_id > TOTAL_TICKETS:
                return False
        except (ValueError, TypeError):
            return False

        request_key = self._request_result_key(client_id, request_id)

        while True:
            with self.redis_client.pipeline() as pipe:
                try:
                    pipe.watch(
                        request_key,
                        "tickets:numbered:available",
                        "tickets:numbered:sold",
                    )

                    existing = pipe.get(request_key)
                    if existing is not None:
                        pipe.unwatch()
                        return existing == "1"

                    seat_available = pipe.sismember("tickets:numbered:available", seat_id)
                    success = bool(seat_available)

                    pipe.multi()
                    if success:
                        pipe.srem("tickets:numbered:available", seat_id)
                        pipe.sadd("tickets:numbered:sold", seat_id)
                    pipe.set(request_key, "1" if success else "0")
                    pipe.execute()
                    return success
                except redis.WatchError:
                    continue

    def get_numbered_sold_count(self):
        """Get count of sold numbered seats."""
        return self.redis_client.scard("tickets:numbered:sold")

    def get_numbered_available_count(self):
        """Get count of available numbered seats."""
        return self.redis_client.scard("tickets:numbered:available")

    def get_sold_seats(self):
        """Get list of all sold seat IDs."""
        return self.redis_client.smembers("tickets:numbered:sold")

    # Stats and health
    def get_stats(self):
        """Get current system statistics."""
        return {
            "unnumbered_sold": self.get_unnumbered_count(),
            "numbered_sold": self.get_numbered_sold_count(),
            "numbered_available": self.get_numbered_available_count(),
        }

    def clear_all(self):
        """Clear all keys (use with caution)."""
        keys = self.redis_client.keys("tickets:*")
        if keys:
            self.redis_client.delete(*keys)
            logger.warning(f"Cleared {len(keys)} keys from Redis")

    def is_duplicate_request(self, request_id: str) -> bool:
        """
        Sección 10: Garantiza idempotencia (Retry safety).
        Comprueba si la clave única del request_id ya existe en el sistema.
        """
        if not request_id:
            return False
        # Si la clave existe en Redis, significa que ya fue procesada por algún worker
        return self.redis_client.exists(f"processed_req:{request_id}")

    def record_server_metric(self, request_id: str, success: bool, timestamp: float):
        """
        Sección 9: Almacena de forma persistente en el servidor las métricas de finalización.
        Esto permite calcular el Throughput real del backend sin depender de los tiempos del cliente.
        """
        if not request_id:
            return
        
        # Guardamos la solicitud en un Hash de Redis para marcarla como procesada (Idempotencia)
        # y registrar su timestamp exacto de reloj del servidor (Métricas precisas)
        metric_data = {
            "success": str(success),
            "timestamp_completado": str(timestamp)
        }
        self.redis_client.hset(f"processed_req:{request_id}", mapping=metric_data)
        
        # Almacenamos el timestamp en una lista ordenada o Set para que el analizador lo recupere rápido
        self.redis_client.rpush("server_metrics_timeline", json.dumps({
            "request_id": request_id,
            "success": success,
            "timestamp": timestamp
        }))
