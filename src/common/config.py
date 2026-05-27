"""Configuration module for the ticket system."""
import os

def _split_urls(raw_value: str) -> list[str]:
    """Parse a comma-separated URL list into normalized backend URLs."""
    return [url.strip().rstrip("/") for url in raw_value.split(",") if url.strip()]


# ==============================================================================
# PARÁMETROS GENERALES DEL SISTEMA (100,000 asientos según enunciado)
# ==============================================================================
TOTAL_TICKETS = 100000

# ==============================================================================
# AWS DELIVERY TOPOLOGY (DIRECCIONAMIENTO PRIVADO DE TU LABORATORIO)
# ==============================================================================
REDIS_SERVER_PRIVATE_IP    = "10.0.1.118"
RABBITMQ_SERVER_PRIVATE_IP = "10.0.1.106"
POSTGRES_SERVER_PRIVATE_IP = "172.31.46.203"

# ==============================================================================
# CONFIGURACIÓN DE REDIS (worker-server)
# ==============================================================================
REDIS_HOST     = os.getenv("REDIS_HOST",     REDIS_SERVER_PRIVATE_IP)
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB       = int(os.getenv("REDIS_DB",   0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# ==============================================================================
# CONFIGURACIÓN DE LA ARQUITECTURA DIRECTA (REST)
# ==============================================================================
REST_HOST = os.getenv("REST_HOST", "0.0.0.0")
REST_PORT = int(os.getenv("REST_PORT", 5001))

_default_backends = "http://127.0.0.1:5001,http://127.0.0.1:5002,http://127.0.0.1:5003"
REST_BACKEND_URLS  = _split_urls(os.getenv("REST_BACKEND_URLS", _default_backends))
REST_BENCHMARK_URL = os.getenv("REST_BENCHMARK_URL", f"http://{REDIS_SERVER_PRIVATE_IP}:8000")

# ==============================================================================
# CONFIGURACIÓN DEL BROKER DE MENSAJERÍA (RABBITMQ EN EC2)
# ==============================================================================
RABBITMQ_HOST     = os.getenv("RABBITMQ_HOST",     RABBITMQ_SERVER_PRIVATE_IP)
RABBITMQ_PORT     = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER     = os.getenv("RABBITMQ_USER",     "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST    = os.getenv("RABBITMQ_VHOST",    "/")

# ==============================================================================
# CONFIGURACIÓN DEL ALMACENAMIENTO PERSISTENTE (POSTGRESQL EN EC2)
# ==============================================================================
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     POSTGRES_SERVER_PRIVATE_IP)
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "tickets")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password_segura_sd")

# ==============================================================================
# CONSTANTES PARA EL MODELADO MATEMÁTICO DE ELASTICIDAD (Req 5)
# ==============================================================================
TARGET_RESPONSE_TIME = 2.0   # Tr: Tiempo objetivo máximo para vaciar el backlog (s)
WORKER_CAPACITY      = 10.0  # C: Capacidad de un worker (1 / 0.100s delay = 10 req/s)
MAX_LAMBDA_WORKERS   = 10    # Límite superior de seguridad para créditos en AWS Academy

# ==============================================================================
# AWS LAMBDA SERVERLESS CONFIGURATION
# ==============================================================================
LAMBDA_FUNCTION_NAME = "Khoula-Sofia-TicketWorkerLambda"
AWS_REGION           = "us-east-1"

# ==============================================================================
# LOGGING
# ==============================================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
