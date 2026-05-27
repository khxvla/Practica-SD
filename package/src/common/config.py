"""Configuration module for the ticket system."""
import os

def _split_urls(raw_value: str) -> list[str]:
    """Parse a comma-separated URL list into normalized backend URLs."""
    return [url.strip().rstrip("/") for url in raw_value.split(",") if url.strip()]


# ==============================================================================
# PARÁMETROS GENERALES DEL SISTEMA (Aumentado a 100,000 asientos por enunciado)
# ==============================================================================
TOTAL_TICKETS = 100000

# ==============================================================================
# AWS DELIVERY TOPOLOGY (DIRECCIONAMIENTO PRIVADO DE TU LABORATORIO)
# ==============================================================================
# Usamos las IPs privadas estables dentro de tu VPC de AWS Academy
RABBITMQ_SERVER_PRIVATE_IP = "10.0.1.106"
POSTGRES_SERVER_PRIVATE_IP = "172.31.46.203"  # <-- Tu nueva IP privada mapeada aquí

# ==============================================================================
# CONFIGURACIÓN DEL BROKER DE MENSAJERÍA (RABBITMQ EN EC2)
# ==============================================================================
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", RABBITMQ_SERVER_PRIVATE_IP)
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

# ==============================================================================
# NUEVA CONFIGURACIÓN DEL ALMACENAMIENTO PERSISTENTE (POSTGRESQL EN EC2)
# ==============================================================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", POSTGRES_SERVER_PRIVATE_IP)
POSTGRES_DB = os.getenv("POSTGRES_DB", "tickets")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password_segura_sd")

# ==============================================================================
# CONSTANTES PARA EL MODELADO MATEMÁTICO DE ELASTICIDAD
# ==============================================================================
TARGET_RESPONSE_TIME = 2.0  # Tr: Tiempo objetivo máximo para vaciar el backlog (segundos)
WORKER_CAPACITY = 10.0      # C: Capacidad de un worker (1 segundo / 0.100s de delay = 10 req/s)
MAX_LAMBDA_WORKERS = 10     # Límite superior de seguridad para créditos en AWS Academy

# AWS Lambda Serverless configuration
LAMBDA_FUNCTION_NAME = "Khoula-Sofia-TicketWorkerLambda"
AWS_REGION = "us-east-1"

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")