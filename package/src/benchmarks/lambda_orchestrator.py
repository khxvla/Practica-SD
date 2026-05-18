"""Dynamic Elasticity Orchestrator. Evaluates RabbitMQ Backlog using Section 5 formulas."""
import sys
import os
import time
import requests
import boto3
import pika
import json
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.config import (
    RABBITMQ_HOST,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    TARGET_RESPONSE_TIME,
    WORKER_CAPACITY,
    MAX_LAMBDA_WORKERS,
    LAMBDA_FUNCTION_NAME,
    AWS_REGION
)

logger = get_logger(__name__)

# Configuración de RabbitMQ Management Plugin HTTP API
RABBITMQ_API_URL = f"http://{RABBITMQ_HOST}:15672/api/queues/%2f/requests"
HTTP_AUTH = (RABBITMQ_USER, RABBITMQ_PASSWORD)

# Inicialización de Boto3 para invocar AWS Lambdas bajo demanda
lambda_client = boto3.client('lambda', region_name=AWS_REGION)

def fetch_queue_state() -> tuple[int, float]:
    """Extrae métricas del broker en tiempo real: Backlog (B) y Tasa de Llegada (lambda)"""
    try:
        response = requests.get(RABBITMQ_API_URL, auth=HTTP_AUTH, timeout=2)
        if response.status_code == 200:
            info = response.json()
            backlog = info.get("messages", 0)
            arrival_rate = info.get("messages_details", {}).get("rate", 0.0)
            return backlog, arrival_rate
    except Exception as e:
        logger.error(f"Fallo de conexión con HTTP Management API de RabbitMQ: {e}")
    return 0, 0.0

def scale_up_cluster(count: int):
    """Invoca asíncronamente nuevas funciones Stateless Lambda en AWS."""
    logger.info(f"🚀 [ELASTIC SCALE-UP] Invocando {count} nuevas instancias en AWS Lambda...")
    for _ in range(count):
        try:
            lambda_client.invoke(
                FunctionName=LAMBDA_FUNCTION_NAME,
                InvocationType='Event'  # ¡CRUCIAL! El hilo no bloquea esperando el retorno
            )
        except Exception as e:
            logger.error(f"Error crítico al solicitar escalado a AWS Lambda: {e}")

def scale_down_cluster(count: int):
    """Inyecta señales QUIT. El primer worker libre la procesará y se auto-eliminará de forma limpia."""
    logger.info(f"📉 [ELASTIC SCALE-DOWN] Inyectando {count} señales QUIT en el bus...")
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
        channel = connection.channel()
        
        for _ in range(count):
            quit_signal = {"type": "QUIT", "action": "QUIT"}
            channel.basic_publish(
                exchange="",
                routing_key="requests",
                body=json.dumps(quit_signal),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        connection.close()
    except Exception as e:
        logger.error(f"Error inyectando señales de reducción elástica: {e}")

def monitor_and_scale_loop():
    logger.info("======================================================")
    logger.info("   INICIANDO ORQUESTRADOR ELÁSTICO MATEMÁTICO DELTA   ")
    logger.info("======================================================")
    
    estimated_active_workers = 0
    
    while True:
        backlog, arrival_rate = fetch_queue_state()
        logger.info(f"📊 [Métricas] Backlog (B): {backlog} msg | Tasa de llegada (λ): {arrival_rate:.2f} msg/s")
        
        # === SECCIÓN 5: IMPLEMENTACIÓN DE LAS ECUACIONES DE REVALORIZACIÓN ===
        if backlog > 0:
            # Escalado basado en Backlog acumulado: N = B / (Tr * C)
            computed_target = math.ceil(backlog / (TARGET_RESPONSE_TIME * WORKER_CAPACITY))
            logger.info(f"-> Ley de Control por Backlog: N = {backlog} / ({TARGET_RESPONSE_TIME} * {WORKER_CAPACITY})")
        else:
            # Escalado basado en Tasa de llegada en flujo continuo: N = λ / C
            computed_target = math.ceil(arrival_rate / WORKER_CAPACITY)
            logger.info(f"-> Ley de Control por Tasa de Entrada: N = {arrival_rate:.2f} / {WORKER_CAPACITY}")

        # Acotar los límites de seguridad configurados
        target_workers = max(0, min(computed_target, MAX_LAMBDA_WORKERS))
        logger.info(f" Concurrencia Deseada: {target_workers} | Concurrencia Estimada: {estimated_active_workers}")

        # Ejecución elástica
        if target_workers > estimated_active_workers:
            diff = target_workers - estimated_active_workers
            scale_up_cluster(diff)
            estimated_active_workers = target_workers
        elif target_workers < estimated_active_workers:
            diff = estimated_active_workers - target_workers
            scale_down_cluster(diff)
            estimated_active_workers = target_workers

        # Bucle de realimentación cada 2 segundos (Control loop window)
        time.sleep(2)

if __name__ == "__main__":
    try:
        monitor_and_scale_loop()
    except KeyboardInterrupt:
        logger.info("Orquestador apagado manualmente.")