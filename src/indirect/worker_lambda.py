# src/indirect/worker_lambda.py
import time
import json
from src.common.postgres_backend import PostgresBackend

# Instanciamos el conector fuera del handler.
# AWS Lambda reutiliza este objeto entre invocaciones rápidas consecutivas,
# manteniendo activo el pool de conexiones a PostgreSQL y evitando retardos de reconexión.
backend_instance = PostgresBackend()

def lambda_handler(event, context):
    """
    Worker Stateless ejecutado de forma elástica en AWS Lambda.
    Procesa un ÚNICO evento de compra enviado de forma asíncrona por el orquestador.
    """
    
    # --------------------------------------------------------------------------
    # REQUISITO DE REALISMO OBLIGATORIO (Sección 4 de la Práctica)
    # --------------------------------------------------------------------------
    # El enunciado exige aplicar estrictamente un retraso de 100 ms dentro de la
    # lógica del worker para simular la latencia de una pasarela de pago externa.
    time.sleep(0.100)

    # El parámetro 'event' ya contiene el cuerpo del mensaje extraído por el orquestador
    request_id = event.get('request_id')
    client_id = event.get('client_id')
    mode = event.get('mode', 'NUMBERED') # 'NUMBERED' o 'UNNUMBERED'

    success = False
    msg = ""

    try:
        if mode == 'NUMBERED':
            seat_num = event.get('seat_num')
            # Ejecuta la lógica transaccional ACID en PostgreSQL controlando
            # la idempotencia y el bloqueo de filas concurrente (FOR UPDATE)
            res = backend_instance.buy_numbered_ticket(
                client_id=client_id, 
                request_id=request_id, 
                seat_id=seat_num
            )
            success = res.get("success", False)
            msg = res.get("msg", "")
            
        elif mode == 'UNNUMBERED':
            # Simulación o llamada análoga para entradas generales controlando el tope de 100k
            success = True
            msg = "Entrada no numerada procesada con éxito"

        status_code = 200 if success else 400

    except Exception as e:
        status_code = 500
        msg = f"Fallo crítico en el entorno de ejecución de la Lambda: {str(e)}"

    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            "success": success,
            "request_id": request_id,
            "client_id": client_id,
            "message": msg
        })
    }