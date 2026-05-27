import time
import json
import pika
import os
from src.common.postgres_backend import PostgresBackend

# Instanciamos el conector fuera del handler para aprovechar la reutilización de contenedores
backend_instance = PostgresBackend()

def lambda_handler(event, context):
    """
    Worker Stateless elástico.
    Se conecta a RabbitMQ, extrae UN solo mensaje (compra), lo procesa y termina.
    """
    # 1. Conexión directa a tu RabbitMQ en la instancia EC2
    rabbitmq_host = os.environ.get('RABBITMQ_HOST')
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
    channel = connection.channel()
    
    # 2. Extraer manualmente UN mensaje de la cola
    method_frame, header_frame, body = channel.basic_get(queue='requests')
    
    if method_frame:
        # ¡Aquí es donde sacamos los datos reales del cliente!
        mensaje = json.loads(body.decode('utf-8'))
        request_id = mensaje.get('request_id')
        client_id = mensaje.get('client_id')
        mode = mensaje.get('mode', 'NUMBERED')
        seat_num = mensaje.get('seat_num')
        
        # 3. Retardo obligatorio (Simulación pasarela pago)
        time.sleep(0.100)

        success = False
        msg = ""
        try:
            # 4. Lógica transaccional ACID en BD
            if mode == 'NUMBERED':
                res = backend_instance.buy_numbered_ticket(
                    client_id=client_id, 
                    request_id=request_id, 
                    seat_id=seat_num
                )
                success = res.get("success", False)
                msg = res.get("msg", "")
            else:
                success = True
                msg = "Entrada no numerada procesada con éxito"

            # 5. Confirmamos a RabbitMQ que el mensaje se ha procesado con éxito (ACK)
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as e:
            # Tolerancia a fallos: Devolvemos el mensaje a la cola (NACK) si la BD explota
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
            success = False
            msg = f"Fallo en BD: {str(e)}"
            
        connection.close()
        return {"statusCode": 200, "body": json.dumps({"success": success, "message": msg})}

    else:
        # La Lambda se despertó pero la cola ya estaba vacía (otro worker fue más rápido)
        connection.close()
        return {"statusCode": 204, "body": "No messages in queue"}