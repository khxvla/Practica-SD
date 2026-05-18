# src/common/postgres_backend.py
import psycopg2
from psycopg2 import extras
from src.common.config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

class PostgresBackend:
    def __init__(self):
        """Establece la conexión de red privada con el clúster relacional en EC2."""
        self.conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        # DESACTIVAR AUTOCOMMIT: Es obligatorio para controlar nosotros de forma atómica
        # cuándo se inicia y cuándo se guarda la transacción en la base de datos.
        self.conn.autocommit = False

    def buy_numbered_ticket(self, client_id, request_id, seat_id):
        """Procesa la compra transaccional controlando la concurrencia y la idempotencia."""
        cursor = self.conn.cursor(cursor_factory=extras.DictCursor)
        try:
            # 1. CONTROL DE IDEMPOTENCIA (Requisito 10):
            # Si el request_id ya existe en la base de datos, significa que ya fue procesado.
            # Respondemos éxito inmediatamente sin duplicar la compra del asiento.
            cursor.execute("SELECT id FROM transactions WHERE request_id = %s", (request_id,))
            if cursor.fetchone():
                return {"success": True, "msg": "Petición repetida detectada (Idempotencia)"}

            # 2. PREVENCIÓN DE CONDICIONES DE CARRERA (Race Conditions - Requisito 2 y 8):
            # Usamos FOR UPDATE para bloquear de forma pesimista la fila del asiento número 'seat_id'.
            # Ningún otro worker concurrente podrá leer o pisar este asiento hasta que terminemos.
            cursor.execute("SELECT status FROM seats WHERE seat_number = %s FOR UPDATE", (seat_id,))
            seat = cursor.fetchone()

            if not seat or seat['status'] != 'available':
                # Si el asiento ya está ocupado, cancelamos la transacción (rollback) y liberamos el bloqueo
                self.conn.rollback()
                return {"success": False, "msg": f"El asiento {seat_id} ya se encuentra vendido o es inválido"}

            # 3. GUARDAR LOS DATOS DE FORMA ATÓMICA (Requisito 9):
            # Si estaba disponible, actualizamos el inventario y registramos la transacción.
            # Guardamos el timestamp usando NOW() del propio servidor para evitar sesgos.
            cursor.execute("UPDATE seats SET status = 'sold' WHERE seat_number = %s", (seat_id,))
            cursor.execute(
                "INSERT INTO transactions (request_id, client_id, seat_number, completed_at) VALUES (%s, %s, %s, NOW())",
                (request_id, client_id, seat_id)
            )
            
            # Confirmamos definitivamente los cambios en el disco duro de la base de datos
            self.conn.commit()
            return {"success": True, "msg": "Compra de asiento registrada con éxito"}

        except Exception as e:
            # Si hay un error de conexión, de sintaxis o de red, deshacemos todo para mantener el sistema consistente
            self.conn.rollback()
            return {"success": False, "msg": f"Rollback transaccional debido al error: {str(e)}"}
        finally:
            cursor.close()

    def close(self):
        """Cierra el pool de conexiones de forma limpia."""
        self.conn.close()