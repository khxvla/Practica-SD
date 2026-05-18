# src/direct/client.py
"""
REST Client for Direct Communication Architecture with Time-Varying Workload Z(t).
Este módulo actúa como el cliente de inyección de carga síncrona directa sobre HTTP.
Simula de forma paralela hilos de clientes que atacan las réplicas REST (Waitress),
aplicando la curva variable en el tiempo Z(t) requerida por el Requisito 6.
"""

import sys
import os
import time
import threading
import requests
import json
import uuid  # Importado para la generación aleatoria de UUIDs únicos para idempotencia
import random  # Importado para la selección aleatoria de asientos en el hot-spot
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
from requests.adapters import HTTPAdapter

# Inyectar de forma segura la raíz del proyecto para localizar el módulo base 'src'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.metrics import MetricsCollector
from src.common.config import REST_BENCHMARK_URL as REST_CLIENT_URL
from src.benchmarks.parser import BenchmarkParser

# Inicializar el gestor de trazas unificado
logger = get_logger(__name__)

class RestClient:
    """Cliente HTTP de alto rendimiento para interactuar directamente con el sistema REST."""
    
    def __init__(self, base_url=REST_CLIENT_URL, timeout=60, max_retries=5):
        """ Inicializa el cliente REST configurando pools y verificando conectividad. """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max(1, max_retries)
        # Utilizar thread local storage para que los hilos concurrentes no compartan sockets corruptos
        self._thread_local = threading.local()
        
        # Verificar que el balanceador o la réplica responde antes de iniciar el estrés
        if not self._check_health():
            logger.warning(f"El servidor central en {base_url} podría no estar respondiendo.")

    def _get_session(self) -> requests.Session:
        """Retorna una sesión HTTP persistente (Connection Pooling) exclusiva por cada hilo client."""
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            # Mantener 32 sockets abiertos concurrentemente en el pool para evitar retardos de handshake TCP
            adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._thread_local.session = session
        return session

    def _check_health(self) -> bool:
        """Verifica la disponibilidad de salud del nodo remoto."""
        try:
            resp = self._get_session().get(f"{self.base_url}/health", timeout=5)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Fallo en el Health check de la réplica: {e}")
            return False

    def _request_with_retries(self, method: str, url: str, **kwargs):
        """Mecanismo tolerante a fallos (Punto 10) que reintenta peticiones ante fallos de red esporádicos."""
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._get_session().request(method=method, url=url, **kwargs)
                # Si el servidor responde un código controlado (menor a 500), la petición es válida
                if response.status_code < 500:
                    return response

                last_exception = RuntimeError(f"HTTP {response.status_code}")
                logger.debug(f"Fallo esporádico del servidor {response.status_code} (Intento {attempt}/{self.max_retries})")
            except requests.RequestException as exc:
                last_exception = exc
                logger.debug(f"Error de transporte de red (Intento {attempt}/{self.max_retries}): {exc}")

            # Retroceso exponencial elástico (backoff sleep) entre reintentos para no saturar los hilos del servidor
            if attempt < self.max_retries:
                time.sleep(min(0.05 * attempt, 0.25))

        if last_exception:
            raise last_exception
        raise RuntimeError("La solicitud HTTP ha fallado definitivamente tras agotar reintentos")

    def buy_unnumbered(self, client_id: str, request_id: str) -> Tuple[bool, float]:
        """Envía una solicitud POST síncrona para adquirir un boleto general sin numerar."""
        start_time = time.time()
        try:
            payload = {
                "client_id": str(client_id),
                "request_id": str(request_id),
                "mode": "UNNUMBERED"
            }
            
            # Lanzar la petición HTTP al endpoint genérico de compras sin numerar
            resp = self._request_with_retries(
                "POST",
                f"{self.base_url}/api/buy/unnumbered",
                json=payload,
                timeout=self.timeout,
            )
            
            latency = time.time() - start_time
            if resp.status_code == 200:
                data = resp.json()
                return data.get("success", False), latency
            else:
                return False, latency
        except Exception as e:
            latency = time.time() - start_time
            logger.debug(f"Compra unnumbered fallida por excepción: {e}")
            return False, latency

    def buy_numbered(self, seat_id: int, client_id: str, request_id: str) -> Tuple[bool, float]:
        """Envía una solicitud POST síncrona directa para adquirir un asiento numerado específico (1-100,000)."""
        start_time = time.time()
        try:
            payload = {
                "client_id": str(client_id),
                "request_id": str(request_id),
                "mode": "NUMBERED"
            }
            
            # Modificado de forma quirúrgica para apuntar al endpoint relacional de PostgreSQL en el servidor directo
            resp = self._request_with_retries(
                "POST",
                f"{self.base_url}/api/buy/numbered/{seat_id}",
                json=payload,
                timeout=self.timeout,
            )
            
            latency = time.time() - start_time
            if resp.status_code == 200:
                data = resp.json()
                return data.get("success", False), latency
            else:
                return False, latency
        except Exception as e:
            latency = time.time() - start_time
            logger.debug(f"Compra numbered fallida por excepción: {e}")
            return False, latency

    def get_stats(self):
        """Interroga al clúster para extraer el estado actual de las métricas en caliente de PostgreSQL."""
        try:
            resp = self._request_with_retries("GET", f"{self.base_url}/api/stats", timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception as e:
            logger.error(f"Fallo al extraer estadísticas del servidor: {e}")
            return None

    def reset(self):
        """Llama al endpoint administrativo de inicialización atómica de PostgreSQL para purgar los benchmarks."""
        try:
            resp = self._request_with_retries("POST", f"{self.base_url}/api/admin/reset", timeout=self.timeout)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Fallo al resetear la base de datos relacional: {e}")
            return False


class RestBenchmarkRunner:
    """Orquestador encargado de ejecutar la carga variable Z(t) usando los recolectores de métricas."""
    
    def __init__(self, base_url=REST_CLIENT_URL, num_workers=10):
        """Inicializa el inyector fijando el pool de hilos concurrentes concurrentes para las solicitudes."""
        self.client = RestClient(base_url)
        self.num_workers = num_workers
        self.metrics = MetricsCollector()

    def run_elastic_benchmark(self) -> MetricsCollector:
        """
        EJECUCIÓN DE LA CARGA VARIABLE EN EL TIEMPO Z(t) (Requisito Funcional 6).
        Sustituye la lectura fija secuencial por un perfil de carga dinámico
        con fases controladas de Valle, Rampa ascendente y Pico masivo.
        """
        logger.info("=============================================================")
        logger.info(f" INICIANDO BENCHMARK DIRECTO REST CON PERFIL DE CARGA Z(t)")
        logger.info(f" Concurrencia máxima asignada del cliente: {self.num_workers} hilos")
        logger.info("=============================================================")
        
        # Purgar y limpiar el estado de PostgreSQL antes de iniciar el muestreo temporal
        logger.info("Reiniciando el estado del almacenamiento en PostgreSQL...")
        if not self.client.reset():
            raise RuntimeError("Fallo crítico: No se pudo resetear la base de datos remota")
        time.sleep(0.5)
        
        # Arrancar el cronómetro del MetricsCollector nativo
        self.metrics.start()
        
        # Instanciar el ejecutor concurrente por hilos para escupir peticiones concurrentes HTTP
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            client_id = f"client-direct-runner"

            # ------------------------------------------------------------------
            # FASE 1: Low load phase (Valle) -> Carga mínima constante
            # ------------------------------------------------------------------
            logger.info("[Z(t) - Fase 1] Ejecutando fase estable de carga baja (Valle)...")
            for _ in range(10):
                # Generar credenciales de idempotencia únicas en cada solicitud
                req_id = str(uuid.uuid4())
                seat_num = random.randint(1, 100000) # Requisito 2: Rango ampliado a 100k
                executor.submit(self._execute_numbered, seat_num, client_id, req_id)
                time.sleep(0.5)  # Intervalo de separación amplio en valle

            # ------------------------------------------------------------------
            # FASE 2: Gradual ramp-up -> Incremento secuencial acelerado de peticiones
            # ------------------------------------------------------------------
            logger.info("[Z(t) - Fase 2] Iniciando rampa ascendente gradual (Ramp-up)...")
            for rampa in range(1, 6):
                intensidad_rafaga = rampa * 15
                logger.info(f"  -> Escalón de rampa {rampa}: Despachando {intensidad_rafaga} peticiones/seg")
                for _ in range(intensidad_rafaga):
                    req_id = str(uuid.uuid4())
                    seat_num = random.randint(1, 100000)
                    executor.submit(self._execute_numbered, seat_num, client_id, req_id)
                time.sleep(1.0) # Esperar al siguiente escalón temporal

            # ------------------------------------------------------------------
            # FASE 3: Sudden spikes -> Inyección atómica e instantánea de un pico de estrés
            # ------------------------------------------------------------------
            logger.info("[Z(t) - Fase 3] ¡ALERTA! Inyectando ráfaga crítica de saturación (Sudden Spike)...")
            for _ in range(250):
                req_id = str(uuid.uuid4())
                seat_num = random.randint(1, 100000)
                executor.submit(self._execute_numbered, seat_num, client_id, req_id)
            time.sleep(1.0) # Ventana de estabilización posterior al pico

            # ------------------------------------------------------------------
            # FASE 4: Sustained high load (Hotspot / Meseta Sostenida)
            # ------------------------------------------------------------------
            # Simula el escenario Hotspot (Punto 8): El 80% de peticiones van al 5% de asientos de alta demanda
            logger.info("[Z(t) - Fase 4] Manteniendo meseta alta con escenario de contención (Hotspot Load)...")
            for _ in range(5):
                for _ in range(40):
                    req_id = str(uuid.uuid4())
                    # Aplicar la regla del Hotspot 80/20: Forzar colisiones en los primeros 5,000 asientos (5% de 100k)
                    if random.random() < 0.80:
                        seat_num = random.randint(1, 5000)
                    else:
                        seat_num = random.randint(5001, 100000)
                    executor.submit(self._execute_numbered, seat_num, client_id, req_id)
                time.sleep(1.0)

            # ------------------------------------------------------------------
            # FASE 5: Cool-down phase -> Parada absoluta para evaluar el enfriamiento del sistema
            # ------------------------------------------------------------------
            logger.info("[Z(t) - Fase 5] Final de inyecciones. Entrando en fase de enfriamiento (Cool-down)...")

        # Detener los contadores globales del MetricsCollector
        self.metrics.end()

        # Extraer e imprimir el estado final consolidado en el disco relacional
        final_db_state = self.client.get_stats()
        logger.info(f"Estado de ocupación de auditoría en PostgreSQL: {final_db_state}")

        # Mostrar el resumen estadístico nativo por pantalla (Percentiles p50, p95, p99 requeridos por Punto 9)
        self.metrics.print_summary()
        return self.metrics

    def run_benchmark(self, benchmark_file: str) -> MetricsCollector:
        """Run a fixed benchmark file through the REST architecture.

        Reads operations from *benchmark_file* and submits them concurrently
        using the thread pool.  This is the interface expected by
        BackendScalingSuite in backend_scaling_runner.py.
        """
        from src.benchmarks.parser import BenchmarkParser

        logger.info("Starting REST benchmark from %s", benchmark_file)
        parser = BenchmarkParser(benchmark_file)
        operations = parser.parse()
        if not operations:
            logger.warning("No operations found in %s", benchmark_file)
            return self.metrics

        self.metrics = MetricsCollector()
        self.metrics.start()

        client_id = f"rest-bench-{os.getpid()}"

        def submit_op(op: dict):
            op_type = op.get("type", "unnumbered")
            req_id = str(uuid.uuid4())
            if op_type == "numbered":
                seat_id = op.get("seat_id", random.randint(1, 100000))
                success, latency = self.client.buy_numbered(seat_id, client_id, req_id)
            else:
                success, latency = self.client.buy_unnumbered(client_id, req_id)
            self.metrics.record_operation(op_type, success, latency)

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(submit_op, op) for op in operations]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    logger.error("Operation failed: %s", exc)

        self.metrics.end()
        self.metrics.print_summary()
        return self.metrics

    def _execute_unnumbered(self, client_id: str, request_id: str):
        """Helper para empaquetar y medir operaciones generales sin numerar."""
        success, latency = self.client.buy_unnumbered(client_id, request_id)
        self.metrics.record_operation("unnumbered", success, latency)

    def _execute_numbered(self, seat_id: int, client_id: str, request_id: str):
        """Helper para empaquetar y medir operaciones directas numeradas."""
        success, latency = self.client.buy_numbered(seat_id, client_id, request_id)
        self.metrics.record_operation("numbered", success, latency)



def main():
    """Punto de entrada principal para el benchmark directo elástico."""
    import argparse
    
    parser = argparse.ArgumentParser(description="REST Elastic Workload Z(t) Client")
    parser.add_argument("--url", default=REST_CLIENT_URL, help="URL base del balanceador de la práctica")
    # Configurar por defecto 16 hilos del cliente para inyectar suficiente carga paralela por HTTP
    parser.add_argument("--workers", type=int, default=16, help="Hilos concurrentes de inyección paralela")
    
    args = parser.parse_args()
    
    # Instanciar el inyector y disparar la simulación de elasticidad directa
    runner = RestBenchmarkRunner(args.url, args.workers)
    metrics = runner.run_elastic_benchmark()
    
    return 0 if metrics.total_operations > 0 else 1

if __name__ == "__main__":
    sys.exit(main())