# Scalable Concert Ticket Acquisition System

Sistema distribuido de venta de entradas para comparar comunicación directa (`REST`) e indirecta (`RabbitMQ`) bajo carga y contención, con soporte de **escalado elástico automático** y backend **PostgreSQL** mediante **AWS Lambda**.

## Requisitos

- Python 3.12
- Redis
- RabbitMQ
- PostgreSQL (para la arquitectura Lambda)
- Cuenta AWS Academy (para ejecución con Lambda)

Instala las dependencias Python:

```bash
python -m pip install -r requirements.txt
```

> **Nota:** Para la arquitectura Lambda también se necesitan `psycopg2`, `boto3` y `pika`. Instálalos manualmente si no están en `requirements.txt`.

### Topología AWS del laboratorio

| Instancia          | IP privada      | IP pública      | Servicios                                           |
|--------------------|-----------------|-----------------|-----------------------------------------------------|
| `worker-server`    | `10.0.1.118`    | `52.91.229.23`  | Redis, servidores REST, load balancer, workers MQ   |
| `rabbitmq-server`  | `10.0.1.106`    | `34.201.110.76` | RabbitMQ broker                                     |
| `postgres-server`  | `172.31.46.203` | —               | PostgreSQL (backend Lambda)                         |
| `client.1` / `client.2` | —         | —               | Ejecución de benchmarks                             |

Todas las máquinas que ejecuten código Python deben clonar el repo e instalar:

```bash
cd ~/Practica-SD
python -m pip install -r requirements.txt
```

---

## Estructura del proyecto

```text
benchmarks/                          Ficheros de benchmark fijos
scripts/                             Scripts de ayuda para Linux/AWS
  └── setup_rabbit.sh
src/
  common/
    config.py                        Configuración centralizada (IPs, constantes elásticas)
    logger.py                        Logger compartido
    metrics.py                       Recolección de métricas
    redis_backend.py                 Backend Redis (arquitecturas directa e indirecta clásica)
    postgres_backend.py              Backend PostgreSQL con control ACID para Lambda
  direct/
    server.py                        Servidor REST Flask
    load_balancer.py                 Balanceador de carga custom (round-robin)
    client.py                        Cliente benchmark REST
  indirect/
    broker_setup.py                  Configuración de la topología RabbitMQ
    worker.py                        Worker RabbitMQ clásico
    worker_lambda.py                 Handler Lambda stateless (PostgreSQL + idempotencia)
    elastic_launcher.py              Lanzador elástico de workers por profundidad de cola
    client.py                        Cliente benchmark RabbitMQ
  benchmarks/
    backend_scaling_runner.py        Runner de escalado backend (genera plots comparativos)
    comprehensive_benchmark.py       Runner de carga elástica Z(t) y pruebas de stress
    lambda_orchestrator.py           Orquestador elástico AWS Lambda (HTTP Management API)
    analyzer.py                      Análisis estadístico de resultados
    parser.py                        Parser de ficheros de benchmark
    plotter.py                       Generador de gráficos
init_system.py                       Resetea e inicializa el estado Redis
DEPLOYMENT_GUIDE.md                  Guía de despliegue rápido one-command
lambda_function.zip                  Paquete desplegable en AWS Lambda
results/
  reports/                           Informes de benchmark generados
  plots/                             Gráficos PNG generados
```

---

## Modelos de entradas

| Tipo          | Descripción                                          |
|---------------|------------------------------------------------------|
| `unnumbered`  | Máximo 100.000 compras exitosas (contador atómico)   |
| `numbered`    | Asientos `1..20000`, cada asiento se vende una sola vez |

---

## Arquitectura Directa (REST)

Todo el backend directo se ejecuta en `worker-server`.

**1. Arrancar Redis:**
```bash
redis-server
```

**2. Inicializar estado:**
```bash
export REDIS_HOST=10.0.1.118
python init_system.py
```

**3. Arrancar réplicas REST (3 shells separadas):**
```bash
export REDIS_HOST=10.0.1.118
python src/direct/server.py --port 5001
python src/direct/server.py --port 5002
python src/direct/server.py --port 5003
```

**4. Arrancar el balanceador de carga:**
```bash
export REDIS_HOST=10.0.1.118
python src/direct/load_balancer.py --host 0.0.0.0 --port 8000
```

**5. Ejecutar benchmarks desde un cliente:**
```bash
export REDIS_HOST=10.0.1.118
export REST_BENCHMARK_URL=http://10.0.1.118:8000
python src/direct/client.py benchmarks/benchmark_unnumbered_20000.txt --url http://10.0.1.118:8000 --workers 4
python src/direct/client.py benchmarks/benchmark_numbered_60000.txt  --url http://10.0.1.118:8000 --workers 4
```

**Resumen de la arquitectura:**
- Punto de entrada único en `:8000`
- Balanceo de carga server-side con implementación propia (round-robin)
- Réplicas backend locales en `127.0.0.1:5001..5003`

---

## Arquitectura Indirecta (RabbitMQ)

**1. Arrancar RabbitMQ en `rabbitmq-server`:**
```bash
rabbitmq-server
```

**2. Configurar la topología del broker:**
```bash
export RABBITMQ_HOST=10.0.1.106
python src/indirect/broker_setup.py
```

**3. Arrancar workers en `worker-server`:**
```bash
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
python src/indirect/worker.py --worker-id worker-1 --prefetch 100
python src/indirect/worker.py --worker-id worker-2 --prefetch 100
python src/indirect/worker.py --worker-id worker-3 --prefetch 100
```

**4. Ejecutar benchmarks desde un cliente:**
```bash
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
python src/indirect/client.py benchmarks/benchmark_unnumbered_20000.txt --workers 4 --in-flight 100
python src/indirect/client.py benchmarks/benchmark_numbered_60000.txt  --workers 4 --in-flight 100
```

---

## Escalado Elástico Automático

### Elastic Launcher (local, workers en proceso)

Lanza y termina workers dinámicamente según la profundidad de la cola de RabbitMQ, aplicando las fórmulas del Requisito 5:

```
N = max(B·C / Tr ,  λ·T / C)
```

```bash
export RABBITMQ_HOST=10.0.1.106
python src/indirect/elastic_launcher.py --workers 3 --max-workers 10 --poll-interval 5
```

| Parámetro        | Descripción                          | Default |
|------------------|--------------------------------------|---------|
| `--workers`      | Workers iniciales                    | 3       |
| `--max-workers`  | Límite superior de workers           | 10      |
| `--poll-interval`| Intervalo de sondeo (segundos)       | 5       |

---

### Lambda Orchestrator (AWS Lambda + PostgreSQL)

Orquestador elástico que invoca funciones AWS Lambda bajo demanda, monitorizando la cola vía la **HTTP Management API** de RabbitMQ.

**Parámetros de configuración** (`src/common/config.py`):

| Variable               | Valor por defecto                     | Descripción                         |
|------------------------|---------------------------------------|-------------------------------------|
| `TARGET_RESPONSE_TIME` | `2.0` s                               | Tiempo objetivo para vaciar backlog |
| `WORKER_CAPACITY`      | `10.0` req/s                          | Capacidad de un worker Lambda       |
| `MAX_LAMBDA_WORKERS`   | `40`                                  | Límite de seguridad (AWS Academy)   |
| `LAMBDA_FUNCTION_NAME` | `Khoula-Sofia-TicketWorkerLambda`     | Nombre de la función Lambda         |
| `AWS_REGION`           | `us-east-1`                           | Región AWS                          |

**Arrancar el orquestador:**
```bash
export RABBITMQ_HOST=10.0.1.106
python src/benchmarks/lambda_orchestrator.py
```

**Worker Lambda (`src/indirect/worker_lambda.py`):**
- Handler stateless invocado en modo `Event` (asíncrono)
- Usa `PostgresBackend` para garantizar **ACID**, **idempotencia** (`SELECT ... WHERE request_id`) y **control de concurrencia** (`SELECT ... FOR UPDATE`)
- Aplica `time.sleep(0.100)` simulando latencia de pasarela de pago (Requisito 4)

**Despliegue del paquete Lambda:**
```bash
# El paquete ya está preparado en lambda_function.zip
aws lambda update-function-code \
  --function-name Khoula-Sofia-TicketWorkerLambda \
  --zip-file fileb://lambda_function.zip \
  --region us-east-1
```

---

## Correctitud esperada

| Tipo        | Éxitos esperados | Restricciones                      |
|-------------|------------------|------------------------------------|
| `unnumbered`| 20.000 (o 100.000)| 0 duplicados                      |
| `numbered`  | ≤ 20.000         | Sin asientos duplicados            |

**Verificación rápida con Redis:**
```bash
python - <<'PY'
from src.common.redis_backend import RedisBackend
backend = RedisBackend()
sold = backend.get_sold_seats()
print("stats:", backend.get_stats())
print("duplicates?:", len(sold) != len(set(sold)))
PY
```

---

## Benchmarking

### Escalado de backends (plots comparativos)

Registra puntos de escalado REST desde un cliente VM. Repite cambiando el número de réplicas activas:

```bash
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
export REST_BENCHMARK_URL=http://10.0.1.118:8000
python src/benchmarks/backend_scaling_runner.py --architecture rest     --ticket-type both --backend-count 1 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_runner.py --architecture rest     --ticket-type both --backend-count 2 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_runner.py --architecture rest     --ticket-type both --backend-count 3 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_runner.py --architecture rabbitmq --ticket-type both --backend-count 1 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_runner.py --architecture rabbitmq --ticket-type both --backend-count 2 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_Runner.py --architecture rabbitmq --ticket-type both --backend-count 3 --client-workers 4 --label aws_backend
```

Usar el mismo `--label` para ambas arquitecturas añade datos a la misma serie y regenera los plots comparativos.

### Benchmark de carga elástica Z(t) y stress

Implementa el patrón de carga variable del Requisito 6 (baja carga → rampa → pico → sostenido → enfriamiento):

```bash
# Test elástico Z(t)
python src/benchmarks/comprehensive_benchmark.py \
  --url http://10.0.1.118:8000 \
  --architecture rest \
  --ticket-type unnumbered \
  --test-type elastic \
  --base-rate 10 \
  --duration 300 \
  --output results/elastic_results.json

# Test de stress (Requisito 7)
python src/benchmarks/comprehensive_benchmark.py \
  --test-type stress \
  --base-rate 10 \
  --max-rate 100 \
  --output results/stress_results.json
```

### Benchmark de hotspot

```bash
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
export REST_BENCHMARK_URL=http://10.0.1.118:8000
python src/benchmarks/backend_scaling_runner.py --architecture rest     --ticket-type numbered --backend-count 3 --client-workers 4 --numbered-benchmark benchmarks/benchmark_numbered_hotspot_25997.txt --label aws_hotspot_backend
python src/benchmarks/backend_scaling_runner.py --architecture rabbitmq --ticket-type numbered --backend-count 3 --client-workers 4 --numbered-benchmark benchmarks/benchmark_numbered_hotspot_25997.txt --label aws_hotspot_backend
```

Genera ficheros como:
- `results/reports/benchmark_summary_hotspot_*.txt`
- `results/plots/rest_numbered_scalability_hotspot.png`
- `results/plots/comparison_numbered_hotspot.png`

---

## Configuración AWS

Los valores por defecto en `src/common/config.py` están alineados con la topología del laboratorio:

| Variable               | Valor por defecto       |
|------------------------|-------------------------|
| `REDIS_HOST`           | `10.0.1.118`            |
| `RABBITMQ_HOST`        | `10.0.1.106`            |
| `POSTGRES_HOST`        | `172.31.46.203`         |
| `REST_BENCHMARK_URL`   | `http://10.0.1.118:8000`|
| `REST_BACKEND_URLS`    | `127.0.0.1:5001..5003`  |

Si AWS rota las IPs, actualiza `src/common/config.py` o sobreescribe con variables de entorno antes de lanzar los procesos.

---

## Escalado dinámico en tiempo real

### Directa (REST)
En `worker-server`, arranca con uno o dos servidores REST detrás del load balancer y añade otro proceso `server.py` durante la ejecución:
```bash
python src/direct/server.py --port 5004  # añadido en caliente
```

### Indirecta (RabbitMQ clásico)
Lanza el benchmark con un worker y añade más mientras el cliente sigue corriendo:
```bash
python src/indirect/worker.py --worker-id worker-4 --prefetch 100
```

### Indirecta (Lambda elástico)
El `lambda_orchestrator.py` gestiona el escalado automáticamente monitorizando la HTTP Management API de RabbitMQ cada 2 segundos.

---

## Resultados

Los resultados se guardan bajo:

```
results/
  reports/    → Informes de texto con métricas por benchmark
  plots/      → Gráficos PNG de throughput, latencia y escalabilidad
```

---

## Notas

- Usa AWS Academy / lab VMs para las ejecuciones de evaluación final.
- La arquitectura directa usa el load balancer Python en `:8000` como único punto de entrada oficial.
- La arquitectura indirecta admite ajuste con `--workers`, `--in-flight` y `--prefetch`.
- La carpeta `scripts/` solo contiene helpers de arranque para Linux/AWS.
- El `lambda_function.zip` contiene el paquete `worker_lambda.py` + `postgres_backend.py` + `psycopg2` listo para desplegarse en AWS Lambda.
