# Scalable Concert Ticket Acquisition System

Distributed ticket-selling system for comparing direct communication (`REST`) and indirect communication (`RabbitMQ`) under load and contention.

## Requirements

- Python 3.12
- Redis
- RabbitMQ
Install Python dependencies:

```bash
python -m pip install -r requirements.txt
```

For the AWS delivery, services are split across the VPC machines:

- `worker-server` (`10.0.1.118` private, `52.91.229.23` public): Redis, direct REST servers, direct load balancer, indirect workers
- `rabbitmq-server` (`10.0.1.106` private, `34.201.110.76` public): RabbitMQ broker
- `client.1` / `client.2`: benchmark execution

Every machine that runs Python code should use the same repo and install:

```bash
cd ~/Practica-SD
python -m pip install -r requirements.txt
```

## Project Layout

```text
benchmarks/               Fixed benchmark files
scripts/                  Linux/AWS helper scripts
src/common/               Shared config, metrics, Redis backend, logging
src/direct/               REST implementation
src/indirect/             RabbitMQ implementation
src/benchmarks/           Backend-scaling runner, parser, analyzer, plots
init_system.py            Reset and initialize Redis state
```

## Ticket Models

- `unnumbered`: at most 20,000 successful purchases
- `numbered`: seats `1..20000`, each seat sold at most once

## Direct Architecture

For the delivery, the whole direct backend runs inside `worker-server`.

Start Redis on `worker-server`:

```bash
cd ~/Practica-SD
redis-server
```

Initialize state on `worker-server`:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
python init_system.py
```

Start the three REST replicas on `worker-server` in separate shells:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
python src/direct/server.py --port 5001
python src/direct/server.py --port 5002
python src/direct/server.py --port 5003
```

Start the load balancer on `worker-server`:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
python src/direct/load_balancer.py --host 0.0.0.0 --port 8000
```

Run direct benchmark executions from a client VM:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
export REST_BENCHMARK_URL=http://10.0.1.118:8000
python src/direct/client.py benchmarks/benchmark_unnumbered_20000.txt --url http://10.0.1.118:8000 --workers 4
python src/direct/client.py benchmarks/benchmark_numbered_60000.txt --url http://10.0.1.118:8000 --workers 4
```

This is the official direct architecture path for the project:

- single entry point on `:8000`
- server-side load balancing
- custom load balancer implementation, which is explicitly allowed by the assignment
- backend replicas stay local to `worker-server`, so the load balancer talks to `127.0.0.1:5001..5003`


## Indirect Architecture

Start RabbitMQ on `rabbitmq-server`:

```bash
cd ~/Practica-SD
rabbitmq-server
```

Set up the broker topology on `rabbitmq-server`:

```bash
cd ~/Practica-SD
export RABBITMQ_HOST=10.0.1.106
python src/indirect/broker_setup.py
```

Start the indirect workers on `worker-server` in separate shells:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
python src/indirect/worker.py --worker-id worker-1 --prefetch 100
python src/indirect/worker.py --worker-id worker-2 --prefetch 100
python src/indirect/worker.py --worker-id worker-3 --prefetch 100
```

Run indirect benchmark executions from a client VM:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
python src/indirect/client.py benchmarks/benchmark_unnumbered_20000.txt --workers 4 --in-flight 100
python src/indirect/client.py benchmarks/benchmark_numbered_60000.txt --workers 4 --in-flight 100
```

## Expected Correctness

- `unnumbered`: `20000` successes, `0` failures
- `numbered`: at most `20000` successes, no duplicated seats

Quick Redis check:

```bash
python - <<'PY'
from src.common.redis_backend import RedisBackend
backend = RedisBackend()
sold = backend.get_sold_seats()
print("stats:", backend.get_stats())
print("duplicates?:", len(sold) != len(set(sold)))
PY
```

## Benchmarking

With `src/benchmarks/runner.py` removed, the benchmark workflow is:

- use `src/direct/client.py` and `src/indirect/client.py` for one-off executions
- use `src/benchmarks/backend_scaling_runner.py` to accumulate AWS data points and regenerate the plots

Record REST backend-scaling points from a client VM. Repeat after changing the number of active REST replicas on `worker-server`:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
export REST_BENCHMARK_URL=http://10.0.1.118:8000
python src/benchmarks/backend_scaling_runner.py --architecture rest --ticket-type both --backend-count 1 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_runner.py --architecture rest --ticket-type both --backend-count 2 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_runner.py --architecture rest --ticket-type both --backend-count 3 --client-workers 4 --label aws_backend
```

Record RabbitMQ backend-scaling points from a client VM. Repeat after changing the number of active workers on `worker-server`:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
export REST_BENCHMARK_URL=http://10.0.1.118:8000
python src/benchmarks/backend_scaling_runner.py --architecture rabbitmq --ticket-type both --backend-count 1 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_runner.py --architecture rabbitmq --ticket-type both --backend-count 2 --client-workers 4 --label aws_backend
python src/benchmarks/backend_scaling_runner.py --architecture rabbitmq --ticket-type both --backend-count 3 --client-workers 4 --label aws_backend
```

Using the same `--label` for both architectures appends everything to the same series and regenerates the comparison plots.

To generate the plots correctly:

- run the benchmark commands from `client.1` or `client.2`
- start Redis on `worker-server` first
- start RabbitMQ and `src/indirect/broker_setup.py` on `rabbitmq-server`
- for `rest`, start `5001`, `5002`, `5003` and `src/direct/load_balancer.py --port 8000` on `worker-server`
- for `rabbitmq`, start the `src/indirect/worker.py` processes on `worker-server`
- update `--backend-count` so it matches the number of active REST replicas or RabbitMQ workers for that run
- keep the services running for the whole benchmark execution
- check the generated files in `results/reports/` and `results/plots/`

Results are saved under:

- `results/reports/`
- `results/plots/`

## AWS Delivery Configuration

Current AWS topology used for the delivery:

- `worker-server`: private `10.0.1.118`, public `52.91.229.23`
- `rabbitmq-server`: private `10.0.1.106`, public `34.201.110.76`
- benchmarks run from the client instances inside the same VPC

The repository defaults in `src/common/config.py` are aligned with that setup:

- `REDIS_HOST=10.0.1.118`
- `RABBITMQ_HOST=10.0.1.106`
- `REST_BENCHMARK_URL=http://10.0.1.118:8000`
- `REST_BACKEND_URLS` stays local (`127.0.0.1:5001..5003`) because the direct REST servers and load balancer run on the same `worker-server`

If AWS rotates the IPs, update `src/common/config.py` or override them with environment variables before launching the processes.

## Additional Requirements

Dynamic scaling can be demonstrated without changing the benchmark files:

- `direct`: on `worker-server`, start with one or two REST servers behind `src/direct/load_balancer.py`, then add another `server.py` process during the run
- `rabbitmq`: on `worker-server`, start the benchmark with one worker, then launch additional `src/indirect/worker.py` processes while the client is still running

Hotspot contention can be tested with the provided benchmark file `benchmarks/benchmark_numbered_hotspot_25997.txt` and a separate label:

```bash
cd ~/Practica-SD
export REDIS_HOST=10.0.1.118
export RABBITMQ_HOST=10.0.1.106
export REST_BENCHMARK_URL=http://10.0.1.118:8000
python src/benchmarks/backend_scaling_runner.py --architecture rest --ticket-type numbered --backend-count 3 --client-workers 4 --numbered-benchmark benchmarks/benchmark_numbered_hotspot_25997.txt --label aws_hotspot_backend
python src/benchmarks/backend_scaling_runner.py --architecture rabbitmq --ticket-type numbered --backend-count 3 --client-workers 4 --numbered-benchmark benchmarks/benchmark_numbered_hotspot_25997.txt --label aws_hotspot_backend
```

This produces labeled report and plot files such as:

- `results/reports/benchmark_summary_hotspot_*.txt`
- `results/plots/rest_numbered_scalability_hotspot.png`
- `results/plots/comparison_numbered_hotspot.png`

## Notes

- Use AWS Academy / lab VMs for the final evaluation runs.
- The direct architecture uses a built-in Python load balancer on port `8000` as the official single entry point.
- The indirect architecture supports tuning with `--workers`, `--in-flight`, and worker `--prefetch`.
- The `scripts/` folder only contains Linux/AWS launch helpers.
