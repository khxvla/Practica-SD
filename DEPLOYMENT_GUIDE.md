# Deployment & Operations Guide

Esta guía cubre el despliegue del sistema Ticket-Service utilizando **Infrastructure as Code (Terraform)** cumpliendo con el **Requisito 11**, y también incluye el script bash `one-command` original.

---

## Opción 1: Despliegue con Terraform (Recomendado - Requisito 11)

La carpeta `terraform/` contiene la definición de IaC necesaria para aprovisionar toda la topología en tu VPC de AWS Academy con un solo comando. Despliega instancias EC2 configuradas automáticamente (RabbitMQ, PostgreSQL, Redis y workers).

### Pasos:
1. Navega a la carpeta de Terraform:
   ```bash
   cd terraform
   ```
2. Inicializa Terraform:
   ```bash
   terraform init
   ```
3. Aplica los cambios (Te pedirá el `vpc_id` y `subnet_id` de tu laboratorio de AWS Academy):
   ```bash
   terraform apply
   ```
4. **Al finalizar**, Terraform imprimirá las IPs privadas (`data_node_private_ip` y `worker_node_private_ip`).
5. Copia esas IPs en el fichero `src/common/config.py`.

---

## Opción 2: Script Bash (One-Command Setup para Instancias EC2)

Si prefieres ejecutar el despliegue de forma manual desde dentro de una instancia EC2 limpia (Ubuntu), puedes usar el siguiente script `setup.sh`:

```bash
#!/bin/bash
set -e

echo "=========================================="
echo "Scalable Ticket Service - Deployment Guide"
echo "=========================================="

# Configuration
REDIS_HOST=${REDIS_HOST:-10.0.1.118}
RABBITMQ_HOST=${RABBITMQ_HOST:-10.0.1.106}
REPO_PATH=${REPO_PATH:-~/Practica-SD}

echo "[1] System Prerequisites"
sudo apt-get update -qq
sudo apt-get install -y python3.12 python3-pip redis-server rabbitmq-server git

echo "[2] Clone Repository"
cd ~
git clone https://github.com/khxvla/Practica-SD.git $REPO_PATH
cd $REPO_PATH

echo "[3] Install Python Dependencies"
pip3 install -r requirements.txt

echo "[4] Environment Setup"
export REDIS_HOST=$REDIS_HOST
export RABBITMQ_HOST=$RABBITMQ_HOST
export REDIS_PASSWORD=${REDIS_PASSWORD:-}

echo "[5] Initialize System"
python3 init_system.py

echo "[6] Start Services"

# Check which role this instance plays
if [ "$INSTANCE_ROLE" = "worker" ]; then
    echo "Starting as WORKER node (Redis + REST/RabbitMQ workers)"
    
    # Start Redis
    redis-server --daemonize yes --logfile /tmp/redis.log
    
    # Start REST servers (background)
    python3 src/direct/server.py --port 5001 &
    python3 src/direct/server.py --port 5002 &
    python3 src/direct/server.py --port 5003 &
    
    # Start load balancer
    python3 src/direct/load_balancer.py --host 0.0.0.0 --port 8000 &
    
    # Start RabbitMQ workers (via elastic launcher)
    python3 src/indirect/elastic_launcher.py --workers 3 &
    
    echo "✅ Worker services started"

elif [ "$INSTANCE_ROLE" = "broker" ]; then
    echo "Starting as BROKER node (RabbitMQ)"
    
    # Start RabbitMQ
    rabbitmq-server --detached
    
    # Setup topology
    python3 src/indirect/broker_setup.py
    
    echo "✅ Broker services started"

else
    echo "Unknown INSTANCE_ROLE. Set INSTANCE_ROLE=worker or broker"
    exit 1
fi

echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo "Redis:         $REDIS_HOST:6379"
echo "RabbitMQ:      $RABBITMQ_HOST:5672"
echo "REST LB:       http://0.0.0.0:8000"
echo "=========================================="
```
