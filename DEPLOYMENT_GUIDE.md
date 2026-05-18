#!/bin/bash
# Deployment & Operations Guide
# One-command setup for AWS EC2 instances

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
