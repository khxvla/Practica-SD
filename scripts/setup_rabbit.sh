#!/bin/bash
echo "--- Instalando y configurando RabbitMQ ---"
sudo dnf install docker -y
sudo systemctl start docker
sudo systemctl enable docker

echo "--- Levantando contenedor RabbitMQ ---"
sudo docker rm -f rabbitmq 2>/dev/null
sudo docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

echo "--- Esperando 15s a que el conejo despierte... ---"
sleep 15

echo "--- Configurando permisos de usuario ---"
sudo docker exec rabbitmq rabbitmqctl add_user admin admin123
sudo docker exec rabbitmq rabbitmqctl set_user_tags admin administrator
sudo docker exec rabbitmq rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"

echo " RabbitMQ listo y configurado con usuario 'admin'"
