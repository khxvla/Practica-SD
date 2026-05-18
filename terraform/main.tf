provider "aws" {
  region = var.aws_region
}

# Security Group para permitir el tráfico necesario
resource "aws_security_group" "ticket_sg" {
  name        = "ticket-system-sg"
  description = "Allow inbound traffic for Ticket System (PostgreSQL, RabbitMQ, Redis, REST)"
  vpc_id      = var.vpc_id

  # SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # RabbitMQ AMQP
  ingress {
    from_port   = 5672
    to_port     = 5672
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # RabbitMQ Management API
  ingress {
    from_port   = 15672
    to_port     = 15672
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # PostgreSQL
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Redis
  ingress {
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # REST API / Load Balancer
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Instancia EC2 para Datos y Mensajería (PostgreSQL + RabbitMQ + Redis)
resource "aws_instance" "data_node" {
  ami           = var.ami_id
  instance_type = var.instance_type
  key_name      = var.key_name
  subnet_id     = var.subnet_id
  vpc_security_group_ids = [aws_security_group.ticket_sg.id]

  # Script de auto-configuración (Cloud-Init)
  user_data = <<-EOF
              #!/bin/bash
              sudo apt-get update
              
              # 1. Instalar y configurar RabbitMQ
              sudo apt-get install -y rabbitmq-server
              sudo systemctl enable rabbitmq-server
              sudo systemctl start rabbitmq-server
              sudo rabbitmq-plugins enable rabbitmq_management
              
              # 2. Instalar y configurar PostgreSQL
              sudo apt-get install -y postgresql postgresql-contrib
              sudo systemctl enable postgresql
              sudo systemctl start postgresql
              sudo -u postgres psql -c "CREATE DATABASE tickets;"
              sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'password_segura_sd';"
              # Permitir conexiones externas
              echo "listen_addresses = '*'" | sudo tee -a /etc/postgresql/*/main/postgresql.conf
              echo "host all all 0.0.0.0/0 md5" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf
              sudo systemctl restart postgresql
              
              # 3. Instalar y configurar Redis
              sudo apt-get install -y redis-server
              sudo sed -i 's/bind 127.0.0.1 ::1/bind 0.0.0.0/g' /etc/redis/redis.conf
              sudo systemctl restart redis-server
              EOF

  tags = {
    Name = "TicketSystem-DataNode"
  }
}

# Instancia EC2 para los Workers (REST o RabbitMQ)
resource "aws_instance" "worker_node" {
  ami           = var.ami_id
  instance_type = var.instance_type
  key_name      = var.key_name
  subnet_id     = var.subnet_id
  vpc_security_group_ids = [aws_security_group.ticket_sg.id]

  # Preinstalar dependencias de Python
  user_data = <<-EOF
              #!/bin/bash
              sudo apt-get update
              sudo apt-get install -y python3-pip python3-venv git
              EOF

  tags = {
    Name = "TicketSystem-WorkerNode"
  }
}
