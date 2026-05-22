provider "aws" {
  region = var.aws_region
}

resource "aws_security_group" "ticket_sg" {
  name        = "ticket-service-sg"
  description = "RabbitMQ, PostgreSQL and SSH access for the ticket service"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    from_port   = 5672
    to_port     = 5672
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    from_port   = 15672
    to_port     = 15672
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "state_node" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = var.key_name
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.ticket_sg.id]

  user_data = <<-EOF
              #!/bin/bash
              set -eux
              apt-get update
              DEBIAN_FRONTEND=noninteractive apt-get install -y rabbitmq-server postgresql postgresql-contrib python3-pip git

              systemctl enable rabbitmq-server
              systemctl start rabbitmq-server
              rabbitmq-plugins enable rabbitmq_management

              systemctl enable postgresql
              systemctl start postgresql
              sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = 'tickets'" | grep -q 1 || sudo -u postgres createdb tickets
              sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD '${var.postgres_password}';"
              sed -i "s/^#listen_addresses = 'localhost'/listen_addresses = '*'/g" /etc/postgresql/*/main/postgresql.conf
              echo "host all all ${var.vpc_cidr} md5" >> /etc/postgresql/*/main/pg_hba.conf
              systemctl restart postgresql
              EOF

  tags = {
    Name = "ticket-state-node-rabbitmq-postgres"
  }
}

resource "aws_iam_role" "lambda_role" {
  name = "ticket-worker-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_lambda_function" "ticket_worker" {
  function_name                  = var.lambda_function_name
  role                           = aws_iam_role.lambda_role.arn
  handler                        = "src.indirect.worker_lambda.lambda_handler"
  runtime                        = "python3.12"
  timeout                        = 30
  memory_size                    = 512
  filename                       = var.lambda_zip_path
  reserved_concurrent_executions = var.max_lambda_concurrency

  vpc_config {
    subnet_ids         = [var.subnet_id]
    security_group_ids = [aws_security_group.ticket_sg.id]
  }

  environment {
    variables = {
      RABBITMQ_HOST          = aws_instance.state_node.private_ip
      POSTGRES_HOST          = aws_instance.state_node.private_ip
      POSTGRES_DB            = "tickets"
      POSTGRES_USER          = "postgres"
      POSTGRES_PASSWORD      = var.postgres_password
      LAMBDA_BATCH_SIZE      = tostring(var.lambda_batch_size)
      MAX_LAMBDA_CONCURRENCY = tostring(var.max_lambda_concurrency)
      PAYMENT_DELAY_SECONDS  = "0.100"
      TOTAL_TICKETS          = "100000"
      LAMBDA_FUNCTION_NAME   = var.lambda_function_name
      AWS_REGION             = var.aws_region
    }
  }
}
