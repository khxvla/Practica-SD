variable "aws_region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "AWS Academy VPC id"
}

variable "subnet_id" {
  description = "Private or public subnet used by the lab"
}

variable "vpc_cidr" {
  description = "CIDR allowed to access RabbitMQ and PostgreSQL"
  default     = "172.31.0.0/16"
}

variable "admin_cidr" {
  description = "CIDR allowed for SSH"
  default     = "0.0.0.0/0"
}

variable "ami_id" {
  description = "Ubuntu 22.04 LTS AMI for the target region"
  default     = "ami-0c7217cdde317cfec"
}

variable "instance_type" {
  description = "EC2 instance type for RabbitMQ and PostgreSQL"
  default     = "t3.micro"
}

variable "key_name" {
  description = "AWS Academy key pair name"
  default     = "vockey"
}

variable "postgres_password" {
  description = "PostgreSQL password used by Lambda"
  sensitive   = true
  default     = "password_segura_sd"
}

variable "lambda_function_name" {
  description = "Name of the stateless worker Lambda"
  default     = "TicketWorkerLambda"
}

variable "lambda_zip_path" {
  description = "Path to the deployment zip created before terraform apply"
  default     = "../build/lambda_function.zip"
}

variable "lambda_batch_size" {
  description = "Messages drained per Lambda invocation"
  default     = 5
}

variable "max_lambda_concurrency" {
  description = "Conservative AWS Academy Lambda concurrency cap"
  default     = 8
}
