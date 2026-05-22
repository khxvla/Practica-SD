output "state_node_private_ip" {
  description = "Private IP for RabbitMQ and PostgreSQL"
  value       = aws_instance.state_node.private_ip
}

output "state_node_public_ip" {
  description = "Public IP for SSH administration"
  value       = aws_instance.state_node.public_ip
}

output "lambda_function_name" {
  description = "Stateless ticket worker Lambda"
  value       = aws_lambda_function.ticket_worker.function_name
}

output "rabbitmq_management_url" {
  description = "RabbitMQ Management UI from inside the VPC or SSH tunnel"
  value       = "http://${aws_instance.state_node.private_ip}:15672"
}
