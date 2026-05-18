output "data_node_private_ip" {
  description = "IP privada del nodo de Datos (RabbitMQ, Postgres, Redis). ¡Copia este valor en config.py!"
  value       = aws_instance.data_node.private_ip
}

output "data_node_public_ip" {
  description = "IP pública del nodo de Datos (para acceso por SSH)"
  value       = aws_instance.data_node.public_ip
}

output "worker_node_private_ip" {
  description = "IP privada del nodo Worker"
  value       = aws_instance.worker_node.private_ip
}

output "worker_node_public_ip" {
  description = "IP pública del nodo Worker (para acceso por SSH)"
  value       = aws_instance.worker_node.public_ip
}
