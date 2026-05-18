variable "aws_region" {
  description = "AWS Region to deploy to"
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "VPC ID from AWS Academy (e.g., vpc-12345678)"
  # No default, you must provide this based on your AWS Academy lab
}

variable "subnet_id" {
  description = "Subnet ID from AWS Academy (e.g., subnet-12345678)"
  # No default, you must provide this based on your AWS Academy lab
}

variable "ami_id" {
  description = "Ubuntu 22.04 LTS AMI ID for us-east-1"
  default     = "ami-0c7217cdde317cfec" 
}

variable "instance_type" {
  description = "EC2 instance type"
  default     = "t2.micro"
}

variable "key_name" {
  description = "AWS Academy Key Pair Name"
  default     = "vockey" # 'vockey' is the standard AWS Academy key pair name
}
