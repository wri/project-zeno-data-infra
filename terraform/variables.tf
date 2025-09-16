variable "subnet_ids" {
  description = "List of subnet IDs"
  type        = list(string)
  default     = ["subnet-0f1544432f2a769d2", "subnet-06be7fcbfc68758ff", "subnet-04591b309ac62bf35"]
}

variable "vpc" {
  description = "VPC ID"
  type        = string
  default     = "vpc-0233b677bf7586002"
}

variable "api_image" {
  default     = "084375562450.dkr.ecr.us-east-1.amazonaws.com/analytics-api:latest"
  description = "API image URI"
  type        = string
}

variable "api_key" {
  type        = string
}

variable "aws_secret_access_key" {
  type = string
}

variable "aws_access_key_id" {
  type = string
}

variable "new_relic_license_key" {
  type        = string
  description = "New Relic License Key"
}