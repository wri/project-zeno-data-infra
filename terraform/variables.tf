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

variable "pipelines_image" {
  description = "Data update pipelines image URI"
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

variable "gfw_aws_secret_access_key" {
  type = string
}

variable "gfw_aws_access_key_id" {
  type = string
}


variable "new_relic_license_key" {
  type        = string
  description = "New Relic License Key"
}

variable "prefect_api_key" {
  description = "Prefect Cloud API key"
  type        = string
  sensitive   = true
}

variable "prefect_account_id" {
  description = "Prefect Cloud account ID"
  type        = string
}

variable "prefect_workspace_id" {
  description = "Prefect Cloud workspace ID"
  type        = string
}

variable "project_name" {
  description = "Global Nature Watch Analytics Pipeline"
  type        = string
  default     = "GNW-Analytics-Pipeline"
}

variable "work_pool_name" {
  description = "Name of the Prefect work pool"
  type        = string
  default     = "gnw-pipeline-ecs-work-pool"
}

variable "worker_count" {
  description = "Number of Prefect workers to run"
  type        = number
  default     = 2
}

variable "worker_cpu" {
  description = "CPU units for worker tasks"
  type        = number
  default     = 2048 # 2 vCPU
}

variable "worker_memory" {
  description = "Memory for worker tasks in MB"
  type        = number
  default     = 8192 # 8 GB
}

variable "flow_cpu" {
  description = "Default CPU units for flow run tasks"
  type        = number
  default     = 4096 # 4 vCPU
}

variable "flow_memory" {
  description = "Default memory for flow run tasks in MB"
  type        = number
  default     = 16384 # 16 GB
}

variable "coiled_token" {
  description = "Access token to write dask on Coiled"
  type = string
}