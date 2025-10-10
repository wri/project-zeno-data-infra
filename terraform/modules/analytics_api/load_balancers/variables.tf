variable "vpc_id" {
  type = string
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "service_sg_id" {
  type = string
}

variable "trusted_cidr_for_8787" {
  type        = string
  description = "CIDR allowed to reach Dask dashboard on 8787"
  default     = "0.0.0.0/0"
}