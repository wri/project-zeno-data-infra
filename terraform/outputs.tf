output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = module.ecs.cluster_arn
}

output "vpc_id" {
  description = "ECS/ALB VPC ID"
  value       = var.vpc
}

output "dask_worker_task_definition_arn" {
  description = "Dask worker task definition ARN"
  value       = aws_ecs_task_definition.dask_worker.arn
}


output "dask_scheduler_endpoint" {
  description = "Stable TCP endpoint for Dask workers"
  value = "tcp://${module.dask_nlb.dns_name}:8786"
}

output "dask_dashboard_url" {
  description = "HTTP dashboard URL"
  value = "http://${module.api_alb.dns_name}:8787"
}

