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

output "dask_scheduler_task_definition_arn" {
  description = "Dask scheduler task definition ARN"
  value       = aws_ecs_task_definition.dask_scheduler.arn
}

