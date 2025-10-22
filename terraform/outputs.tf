output "analysis_results_bucket_name" {
  description = "Name of the results bucket"
  value = aws_s3_bucket.analysis_results.bucket
}

output "analysis_table_name" {
  description = "Name of the results table"
  value = aws_dynamodb_table.analyses.name
}

output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = module.gnw_ecs_cluster.cluster_arn
}

output "vpc_id" {
  description = "ECS/ALB VPC ID"
  value       = var.vpc
}

output "dask_worker_task_definition_arn" {
  description = "Dask worker task definition ARN"
  value       = aws_ecs_task_definition.dask_worker.arn
}

output "dask_dashboard_url" {
  description = "HTTP dashboard URL"
  value = "http://${module.alb.dns_name}:8787"
}
