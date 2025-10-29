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

# Output the DNS validation records that need to be created
output "certificate_validation_records" {
  description = "DNS records that need to be created for certificate validation"
  value = {
    for dvo in aws_acm_certificate.analytics.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      value  = dvo.resource_record_value
    }
  }
}

# Outputs for DNS configuration
output "alb_dns_name" {
  description = "DNS name of the ALB - point analytics.globalnaturewatch.org to this"
  value       = module.alb.dns_name
}

output "alb_zone_id" {
  description = "Zone ID of the ALB - needed for Route53 alias records"
  value       = module.alb.zone_id
}
