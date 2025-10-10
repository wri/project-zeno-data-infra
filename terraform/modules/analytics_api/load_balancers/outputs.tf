output "alb_dns_name" {
  description = "Public DNS name of the new ALB"
  value       = aws_lb.analytics.dns_name
}

output "api_target_group_arn" {
  description = "ARN of the API target group (8000)"
  value       = aws_lb_target_group.analytics_api.arn
}

output "dask_target_group_arn" {
  description = "ARN of the Dask dashboard target group (8787)"
  value       = aws_lb_target_group.analytics_dask.arn
}