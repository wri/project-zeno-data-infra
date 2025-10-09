output "alb_dns_name" {
  description = "Public DNS name of the new ALB"
  value       = aws_lb.analytics.dns_name
}

output "target_group_arn" {
  description = "ARN of the target group attached to analytics service"
  value       = aws_lb_target_group.analytics.arn
}
