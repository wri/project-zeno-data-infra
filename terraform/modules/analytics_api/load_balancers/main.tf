terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~>6"
    }
  }
}


############################
# Security groups
############################
# ALB SG: open 80 now; (443 added in Phase 2)
resource "aws_security_group" "alb" {
  name        = "analytics-alb-sg"
  description = "ALB for analytics API + Dask dashboard"
  vpc_id      = var.vpc_id
}

# 443 will be added in Phase 2
resource "aws_vpc_security_group_ingress_rule" "api_ingress" {
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
}

# Allow 8787 (prefer restricting to your VPN/office CIDR)
resource "aws_vpc_security_group_ingress_rule" "alb_ingress_8787" {
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = var.trusted_cidr_for_8787
  ip_protocol       = "tcp"
  from_port         = 8787
  to_port           = 8787
}

resource "aws_vpc_security_group_egress_rule" "all_egress" {
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "api_from_alb_8000" {
  security_group_id        = var.service_sg_id
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol              = "tcp"
  from_port                = 8000
  to_port                  = 8000
}

resource "aws_vpc_security_group_ingress_rule" "dask_from_alb_8787" {
  security_group_id        = var.service_sg_id
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol              = "tcp"
  from_port                = 8787
  to_port                  = 8787
}

############################
# ALB + Target Group
############################
resource "aws_lb" "analytics" {
  name               = "analytics-https-alb"
  load_balancer_type = "application"
  internal           = false # keep public for now
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.public_subnet_ids
  enable_deletion_protection = true
}

# API TG (8000)
resource "aws_lb_target_group" "analytics_api" {
  name        = "tg-analytics"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/health"
    matcher             = "200-399"
    interval            = 20
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 5
  }
}

# Dask dashboard TG (8787)
resource "aws_lb_target_group" "analytics_dask" {
  name        = "tg-analytics-dask"
  port        = 8787
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  # Dask dashboard usually serves on "/" with 200 OK
  health_check {
    path                = "/"
    matcher             = "200-399"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 5
  }
}

resource "aws_lb_listener" "analytics_api_http" {
  load_balancer_arn = aws_lb.analytics.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.analytics_api.arn
  }
}

resource "aws_lb_listener" "dask_dashboard_http" {
  load_balancer_arn = aws_lb.analytics.arn
  port              = 8787
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.analytics_dask.arn
  }
}
