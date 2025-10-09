############################
# Security groups
############################
# ALB SG: open 80 now; (443 added in Phase 2)
resource "aws_security_group" "alb" {
  name        = "analytics-alb-sg"
  description = "ALB for analytics API"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  # 443 will be added in Phase 2

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Allow ALB -> service on container port (tight, SG-to-SG)
resource "aws_security_group_rule" "svc_from_alb" {
  type                     = "ingress"
  from_port                = 8000
  to_port                  = 8000
  protocol                 = "tcp"
  security_group_id        = var.service_sg_id
  source_security_group_id = aws_security_group.alb.id
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

resource "aws_lb_target_group" "analytics" {
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

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.analytics.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.analytics.arn
  }
}
