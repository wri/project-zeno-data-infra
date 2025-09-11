# terraform {
#     required_providers {
#         aws = {
#             source  = "hashicorp/aws"
#             version = "<6.0.0"
#         }
#     }
# }

locals {
  name_suffix = terraform.workspace == "default" ? "" : "-${terraform.workspace}"
  state_bucket = terraform.workspace == "default" ? "production" : "dev"
}



provider "aws" {
    region = "us-east-1" # Replace with your desired region
}


module "ecs" {
  source = "terraform-aws-modules/ecs/aws"

  cluster_name = "analytics${local.name_suffix}"
  create_task_exec_iam_role = true

  services = {
    analytics = {
      cpu    = 8192
      memory = 32768
      assign_public_ip = true

      name = "analytics${local.name_suffix}"
    
      default_capacity_provider_strategy = {
        FARGATE = {
          weight = 50
          base   = 20
        }
        FARGATE_SPOT = {
          weight = 50
        }
      }

      # Container definition(s)
      container_definitions = {
        api = {
          cpu       = 8192
          memory    = 32768
          essential = true
          portMappings = [
            {
                name          = "api"
                containerPort = 8000
                hostPort      = 8000
                protocol      = "tcp"
            }
          ]
          image     = var.api_image
          command   = ["newrelic-admin", "run-program", "uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
          readonlyRootFilesystem = false
          environment = [
            {
              name = "PYTHONPATH"
              value = "/app/api"
            },
            {
              name = "API_KEY"
              value = var.api_key
            },
            {
              name = "AWS_SECRET_ACCESS_KEY"
              value = var.aws_secret_access_key
            },
            {
              name = "AWS_ACCESS_KEY_ID"
              value = var.aws_access_key_id
            }
          ]
        }
      }

      load_balancer = {
        service = {
          target_group_arn = module.api_alb.target_groups["ex_ecs"].arn
          container_name   = "api"
          container_port   = 8000
        }
      }
    
      enable_cloudwatch_logging = true
      subnet_ids = var.subnet_ids
      security_group_rules = {
        alb_ingress_8000 = {
          type                     = "ingress"
          from_port                = 8000
          to_port                  = 8000
          protocol                 = "tcp"
          description              = "Service port"
          source_security_group_id = "sg-080c4a3dcb3b8052b"
        }
        egress_all = {
          type        = "egress"
          from_port   = 0
          to_port     = 0
          protocol    = "-1"
          cidr_blocks = ["0.0.0.0/0"]
        }
      }
    },
    dask_scheduler = {
      cpu    = 2048
      memory = 8192
      assign_public_ip = true
      name = "dask-scheduler${local.name_suffix}"

      desired_count = 1
      min_capacity  = 1
    
      default_capacity_provider_strategy = {
        FARGATE = {
          weight = 100
          base   = 1
        }
      }

      runtime_platform = {
        cpu_architecture        = "X86_64"  # or "ARM64"
        operating_system_family = "LINUX"
      }
      container_definitions = {
        scheduler = {
          cpu       = 2048
          memory    = 8192
          essential = true
          image     = var.api_image
          
          command = [
            "dask-scheduler",
            "--host", "0.0.0.0", 
            "--port", "8786",
            "--dashboard-address", "0.0.0.0:8787",
            "--protocol", "tcp"
          ]
          
          portMappings = [
            {
              name          = "scheduler"
              containerPort = 8786
              hostPort      = 8786
              protocol      = "tcp"
            },
            {
              name          = "dashboard"
              containerPort = 8787
              hostPort      = 8787
              protocol      = "tcp"
            }
          ]
          
          environment = [
            {
              name  = "PYTHONPATH"
              value = "/app/api"
            },
            {
              name  = "API_KEY"
              value = var.api_key
            },
            {
              name  = "AWS_SECRET_ACCESS_KEY"
              value = var.aws_secret_access_key
            },
            {
              name  = "AWS_ACCESS_KEY_ID"
              value = var.aws_access_key_id
            }
          ]
          
          readonlyRootFilesystem = false
        }
      }

      enable_cloudwatch_logging = true
      subnet_ids = var.subnet_ids

      load_balancer = {
        scheduler = {
          target_group_arn = module.dask_nlb.target_groups["dask_scheduler"].arn
          container_name = "scheduler"
          container_port = 8786
        }
        dashboard = {
          target_group_arn = module.api_alb.target_groups["dask_dashboard"].arn
          container_name = "scheduler"
          container_port = 8787
        }
      }
      
      security_group_rules = {
        alb_ingress_8786 = {
          type                     = "ingress"
          from_port                = 8786
          to_port                  = 8786
          protocol                 = "tcp"
          description              = "Dask Scheduler Port"
          source_security_group_id = "sg-080c4a3dcb3b8052b"
        }
        alb_ingress_8787 = {
          type                     = "ingress"
          from_port                = 8787
          to_port                  = 8787
          protocol                 = "tcp"
          description              = "Dask Scheduler Web UI Port"
          source_security_group_id = "sg-080c4a3dcb3b8052b"
        }
        egress_all = {
          type        = "egress"
          from_port   = 0
          to_port     = 0
          protocol    = "-1"
          cidr_blocks = ["0.0.0.0/0"]
        }
      }
    }
  }

  tags = {
    Environment = "Staging"
    Project     = "Zeno"
  }
}

module "api_alb" {
  source  = "terraform-aws-modules/alb/aws"
  version = "~> 9.0"

  name = "analytics${local.name_suffix}"

  load_balancer_type = "application"

  vpc_id  = var.vpc
  subnets = var.subnet_ids

  # For example only
  enable_deletion_protection = false

  # Security Group
  security_group_ingress_rules = {
    all_http = {
      from_port   = 80
      to_port     = 80
      ip_protocol = "tcp"
      cidr_ipv4   = "0.0.0.0/0"
    }
    dask_dashboard = {
      from_port = 8787
      to_port = 8787
      ip_protocol = "tcp"
      cidr_ipv4 = "0.0.0.0/0"
    }
  }
  security_group_egress_rules = {
    all = {
      ip_protocol = "-1"
      cidr_ipv4   = "10.0.0.0/16"
    }
  }

  listeners = {
    ex_http = {
      port     = 80
      protocol = "HTTP"

      forward = {
        target_group_key = "ex_ecs"
      }
    }
    dask_dashboard = {
      port = 8787
      protocol = "HTTP"
      forward = {
        target_group_key = "dask_dashboard"
      }
    }
  }

  target_groups = {
    ex_ecs = {
      backend_protocol                  = "HTTP"
      backend_port                      = 8000
      target_type                       = "ip"
      deregistration_delay              = 5
      load_balancing_cross_zone_enabled = true

      health_check = {
        enabled             = true
        healthy_threshold   = 5
        interval            = 30
        matcher             = "200"
        path                = "/"
        port                = "traffic-port"
        protocol            = "HTTP"
        timeout             = 5
        unhealthy_threshold = 2
      }

      # Theres nothing to attach here in this definition. Instead,
      # ECS will attach the IPs of the tasks to this target group
      create_attachment = false
    }

    dask_dashboard = {
      backend_protocol = "HTTP"
      backend_port = 8787
      target_type = "ip"
      deregistration_delay = 5
      load_balancing_cross_zone_enabled = true
      health_check = {
        enabled = true
        healthy_threshold = 2
        interval = 30
        matcher = "200"
        path = "/status"
        port = "traffic-port"
        protocol = "HTTP"
        timeout = 5
        unhealthy_threshold = 2
      }
      create_attachment = false
    }
  }
}

resource "aws_ecs_task_definition" "dask_worker" {
  family                   = "dask-worker"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 2048
  memory                   = 8192
  execution_role_arn       = module.ecs.task_exec_iam_role_arn

  # Set CPU architecture here
  runtime_platform {
    cpu_architecture        = "X86_64"  # or "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([
    {
      name  = "dask-worker"
      image = var.api_image
      
      environment = [
        {
          name  = "PYTHONPATH"
          value = "/app/api"
        },
        {
          name  = "API_KEY"
          value = var.api_key
        },
        {
          name  = "AWS_SECRET_ACCESS_KEY"
          value = var.aws_secret_access_key
        },
        {
          name  = "AWS_ACCESS_KEY_ID"
          value = var.aws_access_key_id
        }
      ]
      
      # logConfiguration = {
      #   logDriver = "awslogs"
      #   options = {
      #     awslogs-group         = aws_cloudwatch_log_group.dask.name
      #     awslogs-region        = var.aws_region
      #     awslogs-stream-prefix = "dask-worker"
      #   }
      # }
    }
  ])
}

module "dask_nlb" {
  source  = "terraform-aws-modules/alb/aws"
  version = "~> 9.0"
  
  name               = "dask${local.name_suffix}"
  load_balancer_type = "network"
  vpc_id             = var.vpc
  subnets            = var.subnet_ids
  enable_deletion_protection = false
  
  listeners = {
    dask_scheduler = {
      port     = 8786
      protocol = "TCP"
      forward = {
        target_group_key = "dask_scheduler"
      }
    }
  }
  
  target_groups = {
    dask_scheduler = {
      backend_protocol = "TCP"
      backend_port = 8786
      target_type = "ip"
      deregistration_delay = 10
      load_balancing_cross_zone_enabled = true
      
      # health_check = {
      #   enabled = true
      #   healthy_threshold = 2
      #   interval = 30
      #   port = 8787  # Health check dashboard
      #   protocol = "HTTP"
      #   path = "/status"
      #   timeout = 10
      #   unhealthy_threshold = 2
      #   matcher = "200"
      # }
      
      create_attachment = false
    }
  }
}