# terraform {
#     required_providers {
#         aws = {
#             source  = "hashicorp/aws"
#             version = "<6.0.0"
#         }
#     }
# }

terraform {
  backend "s3" {
    bucket         = "tf-state-zeno-rest-api"
    key            = "terraform/state/production/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-locks-production"
  }
}

locals {
  name_suffix = terraform.workspace == "default" ? "" : "-${terraform.workspace}"
  state_bucket = terraform.workspace == "default" ? "production" : "dev"
  cluster_name = "analytics${local.name_suffix}"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_vpc" "selected" {
  id = var.vpc
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

      # Enable autoscaling
      enable_autoscaling = true
      autoscaling_min_capacity = 1
      autoscaling_max_capacity = 10
      
      autoscaling_policies = {
        cpu_scaling = {
          policy_type = "TargetTrackingScaling"
          target_tracking_scaling_policy_configuration = {
            target_value = 40.0
            predefined_metric_specification = {
              predefined_metric_type = "ECSServiceAverageCPUUtilization"
            }
            scale_out_cooldown = 300
            scale_in_cooldown = 300
          }
        }
        
        memory_scaling = {
          policy_type = "TargetTrackingScaling"
          target_tracking_scaling_policy_configuration = {
            target_value = 50.0
            predefined_metric_specification = {
              predefined_metric_type = "ECSServiceAverageMemoryUtilization"
            }
            scale_out_cooldown = 300
            scale_in_cooldown = 300
          }
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
            },
            {
              name  = "ANALYSES_TABLE_NAME"
              value = aws_dynamodb_table.analyses.name
            },
            {
              name  = "ANALYSIS_RESULTS_BUCKET_NAME"
              value = aws_s3_bucket.analysis_results.bucket
            },
            {
              name  = "DASK_SCHEDULER_ADDRESS"
              value = "tcp://${module.dask_nlb.dns_name}:8786"
            },
            {
              name = "NEW_RELIC_LICENSE_KEY"
              value = var.new_relic_license_key
            }
          ]
        }
      }

      load_balancer = {
        service = {
          target_group_arn = module.alb.target_groups["ex_ecs"].arn
          container_name   = "api"
          container_port   = 8000
        }
      }
    
      enable_cloudwatch_logging = true
      subnet_ids = var.subnet_ids
      security_group_ingress_rules = {
        alb_ingress_8000 = {
          type                     = "ingress"
          from_port                = 8000
          to_port                  = 8000
          protocol                 = "tcp"
          description              = "Service port"
          referenced_security_group_id = module.alb.security_group_id
        }
      }
      security_group_egress_rules = {
        egress_all = {
          type        = "egress"
          from_port   = 0
          to_port     = 65535
          protocol    = "-1"
          cidr_ipv4 = "0.0.0.0/0"
        }
      }
    },
    dask_scheduler = {
      cpu    = 4096
      memory = 16384
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
          cpu       = 4096
          memory    = 16384
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
          target_group_arn = module.alb.target_groups["dask_dashboard"].arn
          container_name = "scheduler"
          container_port = 8787
        }
      }
      
      security_group_ingress_rules = {
        alb_ingress_8786 = {
          type                     = "ingress"
          from_port                = 8786
          to_port                  = 8786
          protocol                 = "tcp"
          description              = "Dask Scheduler Port"
          referenced_security_group_id = module.dask_nlb.security_group_id
        }
        alb_ingress_8787 = {
          type                     = "ingress"
          from_port                = 8787
          to_port                  = 8787
          protocol                 = "tcp"
          description              = "Dask Scheduler Web UI Port"
          referenced_security_group_id = module.alb.security_group_id
        }
      }
      security_group_egress_rules = {
        egress_all = {
          type        = "egress"
          from_port   = 0
          to_port     = 65535
          protocol    = "-1"
          cidr_ipv4 = "0.0.0.0/0"
        }
      }
    }

    dask_cluster_manager = {
      cpu    = 1024
      memory = 4096
      assign_public_ip = true
      name = "dask-manager${local.name_suffix}"

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
        dask_cluster_manager = {
          cpu       = 1024
          memory    = 4096
          essential = true
          image     = var.api_image
          
          command = [
            "python",
            "/app/api/dask_cluster/start_cluster.py",
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
            },
            {
              name  = "DASK_SCHEDULER_ADDRESS"
              value = "tcp://${module.dask_nlb.dns_name}:8786"
            },
            {
              name  = "DASK_VPC"
              value = var.vpc
            },
            {
              name  = "DASK_CLUSTER_ARN"
              value = "arn:aws:ecs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:cluster/${local.cluster_name}"
            },
            {
              name  = "DASK_WORKER_TASK_DEFINITION_ARN"
              value = aws_ecs_task_definition.dask_worker.arn
            },
            {
              name  = "DASK_WORKER_SECURITY_GROUP"
              value = aws_security_group.dask_workers.id
            }
          ]
          
          readonlyRootFilesystem = false
        }
      }

      enable_cloudwatch_logging = true
      subnet_ids = var.subnet_ids
      security_group_ingress_rules = {
        allow_all_tcp_from_vpc = {
          from_port   = 0
          to_port     = 65535
          protocol    = "tcp"
          description = "Allow all TCP traffic from within the VPC for scheduler communication"
          cidr_ipv4   = data.aws_vpc.selected.cidr_block
        }
      }
      security_group_egress_rules = {
        egress_all = {
          type        = "egress"
          from_port   = 0
          to_port     = 65535
          protocol    = "-1"
          cidr_ipv4 = "0.0.0.0/0"
        }
      }
    }
  }

  tags = {
    Environment = "Staging"
    Project     = "Zeno"
  }
}

module "alb" {
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
  cpu                      = 8192
  memory                   = 32768
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
      protocol = "TCP"
      port = 8786
      target_type = "ip"
      deregistration_delay = 150
      load_balancing_cross_zone_enabled = true
  
      create_attachment = false
    }
  }
  security_group_ingress_rules = {
    alb_ingress_8786_api = {
      type                     = "ingress"
      from_port                = 8786
      to_port                  = 8786
      protocol                 = "tcp"
      description              = "Dask Scheduler Port for api"
      cidr_ipv4 = "0.0.0.0/0" #TODO: Lock this down further, analytics services security group or vpc cidr not working
    }
  }

  security_group_egress_rules = {
    all_traffic = {
      ip_protocol = "-1"
      cidr_ipv4   = "0.0.0.0/0"
    }
  }
}


resource "aws_security_group" "dask_workers" {
  name_prefix = "dask-workers-${local.name_suffix}"
  vpc_id      = var.vpc

  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.selected.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "dask-workers-${local.name_suffix}"
  }
}

###############################################################
#     Infrastructure for AwsDynamoDbS3AnalysisRepository      #
###############################################################

resource "aws_s3_bucket" "analysis_results" {
  bucket = "gnw-analytics-api-analysis-results"

  tags = {
    Environment = "Staging"
    Project     = "Zeno"
  }
}

resource "aws_dynamodb_table" "analyses" {
  name         = "Analyses"
  billing_mode = "PAY_PER_REQUEST" # On-demand, scales automatically. Suitable for variable workloads.
  hash_key     = "resource_id"

  attribute {
    name = "resource_id"
    type = "S"
  }

  tags = {
    Environment = "Staging"
    Project     = "Zeno"
  }
}

# Create an IAM policy that grants access to the specific DDB table and S3 bucket
data "aws_iam_policy_document" "ecs_task_analysis_access" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
      "dynamodb:Scan"
    ]
    resources = [aws_dynamodb_table.analyses.arn]
  }
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = ["${aws_s3_bucket.analysis_results.arn}/*"]
  }
  # Allow listing the bucket (often needed for SDKs)
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.analysis_results.arn]
  }
}

resource "aws_iam_policy" "ecs_task_analysis" {
  name   = "ECSTaskAnalysisAccess${local.name_suffix}"
  path   = "/"
  policy = data.aws_iam_policy_document.ecs_task_analysis_access.json
}

# Attach the new policy to the ECS Task Execution Role created by the module
resource "aws_iam_role_policy_attachment" "ecs_task_analysis" {
  role       = module.ecs.task_exec_iam_role_name # This references the role created by the ECS module
  policy_arn = aws_iam_policy.ecs_task_analysis.arn
}