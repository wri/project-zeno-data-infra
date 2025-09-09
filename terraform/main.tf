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

provider "aws" {
    region = "us-east-1" # Replace with your desired region
}


module "ecs" {
  source = "terraform-aws-modules/ecs/aws"

  cluster_name = "analytics"
  create_task_exec_iam_role = true
  default_capacity_provider_strategy = {
    FARGATE = {
      weight = 50
      base   = 20
    }
    FARGATE_SPOT = {
      weight = 50
    }
  }

  services = {
    analytics = {
      cpu    = 8192
      memory = 32768
      assign_public_ip = true

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
          command   = ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
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

  name = "analytics"

  load_balancer_type = "application"

  vpc_id  = "vpc-0233b677bf7586002"
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
  name   = "ECSTaskAnalysisAccess"
  path   = "/"
  policy = data.aws_iam_policy_document.ecs_task_analysis_access.json
}

# Attach the new policy to the ECS Task Execution Role created by the module
resource "aws_iam_role_policy_attachment" "ecs_task_analysis" {
  role       = module.ecs.task_exec_iam_role_name # This references the role created by the ECS module
  policy_arn = aws_iam_policy.ecs_task_analysis.arn
}