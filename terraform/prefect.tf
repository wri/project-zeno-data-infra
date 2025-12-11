provider "prefect" {
  api_key      = var.prefect_api_key
  account_id   = var.prefect_account_id
  workspace_id = var.prefect_workspace_id
}

locals {
  azs = [
    "us-east-1a",
    "us-east-1b",
    "us-east-1c",
  ]

  # Suffix the VPC name so each TF workspace has its own networking
  vpc_name                = "prefect-ecs-vpc${local.name_suffix}"
  base_vpc_cidr           = "10.0.0.0/16"
  flow_log_retention_days = 7
}

module "prefect_ecs_worker" {
  source  = "PrefectHQ/ecs-worker/prefect"
  version = "~> 0.0.3"

  # Suffix all names to keep resources isolated by TF workspace
  name                 = "${var.project_name}${local.name_suffix}"
  vpc_id               = module.prefect_vpc.vpc_id
  worker_subnets       = module.prefect_vpc.private_subnets
  prefect_api_key      = var.prefect_api_key
  prefect_account_id   = var.prefect_account_id
  prefect_workspace_id = var.prefect_workspace_id

  # Work pool must match the suffixed pool you create below
  worker_work_pool_name = "${var.work_pool_name}${local.name_suffix}"

  worker_desired_count = var.worker_count
  worker_cpu           = var.worker_cpu
  worker_memory        = var.worker_memory

  worker_log_retention_in_days = 30
}

resource "prefect_block" "aws_credentials" {
  # Suffix the block name to avoid collisions across TF workspaces
  name      = "aws-credentials-us-east-1${local.name_suffix}"
  type_slug = "aws-credentials"

  data = jsonencode({
    aws_access_key_id     = var.aws_access_key_id
    aws_secret_access_key = var.aws_secret_access_key
    region_name           = var.region
  })
}

resource "prefect_work_pool" "ecs_pool" {
  # Suffix so each TF workspace has its own pool
  name = "${var.work_pool_name}${local.name_suffix}"
  type = "ecs"

  base_job_template = jsonencode({
    job_configuration = {
      env    = "{{ env }}"
      vpc_id = module.prefect_vpc.vpc_id
      task_definition = {
        executionRoleArn = module.prefect_ecs_worker.prefect_worker_execution_role_arn
        containerDefinitions = [{
          image = "{{ image }}",
          name  = "prefect"
        }]
        cpu                     = "{{ cpu }}"
        memory                  = "{{ memory }}"
        networkMode             = "awsvpc"
        requiresCompatibilities = ["FARGATE"]
        runtimePlatform = {
          cpuArchitecture       = "ARM64"
          operatingSystemFamily = "LINUX"
        }
      }
      vpc_configuration = {
        subnets            = module.prefect_vpc.private_subnets
        security_group_ids = [module.prefect_ecs_worker.prefect_worker_security_group]
      }
      task_run_request = {
        cluster    = module.prefect_ecs_worker.prefect_worker_cluster_name
        launchType = "FARGATE"
        overrides = {
          containerOverrides = [{
            cpu    = "{{ cpu }}",
            memory = "{{ memory }}"
          }]
        }
      }
      configure_cloudwatch_logs = "True"
    }
    variables = {
      properties = {
        image  = { type = "string" }
        cpu    = { type = "integer", default = var.flow_cpu }
        memory = { type = "integer", default = var.flow_memory }
        env    = { type = "object", additionalProperties = { type = "string" } }
      }
    }
  })
}

resource "prefect_flow" "gnw_zonal_stats_update" {
  # Suffix flow names too; Prefect treats names as workspace-unique
  name = "GNW zonal stats update${local.name_suffix}"
}

resource "prefect_deployment" "gnw_zonal_stats_update" {
  # Suffix the deployment name and point to the suffixed work pool
  name           = "gnw-zonal-stats-update${local.name_suffix}"
  work_pool_name = prefect_work_pool.ecs_pool.name
  flow_id        = prefect_flow.gnw_zonal_stats_update.id
  path           = "/app"
  entrypoint     = "pipelines/run_updates.py:main"

  job_variables = jsonencode({
    image = var.pipelines_image
    env = {
      API_KEY               = var.api_key
      DASK_COILED__TOKEN    = var.coiled_token
      AWS_ACCESS_KEY_ID     = var.aws_access_key_id
      AWS_SECRET_ACCESS_KEY = var.aws_secret_access_key
      PIPELINES_IMAGE       = var.pipelines_image
      TF_WORKSPACE          = terraform.workspace
      AWS_REQUEST_PAYER     = "requester"  # for reading COGS from gfw account
    }
  })

  parameters = jsonencode({
    dist_version = null
    is_latest    = false
  })
}


resource "prefect_webhook" "dist_update_event" {
  name        = "dist-updated-event"
  description = "Fire an event when a new DIST alerts version is published."
  enabled     = true
  template = jsonencode({
    event = "dist_updated"
    payload = {
      dataset = "{{body.dataset}}"
      version = "{{body.version}}"
    }
    resource = {
      "prefect.resource.id"   = "{{body.dataset}}/{{ body.version }}"
      "prefect.resource.name" = "DIST Alerts update for v{{ body.version }}"
    }
  })
}

resource "prefect_automation" "run_pipelines_on_dist_update" {
  name    = "run-gnw-zonal-stats-on-dist-update"
  enabled = true

  trigger = {
    event = {
      posture   = "Reactive"
      expect    = ["dist_updated"]
      threshold = 1
      within    = 0
    }
  }
  actions = [
    {
      type          = "run-deployment"
      source        = "selected"
      deployment_id = prefect_deployment.gnw_zonal_stats_update.id
      parameters    = jsonencode({
        dist_version = "{{ event.payload.version }}"
      })
      job_variables = jsonencode({})
    },
  ]
}


# taken from https://github.com/PrefectHQ/terraform-prefect-ecs-worker/tree/main/examples/ecs-worker
module "prefect_vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.19.0"

  name = local.vpc_name
  cidr = local.base_vpc_cidr
  azs  = local.azs

  # Enable a NAT gateway to allow private subnets to route traffic to the internet.
  enable_nat_gateway = true
  enable_vpn_gateway = false

  # So as to not waste IP addresses, we only create one NAT gateway across all AZs.
  one_nat_gateway_per_az = false
  single_nat_gateway     = true

  # Create an internet gateway to allow public subnets to route traffic to the internet.
  create_igw = true

  # The public subnets are used to route traffic from private subnets to the internet through the NAT gateway.
  # We only need one public subnet per AZ to route traffic to the internet from the private subnets.
  public_subnets = [for k, v in local.azs : cidrsubnet(local.base_vpc_cidr, 4, k)]

  # The private subnets are used to run the Prefect Server in Fargate.
  private_subnets = concat(
    # Assign primary VPC CIDR blocks to the private subnets
    [for k, v in local.azs : cidrsubnet(local.base_vpc_cidr, 4, k + 3)]
  )

  private_subnet_names = [for k, v in local.azs : "${local.vpc_name}-private-${local.azs[k]}"]
  public_subnet_names  = [for k, v in local.azs : "${local.vpc_name}-public-${local.azs[k]}"]

  # Enable flow logs to capture all traffic in and out of the VPC.
  enable_flow_log                                 = true
  create_flow_log_cloudwatch_log_group            = true
  create_flow_log_cloudwatch_iam_role             = true
  flow_log_cloudwatch_log_group_retention_in_days = local.flow_log_retention_days

  # The default security group is not used and by default the default security group
  # is deny on all ports and protocols both ingress and egress.
  manage_default_security_group = false

  # The default route table is not used and does not need to be managed by Terraform.
  manage_default_route_table = false

  tags = {
    Name      = local.vpc_name
    Workspace = terraform.workspace
    Project   = var.project_name
    ManagedBy = "terraform"
  }
}
