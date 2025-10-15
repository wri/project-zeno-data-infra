bucket         = "tf-state-zeno-rest-api"
key            = "terraform.tfstate"
region         = "us-east-1"
encrypt        = true
dynamodb_table = "terraform-locks-production"