terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "tc-fase2-terraform-state"
    key    = "alfabetizacao/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 - Data Lake (Bronze / Silver / Gold)
resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake_lifecycle" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-to-glacier"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "silver-intelligent-tiering"
    status = "Enabled"
    filter { prefix = "silver/" }
    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
  }
}

# Kinesis Data Stream
resource "aws_kinesis_stream" "indicadores" {
  name             = "alfabetizacao-indicadores-stream"
  shard_count      = 1
  retention_period = 24
  tags             = local.common_tags
}

# SNS Topic para alertas
resource "aws_sns_topic" "alerts" {
  name = "alfabetizacao-alerts"
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# IAM Role para Glue Jobs
resource "aws_iam_role" "glue_role" {
  name = "tc-fase2-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = ["${aws_s3_bucket.data_lake.arn}", "${aws_s3_bucket.data_lake.arn}/*"]
    }]
  })
}

locals {
  common_tags = {
    Project     = "tech-challenge-fase2"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
