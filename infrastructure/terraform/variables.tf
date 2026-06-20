variable "aws_region" {
  description = "Regiao AWS para deploy dos recursos"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "Nome do bucket S3 principal (Data Lake)"
  type        = string
  default     = "tc-fase2-alfabetizacao"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "alert_email" {
  description = "Email para receber alertas CloudWatch via SNS"
  type        = string
}
