# 1. EL PROVEEDOR
provider "aws" {
  region = "us-east-1"
}

# 2. VARIABLES LOCALES
locals {
  project_name = "logicash"
  suffix       = "paulo-zapata-9908"
}

# 3. LOS RECURSOS

# Bucket para Datos Crudos (Bronze)
resource "aws_s3_bucket" "raw_bucket" {
  bucket = "${local.project_name}-raw-${local.suffix}"

  tags = {
    Name        = "Capa Bronze Raw"
    Environment = "Dev"
    Project     = "LogiCash"
  }
}

# Bucket para Datos Limpios (Silver)
resource "aws_s3_bucket" "silver_bucket" {
  bucket = "${local.project_name}-silver-${local.suffix}"
  force_destroy = true

  tags = {
    Name = "Capa Silver Curated"
  }
}

# Bucket para Datos Finales (Gold)
resource "aws_s3_bucket" "gold_bucket" {
  bucket = "${local.project_name}-gold-${local.suffix}"
  force_destroy = true

  tags = {
    Name = "Capa Gold Analytics"
  }
}

# Bucket para Scripts de Glue y Logs
resource "aws_s3_bucket" "scripts_bucket" {
  bucket = "${local.project_name}-assets-${local.suffix}"
  force_destroy = true

  tags = {
    Name = "Glue Scripts and Temp"
  }
}