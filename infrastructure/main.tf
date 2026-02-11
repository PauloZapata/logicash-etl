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

# ============================================================
# 4. AWS GLUE - CATALOGACIÓN DE DATOS
# ============================================================

# --- IAM ROLE PARA GLUE ---
# Este rol permite al servicio Glue asumir permisos para leer/escribir en S3
# y ejecutar operaciones de catalogación de datos.

# Trust Policy: Permite que el servicio Glue asuma este rol
data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

# Crear el rol IAM para Glue
resource "aws_iam_role" "glue_role" {
  name               = "${local.project_name}_glue_role_${local.suffix}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  tags = {
    Name    = "Glue ETL Role"
    Project = "LogiCash"
  }
}

# Adjuntar política gestionada AWSGlueServiceRole (permisos base de Glue)
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Política inline: Permisos específicos de S3 sobre nuestros buckets
# Principio de menor privilegio: Solo acceso a Raw (lectura) y Silver (escritura)
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${local.project_name}_glue_s3_policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.raw_bucket.arn}",
          "${aws_s3_bucket.raw_bucket.arn}/*",
          "${aws_s3_bucket.silver_bucket.arn}",
          "${aws_s3_bucket.silver_bucket.arn}/*",
          "${aws_s3_bucket.scripts_bucket.arn}",
          "${aws_s3_bucket.scripts_bucket.arn}/*"
        ]
      }
    ]
  })
}

# --- GLUE DATABASE ---
# Base de datos del catálogo donde se registrarán las tablas descubiertas
resource "aws_glue_catalog_database" "logicash_db" {
  name = "${local.project_name}_db"

  description = "Base de datos del catálogo para el proyecto LogiCash. Contiene tablas de transacciones y ATMs."
}

# --- GLUE CRAWLER ---
# Crawler que escanea el bucket Raw y descubre automáticamente el schema de los datos
resource "aws_glue_crawler" "raw_crawler" {
  name          = "${local.project_name}_raw_crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.logicash_db.name
  description   = "Crawler que cataloga los datos crudos del bucket Bronze/Raw"

  s3_target {
    path = "s3://${aws_s3_bucket.raw_bucket.bucket}"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Name    = "Raw Data Crawler"
    Project = "LogiCash"
  }
}

# ============================================================
# 5. AWS GLUE - ETL JOB
# ============================================================

# --- SUBIR SCRIPT ETL A S3 ---
# Sube el archivo local del ETL Job al bucket de scripts.
# El etag con filemd5() detecta cambios en el archivo local para re-subirlo automáticamente.
resource "aws_s3_object" "etl_script" {
  bucket = aws_s3_bucket.scripts_bucket.id
  key    = "scripts/etl_job.py"
  source = "../glue_jobs/etl_job.py"
  etag   = filemd5("../glue_jobs/etl_job.py")

  tags = {
    Name    = "ETL Job Script"
    Project = "LogiCash"
  }
}

# --- GLUE JOB ---
# Job ETL que ejecuta la pipeline Bronze -> Silver usando PySpark.
# Lee CSVs del bucket Raw, transforma y escribe Parquet al bucket Silver.
resource "aws_glue_job" "etl_job" {
  name     = "${local.project_name}_etl_job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.scripts_bucket.bucket}/scripts/etl_job.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  # Argumentos dinámicos que el script recibe via getResolvedOptions
  # Estos reemplazan las rutas S3 hardcodeadas en el código
  default_arguments = {
    "--bucket_raw"       = aws_s3_bucket.raw_bucket.id
    "--bucket_processed" = aws_s3_bucket.silver_bucket.id
    "--job-language"     = "python"
  }

  # Depende explícitamente de que el script ya exista en S3
  depends_on = [aws_s3_object.etl_script]

  tags = {
    Name    = "LogiCash ETL Job"
    Project = "LogiCash"
  }
}