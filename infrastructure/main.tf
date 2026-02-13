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

# Habilitar notificaciones de EventBridge en el bucket Raw
# Necesario para que EventBridge detecte la creación del archivo _READY
resource "aws_s3_bucket_notification" "raw_bucket_notification" {
  bucket      = aws_s3_bucket.raw_bucket.id
  eventbridge = true
}

# --- S3 PLACEHOLDERS (Carpetas virtuales para estructura incremental) ---
# S3 no tiene carpetas reales; estos objetos vacíos crean la estructura esperada
# para que el Crawler y Spark encuentren las rutas correctas.

resource "aws_s3_object" "folder_dim_atms" {
  bucket  = aws_s3_bucket.raw_bucket.id
  key     = "dim_atms/"
  content = ""
}

resource "aws_s3_object" "folder_fact_transactions" {
  bucket  = aws_s3_bucket.raw_bucket.id
  key     = "fact_transactions/"
  content = ""
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
          "s3:DeleteObject",
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

  # Crawlear cada subcarpeta por separado para que Glue cree una tabla por cada una
  s3_target {
    path = "s3://${aws_s3_bucket.raw_bucket.bucket}/dim_atms/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.raw_bucket.bucket}/fact_transactions/"
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
    "--bucket_raw"                          = aws_s3_bucket.raw_bucket.id
    "--bucket_processed"                    = aws_s3_bucket.silver_bucket.id
    "--job-language"                        = "python"
    "--enable-continuous-cloudwatch-log"     = "true"
    "--enable-cloudwatch-log-level"          = "INFO"
  }

  # Depende explícitamente de que el script ya exista en S3
  depends_on = [aws_s3_object.etl_script]

  tags = {
    Name    = "LogiCash ETL Job"
    Project = "LogiCash"
  }
}

# ============================================================
# 6. AWS STEP FUNCTIONS - ORQUESTACIÓN DEL PIPELINE
# ============================================================
# Automatiza el flujo completo: Crawler → Espera → ETL Job
# Usa un loop de polling para verificar que el Crawler termine antes de lanzar el Job.

# --- IAM ROLE PARA STEP FUNCTIONS ---
# Trust Policy: Permite que el servicio Step Functions asuma este rol
data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "step_functions_role" {
  name               = "${local.project_name}_step_functions_role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json

  tags = {
    Name    = "Step Functions Orchestrator Role"
    Project = "LogiCash"
  }
}

# --- IAM POLICY: S3 Delete para eliminar _READY trigger ---
# Permite a Step Functions borrar el archivo flag al inicio del pipeline
resource "aws_iam_role_policy" "sfn_s3_delete_policy" {
  name = "${local.project_name}_sfn_s3_delete_policy"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.raw_bucket.arn}/_READY"]
      }
    ]
  })
}

# --- IAM POLICY: Permisos de Glue + Redshift Data API para Step Functions ---
# Permite iniciar/consultar Crawlers, Jobs y ejecutar SQL en Redshift Serverless
resource "aws_iam_role_policy" "sfn_glue_policy" {
  name = "${local.project_name}_sfn_glue_policy"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler",
          "glue:StartJobRun",
          "glue:GetJobRun"
        ]
        Resource = "*"
      },
      {
        # Permisos para Redshift Data API (ejecutar SQL desde Step Functions)
        Effect = "Allow"
        Action = [
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement",
          "redshift-data:GetStatementResult"
        ]
        Resource = "*"
      },
      {
        # Permiso para que Redshift Data API obtenga credenciales
        # Requiere acceso tanto al Namespace como al Workgroup
        Effect = "Allow"
        Action = [
          "redshift-serverless:GetCredentials"
        ]
        Resource = [
          aws_redshiftserverless_namespace.logicash_namespace.arn,
          aws_redshiftserverless_workgroup.logicash_workgroup.arn
        ]
      },
      {
        # Permiso para leer el secreto de credenciales de Redshift
        # Necesario cuando se usa secret_arn en Redshift Data API
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.redshift_admin_secret.arn
        ]
      }
    ]
  })
}

# --- STATE MACHINE ---
# Flujo completo: DeleteTrigger → Crawler → polling → ETL Job → Redshift SQL
# El trigger _READY se elimina al inicio para evitar re-ejecuciones accidentales
resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${local.project_name}_etl_pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "LogiCash ETL Pipeline - Crawler + Glue Job + Redshift SQL"
    StartAt = "DeleteTriggerFile"
    States = {

      # Paso 0: Eliminar el archivo _READY para evitar re-disparos
      # CRÍTICO: Se ejecuta PRIMERO para garantizar idempotencia del trigger
      DeleteTriggerFile = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:s3:deleteObject"
        Parameters = {
          Bucket = aws_s3_bucket.raw_bucket.id
          Key    = "_READY"
        }
        ResultPath = null
        Next       = "StartCrawler"
      }

      # Paso 1: Iniciar el Crawler para catalogar datos crudos en S3
      StartCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.raw_crawler.name
        }
        ResultPath = null
        Next       = "WaitForCrawler"
      }

      # Paso 2: Esperar 15 segundos antes de consultar el estado
      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 15
        Next    = "GetCrawlerStatus"
      }

      # Paso 3: Consultar el estado actual del Crawler
      GetCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.raw_crawler.name
        }
        ResultPath = "$.CrawlerResult"
        Next       = "CheckCrawlerStatus"
      }

      # Paso 4: Evaluar si el Crawler terminó o sigue corriendo
      CheckCrawlerStatus = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.CrawlerResult.Crawler.State"
            StringEquals = "RUNNING"
            Next         = "WaitForCrawler"
          },
          {
            Variable     = "$.CrawlerResult.Crawler.State"
            StringEquals = "STOPPING"
            Next         = "WaitForCrawler"
          }
        ]
        Default = "RunETLJob"
      }

      # Paso 5: Ejecutar el Job ETL de forma sincrónica (.sync)
      RunETLJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.etl_job.name
        }
        ResultPath = null
        Next       = "RunRedshiftSQL"
      }

      # Paso 6: Cargar datos Silver a Redshift Serverless
      # SQL leído desde archivos locales para Single Source of Truth:
      #   - sql/ddl_staging.sql → templatefile() (inyecta bucket y role ARN)
      #   - sql/ddl_gold.sql   → file() (SQL puro, sin variables)
      # Terraform concatena ambos en un solo bloque SQL en tiempo de plan.
      RunRedshiftSQL = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:redshiftdata:executeStatement"
        Parameters = {
          WorkgroupName = aws_redshiftserverless_workgroup.logicash_workgroup.workgroup_name
          Database      = "dev"
          SecretArn     = aws_secretsmanager_secret.redshift_admin_secret.arn
          Sql           = "${templatefile("../sql/ddl_staging.sql", { silver_bucket = aws_s3_bucket.silver_bucket.bucket, redshift_role_arn = aws_iam_role.redshift_serverless_role.arn })}\n${file("../sql/ddl_gold.sql")}"
        }
        ResultPath = "$.RedshiftResult"
        Next       = "WaitForRedshift"
      }

      # Paso 7: Esperar a que el SQL de Redshift termine (polling asíncrono)
      WaitForRedshift = {
        Type    = "Wait"
        Seconds = 10
        Next    = "GetRedshiftStatus"
      }

      # Paso 8: Consultar el estado de la ejecución SQL
      GetRedshiftStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:redshiftdata:describeStatement"
        Parameters = {
          "Id.$" = "$.RedshiftResult.Id"
        }
        ResultPath = "$.RedshiftStatus"
        Next       = "CheckRedshiftStatus"
      }

      # Paso 9: Evaluar si el SQL terminó, sigue corriendo o falló
      CheckRedshiftStatus = {
        Type = "Choice"
        Choices = [
          {
            # SQL aún ejecutándose → seguir esperando
            Variable     = "$.RedshiftStatus.Status"
            StringEquals = "STARTED"
            Next         = "WaitForRedshift"
          },
          {
            Variable     = "$.RedshiftStatus.Status"
            StringEquals = "SUBMITTED"
            Next         = "WaitForRedshift"
          },
          {
            Variable     = "$.RedshiftStatus.Status"
            StringEquals = "PICKED"
            Next         = "WaitForRedshift"
          },
          {
            # SQL terminó exitosamente
            Variable     = "$.RedshiftStatus.Status"
            StringEquals = "FINISHED"
            Next         = "PipelineSuccess"
          },
          {
            # SQL falló
            Variable     = "$.RedshiftStatus.Status"
            StringEquals = "FAILED"
            Next         = "RedshiftFailed"
          }
        ]
        Default = "WaitForRedshift"
      }

      # Paso 10: Pipeline completado exitosamente
      PipelineSuccess = {
        Type = "Succeed"
      }

      # Paso 11: Redshift SQL falló
      RedshiftFailed = {
        Type  = "Fail"
        Error = "RedshiftSQLFailed"
        Cause = "La ejecución SQL en Redshift Serverless falló. Revisar CloudWatch logs."
      }
    }
  })

  tags = {
    Name    = "LogiCash ETL Pipeline"
    Project = "LogiCash"
  }
}

# ============================================================
# 7. REDSHIFT SERVERLESS - DATA WAREHOUSING
# ============================================================
# Capa analítica para consultas SQL sobre datos procesados (Silver/Parquet).
# Optimizado para Free Trial de AWS ($300 créditos).

# --- 7.1 NETWORKING (VPC, Subnets, Internet Gateway) ---

# VPC dedicada para Redshift Serverless
resource "aws_vpc" "logicash_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name    = "${local.project_name}_vpc"
    Project = "LogiCash"
  }
}

# Internet Gateway para acceso público desde PC local
resource "aws_internet_gateway" "logicash_igw" {
  vpc_id = aws_vpc.logicash_vpc.id

  tags = {
    Name    = "${local.project_name}_igw"
    Project = "LogiCash"
  }
}

# Route Table con ruta a Internet
resource "aws_route_table" "logicash_rt" {
  vpc_id = aws_vpc.logicash_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.logicash_igw.id
  }

  tags = {
    Name    = "${local.project_name}_route_table"
    Project = "LogiCash"
  }
}

# 3 Subnets Públicas en 3 AZs diferentes (requerido por Redshift Serverless para HA)
resource "aws_subnet" "logicash_subnet_a" {
  vpc_id                  = aws_vpc.logicash_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    Name    = "${local.project_name}_subnet_a"
    Project = "LogiCash"
  }
}

resource "aws_subnet" "logicash_subnet_b" {
  vpc_id                  = aws_vpc.logicash_vpc.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true

  tags = {
    Name    = "${local.project_name}_subnet_b"
    Project = "LogiCash"
  }
}

resource "aws_subnet" "logicash_subnet_c" {
  vpc_id                  = aws_vpc.logicash_vpc.id
  cidr_block              = "10.0.3.0/24"
  availability_zone       = "us-east-1c"
  map_public_ip_on_launch = true

  tags = {
    Name    = "${local.project_name}_subnet_c"
    Project = "LogiCash"
  }
}

# Asociar las 3 subnets a la Route Table pública
resource "aws_route_table_association" "rta_a" {
  subnet_id      = aws_subnet.logicash_subnet_a.id
  route_table_id = aws_route_table.logicash_rt.id
}

resource "aws_route_table_association" "rta_b" {
  subnet_id      = aws_subnet.logicash_subnet_b.id
  route_table_id = aws_route_table.logicash_rt.id
}

resource "aws_route_table_association" "rta_c" {
  subnet_id      = aws_subnet.logicash_subnet_c.id
  route_table_id = aws_route_table.logicash_rt.id
}

# Security Group para Redshift Serverless
# Puerto 5439 abierto a 0.0.0.0/0 (solo entorno educativo, NO para producción)
resource "aws_security_group" "redshift_sg" {
  name        = "${local.project_name}_redshift_sg"
  description = "Security Group para Redshift Serverless - Entorno educativo"
  vpc_id      = aws_vpc.logicash_vpc.id

  # Ingress: Puerto 5439 (Redshift) desde cualquier IP
  ingress {
    description = "Redshift access from anywhere (educational only)"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Egress: Salida sin restricciones
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${local.project_name}_redshift_sg"
    Project = "LogiCash"
  }
}

# --- 7.2 SEGURIDAD: Contraseña + Secrets Manager ---
# Genera una contraseña segura y la almacena en Secrets Manager.
# Elimina la necesidad de credenciales hardcodeadas o procesos manuales.

# Contraseña aleatoria para el usuario admin de Redshift
resource "random_password" "redshift_admin_password" {
  length  = 16
  special = true

  # Redshift no permite / @ " ' espacio en contraseñas
  override_special = "!#$%^&*()-_=+[]{}|;:,.<>?"
}

# Secreto en AWS Secrets Manager (la "bóveda")
resource "aws_secretsmanager_secret" "redshift_admin_secret" {
  name        = "${local.project_name}/redshift/admin-credentials"
  description = "Credenciales del usuario admin de Redshift Serverless para LogiCash"

  tags = {
    Name    = "Redshift Admin Credentials"
    Project = "LogiCash"
  }
}

# Versión del secreto: Almacena el JSON con username y password
resource "aws_secretsmanager_secret_version" "redshift_admin_secret_value" {
  secret_id = aws_secretsmanager_secret.redshift_admin_secret.id

  secret_string = jsonencode({
    username = "admin"
    password = random_password.redshift_admin_password.result
  })
}

# --- 7.3 IAM ROLE PARA REDSHIFT SERVERLESS ---
# Permite a Redshift leer datos desde S3 (COPY command)

data "aws_iam_policy_document" "redshift_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "redshift_serverless_role" {
  name               = "${local.project_name}_redshift_serverless_role"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume_role.json

  tags = {
    Name    = "Redshift Serverless Role"
    Project = "LogiCash"
  }
}

# Política S3 ReadOnly para ejecutar COPY desde el bucket Silver
resource "aws_iam_role_policy_attachment" "redshift_s3_read" {
  role       = aws_iam_role.redshift_serverless_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# --- 7.4 REDSHIFT SERVERLESS (Namespace + Workgroup) ---

# Namespace: Contenedor lógico de la base de datos y configuración
# Usa credenciales generadas automáticamente (no hardcodeadas)
resource "aws_redshiftserverless_namespace" "logicash_namespace" {
  namespace_name      = "${local.project_name}-namespace"
  db_name             = "dev"
  admin_username      = "admin"
  admin_user_password = random_password.redshift_admin_password.result
  iam_roles           = [aws_iam_role.redshift_serverless_role.arn]

  tags = {
    Name    = "LogiCash Redshift Namespace"
    Project = "LogiCash"
  }
}

# Workgroup: Compute layer con capacidad mínima para Free Trial
resource "aws_redshiftserverless_workgroup" "logicash_workgroup" {
  workgroup_name = "${local.project_name}-workgroup"
  namespace_name = aws_redshiftserverless_namespace.logicash_namespace.namespace_name

  # CRÍTICO: 8 RPU es el mínimo para Free Trial ($300 créditos)
  base_capacity = 8

  # Subnets en 3 AZs (requerido por Redshift Serverless para HA)
  subnet_ids = [
    aws_subnet.logicash_subnet_a.id,
    aws_subnet.logicash_subnet_b.id,
    aws_subnet.logicash_subnet_c.id
  ]

  security_group_ids  = [aws_security_group.redshift_sg.id]
  publicly_accessible = true

  tags = {
    Name    = "LogiCash Redshift Workgroup"
    Project = "LogiCash"
  }
}

# --- 7.5 CONTROL DE COSTOS (Usage Limit) ---
# Limita el consumo diario de RPU-hours para proteger los créditos del Free Trial
resource "aws_redshiftserverless_usage_limit" "compute_limit" {
  resource_arn  = aws_redshiftserverless_workgroup.logicash_workgroup.arn
  usage_type    = "serverless-compute"
  amount        = 60
  period        = "daily"
  breach_action = "log"
}

# --- 7.6 BOOTSTRAPPING: SQL Grants automáticos ---
# Ejecuta GRANT de permisos al rol IAM de Step Functions al crear la infraestructura.
# Esto permite que la Step Function ejecute SQL en Redshift via Data API
# sin necesidad de intervención manual post-deploy.
resource "aws_redshiftdata_statement" "init_redshift_permissions" {
  workgroup_name = aws_redshiftserverless_workgroup.logicash_workgroup.workgroup_name
  database       = "dev"
  secret_arn     = aws_secretsmanager_secret.redshift_admin_secret.arn

  sql = join("; ", [
    "CREATE USER \"IAMR:${local.project_name}_step_functions_role\" PASSWORD DISABLE",
    "GRANT USAGE ON DATABASE dev TO \"IAMR:${local.project_name}_step_functions_role\"",
    "GRANT CREATE ON DATABASE dev TO \"IAMR:${local.project_name}_step_functions_role\"",
    "ALTER USER \"IAMR:${local.project_name}_step_functions_role\" CREATEUSER"
  ])

  # Espera a que el Workgroup y el secreto estén disponibles
  depends_on = [
    aws_redshiftserverless_workgroup.logicash_workgroup,
    aws_secretsmanager_secret_version.redshift_admin_secret_value
  ]
}

# --- 7.7 OUTPUTS ---

output "redshift_endpoint" {
  description = "Endpoint de conexión para Redshift Serverless"
  value       = aws_redshiftserverless_workgroup.logicash_workgroup.endpoint
}

output "redshift_workgroup_name" {
  description = "Nombre del workgroup para conexión"
  value       = aws_redshiftserverless_workgroup.logicash_workgroup.workgroup_name
}

output "redshift_secret_arn" {
  description = "ARN del secreto en Secrets Manager con credenciales de Redshift"
  value       = aws_secretsmanager_secret.redshift_admin_secret.arn
}

# ============================================================
# 8. EVENTBRIDGE - TRIGGER AUTOMÁTICO POR FLAG FILE
# ============================================================
# Detecta la creación del archivo _READY en el bucket Raw para disparar
# la Step Function. Evita condiciones de carrera: el pipeline SOLO arranca
# cuando todos los CSVs del lote ya fueron subidos.

# --- REGLA DE EVENTBRIDGE ---
# Filtra eventos S3 Object Created con key exacta "_READY"
# NO se dispara con archivos .csv individuales
resource "aws_cloudwatch_event_rule" "s3_ready_trigger" {
  name        = "${local.project_name}_s3_ready_trigger"
  description = "Dispara el pipeline ETL cuando se detecta el archivo _READY en el bucket Raw"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.raw_bucket.id]
      }
      object = {
        key = ["_READY"]
      }
    }
  })

  tags = {
    Name    = "S3 Ready File Trigger"
    Project = "LogiCash"
  }
}

# --- TARGET: Step Functions ---
# Conecta la regla de EventBridge con la State Machine
resource "aws_cloudwatch_event_target" "trigger_step_function" {
  rule     = aws_cloudwatch_event_rule.s3_ready_trigger.name
  arn      = aws_sfn_state_machine.etl_pipeline.arn
  role_arn = aws_iam_role.eventbridge_sfn_role.arn
}

# --- IAM ROLE PARA EVENTBRIDGE ---
# Permite a EventBridge iniciar ejecuciones de la Step Function
data "aws_iam_policy_document" "eventbridge_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "eventbridge_sfn_role" {
  name               = "${local.project_name}_eventbridge_sfn_role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume_role.json

  tags = {
    Name    = "EventBridge to Step Functions Role"
    Project = "LogiCash"
  }
}

resource "aws_iam_role_policy" "eventbridge_sfn_policy" {
  name = "${local.project_name}_eventbridge_sfn_policy"
  role = aws_iam_role.eventbridge_sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.etl_pipeline.arn]
      }
    ]
  })
}