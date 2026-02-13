-- ============================================================
-- LOGICASH - DDL STAGING: Carga de datos Silver → Redshift
-- ============================================================
-- Leído por Terraform via templatefile() e inyectado en la
-- Step Function para ejecutarse con Redshift Data API.
--
-- Variables inyectadas por Terraform:
--   ${silver_bucket}     → Nombre del bucket S3 Silver
--   ${redshift_role_arn} → ARN del IAM Role de Redshift para COPY
--
-- Notas Técnicas:
--   1. DECIMAL(18,2) para montos, alineado con el ETL de Glue.
--   2. FLOAT8 para lat/lon, compatible con Spark Double.
--   3. Full Load idempotente: DROP IF EXISTS + CREATE + COPY.
-- ============================================================

-- 1. Crear schema de staging si no existe
CREATE SCHEMA IF NOT EXISTS staging;

-- 2. Eliminar tabla anterior para idempotencia (full refresh)
DROP TABLE IF EXISTS staging.fact_transactions;

-- 3. Crear tabla de staging con esquema explícito
CREATE TABLE staging.fact_transactions (
    id_transaccion      VARCHAR(36),
    id_atm              VARCHAR(50),
    fecha               TIMESTAMP,
    monto               DECIMAL(18,2),
    tipo_movimiento     VARCHAR(20),
    status_transaccion  VARCHAR(20),
    ubicacion           VARCHAR(500),
    latitud             FLOAT8,
    longitud            FLOAT8,
    capacidad_maxima    BIGINT,
    modelo              VARCHAR(50),
    estado              VARCHAR(30)
);

-- 4. Cargar datos Parquet desde el bucket Silver usando COPY
COPY staging.fact_transactions
FROM 's3://${silver_bucket}/fact_transactions/'
IAM_ROLE '${redshift_role_arn}'
FORMAT AS PARQUET;