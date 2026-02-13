/*
  LOGICASH - REDSHIFT DDL & LOAD SCRIPT
  =====================================
  Este script inicializa la tabla de staging en Redshift y carga
  los datos procesados desde S3 Silver.
  
  Notas Técnicas:
  1. Se usa FLOAT8 para latitud/longitud para compatibilidad con Spark Double.
  2. Se usa DECIMAL(18,2) para montos, alineado con el ETL.
  3. La carga es Full Load (Truncate/Drop + Copy).
*/

-- 1. LIMPIEZA PREVIA
-- Borramos la tabla si existe para asegurar que la definición sea la correcta
DROP TABLE IF EXISTS public.stg_fact_transactions;

-- 2. CREACIÓN DE TABLA (STAGING)
-- Definición alineada con el esquema Parquet generado por AWS Glue
CREATE TABLE public.stg_fact_transactions (
    id_atm VARCHAR(50),             -- Key del Join
    id_transaccion VARCHAR(50),
    fecha TIMESTAMP,
    monto DECIMAL(18,2),            -- Coincide con el casteo del ETL
    tipo_movimiento VARCHAR(20),
    status_transaccion VARCHAR(20),
    ubicacion VARCHAR(200),
    latitud FLOAT8,                 -- Tipo compatible con Parquet Double
    longitud FLOAT8,                -- Tipo compatible con Parquet Double
    capacidad_maxima BIGINT,        -- Entero largo para capacidades
    modelo VARCHAR(50),
    estado VARCHAR(20)
);

-- 3. CARGA DE DATOS (COPY)
-- Ingesta masiva desde S3 Silver
COPY public.stg_fact_transactions
FROM 's3://logicash-silver-paulo-zapata-9908/fact_transactions/'
IAM_ROLE 'arn:aws:iam::243768738483:role/logicash_redshift_serverless_role'
FORMAT AS PARQUET;

-- 4. VALIDACIÓN RÁPIDA (Sanity Check)
-- Verificar que cargaron datos y ver una muestra
SELECT COUNT(*) as total_registros FROM public.stg_fact_transactions;
SELECT * FROM public.stg_fact_transactions LIMIT 5;