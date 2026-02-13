-- ============================================================
-- LOGICASH - DDL GOLD: Modelo de Negocio para Analytics
-- ============================================================
-- Leído por Terraform via templatefile() e inyectado en la Step Function.
-- Variables inyectadas:
--   ${gold_bucket}       → Bucket S3 destino para UNLOAD
--   ${redshift_role_arn}  → ARN del rol IAM de Redshift
--
-- Transforma datos técnicos del schema staging → tablas de negocio
-- en el schema gold, listas para consumo por BI (Power BI, QuickSight).
-- Luego exporta (UNLOAD) cada tabla Gold a S3 en formato Parquet.
--
-- Idempotente: Usa DROP TABLE IF EXISTS antes de cada CREATE.
--              Usa ALLOWOVERWRITE en cada UNLOAD.
-- ============================================================

-- 1. Crear schema gold si no existe
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. DIMENSIÓN ATM (Catálogo Maestro)
-- Extraemos la última versión de cada cajero (SCD Tipo 1 simple)
DROP TABLE IF EXISTS gold.dim_atms;
CREATE TABLE gold.dim_atms AS
SELECT DISTINCT
    id_atm,
    ubicacion,
    modelo,
    capacidad_maxima,
    latitud,
    longitud,
    estado
FROM staging.fact_transactions;

-- 3. REPORTE DIARIO DE BALANCE (Tabla de Hechos Agregada)
-- Tabla principal para dashboards. Pivotea montos por tipo de movimiento.
DROP TABLE IF EXISTS gold.rpt_diario_balance;
CREATE TABLE gold.rpt_diario_balance AS
SELECT
    id_atm,
    TRUNC(fecha) AS fecha_dia,
    COUNT(*) AS total_transacciones,
    SUM(CASE WHEN tipo_movimiento = 'DEPOSITO' THEN monto ELSE 0 END) AS total_depositos,
    SUM(CASE WHEN tipo_movimiento = 'RETIRO' THEN monto ELSE 0 END) AS total_retiros,
    (SUM(CASE WHEN tipo_movimiento = 'DEPOSITO' THEN monto ELSE 0 END) -
     SUM(CASE WHEN tipo_movimiento = 'RETIRO' THEN monto ELSE 0 END)) AS flujo_neto_dia
FROM staging.fact_transactions
GROUP BY id_atm, TRUNC(fecha);

-- 4. RANKING DE ATMs POR VOLUMEN TOTAL
-- Top ATMs por dinero total movido, útil para priorizar recargas.
DROP TABLE IF EXISTS gold.top_atms_ranking;
CREATE TABLE gold.top_atms_ranking AS
SELECT
    id_atm,
    ubicacion,
    modelo,
    COUNT(*) AS total_transacciones,
    SUM(monto) AS dinero_total_movido,
    AVG(monto) AS monto_promedio
FROM staging.fact_transactions
GROUP BY id_atm, ubicacion, modelo
ORDER BY dinero_total_movido DESC;

-- ============================================================
-- 5. UNLOAD: Exportar tablas Gold a S3 en formato Parquet
-- ============================================================
-- Permite consumo externo (Athena, QuickSight, Data Lake) sin
-- depender de Redshift activo. ALLOWOVERWRITE para idempotencia.

-- 5.1 Exportar dim_atms
UNLOAD ('SELECT * FROM gold.dim_atms')
TO 's3://${gold_bucket}/dim_atms/'
IAM_ROLE '${redshift_role_arn}'
FORMAT AS PARQUET
ALLOWOVERWRITE;

-- 5.2 Exportar rpt_diario_balance
UNLOAD ('SELECT * FROM gold.rpt_diario_balance')
TO 's3://${gold_bucket}/rpt_diario_balance/'
IAM_ROLE '${redshift_role_arn}'
FORMAT AS PARQUET
ALLOWOVERWRITE;

-- 5.3 Exportar top_atms_ranking
UNLOAD ('SELECT * FROM gold.top_atms_ranking')
TO 's3://${gold_bucket}/top_atms_ranking/'
IAM_ROLE '${redshift_role_arn}'
FORMAT AS PARQUET
ALLOWOVERWRITE;