-- ============================================================
-- LOGICASH - DDL GOLD: Modelo de Negocio para Analytics
-- ============================================================
-- Leído por Terraform via file() e inyectado en la Step Function.
-- No requiere variables de Terraform (SQL puro).
--
-- Transforma datos técnicos del schema staging → tablas de negocio
-- en el schema gold, listas para consumo por BI (Power BI, QuickSight).
--
-- Idempotente: Usa DROP TABLE IF EXISTS antes de cada CREATE.
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