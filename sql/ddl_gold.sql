/* CAPA GOLD - MODELO DE NEGOCIO
   =============================
   Transformación de datos técnicos (Staging) a datos de negocio.
*/

-- 1. DIMENSIÓN ATM (Catálogo Maestro)
-- Extraemos la última versión de cada cajero (SCD Tipo 1 simple)
DROP TABLE IF EXISTS public.dim_atms;
CREATE TABLE public.dim_atms AS
SELECT DISTINCT 
    id_atm,
    ubicacion,
    modelo,
    capacidad_maxima,
    latitud,
    longitud,
    estado
FROM public.stg_fact_transactions;

-- 2. REPORTE DIARIO DE BALANCE (Tabla de Hechos Agregada)
-- Esta es la tabla que conectaría PowerBI. Es súper rápida de leer.
DROP TABLE IF EXISTS public.rpt_diario_balance;
CREATE TABLE public.rpt_diario_balance AS
SELECT 
    id_atm,
    TRUNC(fecha) as fecha_dia,
    COUNT(*) as total_transacciones,
    -- Pivotamos los montos según el tipo de movimiento
    SUM(CASE WHEN tipo_movimiento = 'DEPOSITO' THEN monto ELSE 0 END) as total_depositos,
    SUM(CASE WHEN tipo_movimiento = 'RETIRO' THEN monto ELSE 0 END) as total_retiros,
    -- Flujo neto del día (Depositos - Retiros)
    (SUM(CASE WHEN tipo_movimiento = 'DEPOSITO' THEN monto ELSE 0 END) - 
     SUM(CASE WHEN tipo_movimiento = 'RETIRO' THEN monto ELSE 0 END)) as flujo_neto_dia
FROM public.stg_fact_transactions
WHERE status_transaccion = 'EXITOSA'
GROUP BY id_atm, TRUNC(fecha);

-- 3. CONSULTA DE VALIDACIÓN (Lo que vería el Gerente)
SELECT * FROM public.rpt_diario_balance 
ORDER BY fecha_dia DESC, flujo_neto_dia ASC 
LIMIT 10;