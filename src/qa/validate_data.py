"""
QA Data Validation Script - LogiCash ETL
========================================
Script independiente para validar datos procesados sin ejecutar el ETL completo.
Útil para debugging, análisis ad-hoc y validaciones rápidas.

Uso:
    spark-submit src/qa/validate_data.py

Prerequisitos:
    - Datos procesados en: data/processed/fact_transactions/ (formato Parquet)
    - O datos raw en: data/raw/ (CSVs) para validación en caliente
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, broadcast

# --- CONFIGURACIÓN DEL ENTORNO ---
print("🔍 === QA DATA VALIDATION SCRIPT ===")
print("Inicializando entorno Spark para validaciones...")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("logicash_qa_validation", {})

print("✅ Entorno inicializado correctamente.")

# --- DECISIÓN: ¿VALIDAR DESDE PARQUET O DESDE RAW? ---
print("\n📁 Detectando fuente de datos disponible...")

processed_path = "data/processed/fact_transactions"
raw_atms_path = "data/raw/dim_atms.csv"
raw_transactions_path = "data/raw/fact_transactions.csv"

try:
    # Intentar leer desde datos procesados (más rápido)
    print(f"🔍 Intentando leer desde: {processed_path}")
    df_clean = spark.read.parquet(processed_path)
    print("✅ Leyendo desde datos PROCESADOS (Parquet)")
    data_source = "processed"
    
except Exception:
    # Si no hay datos procesados, generar desde raw
    print("⚠️  Datos procesados no encontrados. Generando desde RAW...")
    try:
        # Recrear transformaciones mínimas
        df_atms = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(raw_atms_path)
            
        df_transactions = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(raw_transactions_path)
            
        # JOIN y limpieza básica
        df_joined = df_transactions.join(broadcast(df_atms), on="id_atm", how="left")
        df_clean = df_joined.filter(
            (col("id_atm").isNotNull()) & 
            (col("monto") > 0) & 
            (col("status_transaccion") == 'EXITOSA')
        ).withColumn("fecha_dia", to_date(col("fecha")))
        
        print("✅ Datos generados desde RAW")
        data_source = "raw"
        
    except Exception as e:
        print(f"❌ Error: No se pueden cargar datos desde ninguna fuente")
        print(f"    Procesados: {processed_path}")
        print(f"    Raw: {raw_atms_path}, {raw_transactions_path}")
        print(f"    Error: {str(e)}")
        sys.exit(1)

# --- VALIDACIONES DE CALIDAD ---
print(f"\n🔍 === VALIDACIONES DE CALIDAD ({data_source.upper()}) ===")

# Registrar como vista temporal
df_clean.createOrReplaceTempView("transactions_clean")

# Estadísticas generales
print(f"\n📊 Estadísticas Generales:")
total_records = df_clean.count()
print(f"   📈 Total de registros: {total_records:,}")

print("\n📊 Top 5 Cajeros con Más Dinero Movido:")
print("=" * 50)
top_atms_query = """
    SELECT 
        id_atm,
        ubicacion,
        COUNT(*) as total_transacciones,
        ROUND(SUM(monto), 2) as dinero_total_movido,
        ROUND(AVG(monto), 2) as monto_promedio
    FROM transactions_clean 
    GROUP BY id_atm, ubicacion
    ORDER BY dinero_total_movido DESC
    LIMIT 5
"""
spark.sql(top_atms_query).show(truncate=False)

print("\n📅 Total de Dinero por Día:")
print("=" * 35)
daily_summary_query = """
    SELECT 
        fecha_dia,
        COUNT(*) as total_transacciones,
        ROUND(SUM(monto), 2) as dinero_total_dia,
        ROUND(MIN(monto), 2) as monto_min,
        ROUND(MAX(monto), 2) as monto_max,
        ROUND(AVG(monto), 2) as promedio
    FROM transactions_clean 
    GROUP BY fecha_dia
    ORDER BY fecha_dia DESC
"""
spark.sql(daily_summary_query).show(truncate=False)

print("\n🚨 Verificaciones de Calidad:")
print("=" * 30)

# Verificación de nulls
null_check_query = """
    SELECT 
        COUNT(*) as total_registros,
        COUNT(id_atm) as id_atm_validos,
        COUNT(monto) as montos_validos,
        COUNT(ubicacion) as ubicaciones_validas
    FROM transactions_clean
"""
print("🔍 Verificación de NULLs:")
spark.sql(null_check_query).show()

# Verificación de rangos
range_check_query = """
    SELECT 
        MIN(monto) as monto_minimo,
        MAX(monto) as monto_maximo,
        COUNT(CASE WHEN monto <= 0 THEN 1 END) as montos_invalidos,
        COUNT(DISTINCT id_atm) as cajeros_unicos
    FROM transactions_clean
"""
print("📏 Verificación de Rangos:")
spark.sql(range_check_query).show()

print("\n✅ === VALIDACIONES COMPLETADAS ===")
print(f"Fuente de datos: {data_source.upper()}")
print("Si encuentras inconsistencias, revisar:")
print("  1. Filtros de calidad en etl_job.py")
print("  2. Generación de datos en data_generator.py")
print("  3. Integridad de joins entre dimensiones y hechos")

job.commit()
