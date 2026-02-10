"""
QA Data Validation Script - OPTIMIZADO para Performance
======================================================
Versión optimizada con cache, reutilización de contexto y consultas eficientes.

MEJORAS DE PERFORMANCE:
- Cache de DataFrames en memoria
- Reutilización de vistas temporales
- Consultas combinadas (menos roundtrips)
- Schema explícito para CSVs
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, broadcast
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import time

# --- CONFIGURACIÓN OPTIMIZADA ---
print("🚀 === QA DATA VALIDATION (PERFORMANCE OPTIMIZED) ===")
start_time = time.time()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# OPTIMIZACIÓN 1: Configuración de Spark para mejor performance local
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

job = Job(glueContext)
job.init("logicash_qa_validation_fast", {})

init_time = time.time() - start_time
print(f"✅ Entorno inicializado en {init_time:.2f}s")

# --- CARGA DE DATOS OPTIMIZADA ---
print("\n📁 Cargando datos con optimizaciones...")
load_start = time.time()

processed_path = "data/processed/fact_transactions"
raw_atms_path = "data/raw/dim_atms.csv"
raw_transactions_path = "data/raw/fact_transactions.csv"

try:
    # OPTIMIZACIÓN 2: Leer Parquet (10x más rápido que CSV)
    print(f"🔍 Intentando leer desde: {processed_path}")
    df_clean = spark.read.parquet(processed_path)
    
    # OPTIMIZACIÓN 3: Cache en memoria para múltiples consultas
    df_clean.cache()
    record_count = df_clean.count()  # Fuerza el cache
    
    print(f"✅ Datos PROCESADOS cargados: {record_count:,} registros")
    data_source = "processed"
    
except Exception:
    print("⚠️  Generando desde RAW con schema explícito...")
    
    # OPTIMIZACIÓN 4: Schema explícito (evita inferencia costosa)
    atm_schema = StructType([
        StructField("id_atm", StringType(), True),
        StructField("ubicacion", StringType(), True),
        StructField("zona", StringType(), True),
        StructField("tipo_atm", StringType(), True)
    ])
    
    transaction_schema = StructType([
        StructField("id_transaccion", StringType(), True),
        StructField("id_atm", StringType(), True),
        StructField("fecha", TimestampType(), True),
        StructField("monto", DoubleType(), True),
        StructField("tipo_transaccion", StringType(), True),
        StructField("status_transaccion", StringType(), True)
    ])
    
    try:
        df_atms = spark.read.format("csv") \
            .option("header", "true") \
            .schema(atm_schema) \
            .load(raw_atms_path)
            
        df_transactions = spark.read.format("csv") \
            .option("header", "true") \
            .schema(transaction_schema) \
            .load(raw_transactions_path)
            
        # OPTIMIZACIÓN 5: Cache dimensiones pequeñas
        df_atms.cache()
        df_atms.count()  # Fuerza cache
        
        df_joined = df_transactions.join(broadcast(df_atms), on="id_atm", how="left")
        df_clean = df_joined.filter(
            (col("id_atm").isNotNull()) & 
            (col("monto") > 0) & 
            (col("status_transaccion") == 'EXITOSA')
        ).withColumn("fecha_dia", to_date(col("fecha")))
        
        # Cache resultado final
        df_clean.cache()
        record_count = df_clean.count()
        
        print(f"✅ Datos RAW procesados: {record_count:,} registros")
        data_source = "raw"
        
    except Exception as e:
        print(f"❌ Error cargando datos: {str(e)}")
        sys.exit(1)

load_time = time.time() - load_start
print(f"⏱️  Carga completada en {load_time:.2f}s")

# --- VALIDACIONES OPTIMIZADAS ---
print(f"\n🔍 === VALIDACIONES RÁPIDAS ({data_source.upper()}) ===")
validation_start = time.time()

# OPTIMIZACIÓN 6: Una sola vista temporal, múltiples consultas
df_clean.createOrReplaceTempView("transactions_clean")

# OPTIMIZACIÓN 7: Consulta combinada para estadísticas básicas
print("\n📊 Estadísticas Generales + Top Cajeros:")
combined_stats_query = """
WITH cajero_stats AS (
    SELECT 
        id_atm,
        ubicacion,
        COUNT(*) as total_transacciones,
        ROUND(SUM(monto), 2) as dinero_total_movido,
        ROUND(AVG(monto), 2) as monto_promedio
    FROM transactions_clean 
    GROUP BY id_atm, ubicacion
),
daily_stats AS (
    SELECT 
        fecha_dia,
        COUNT(*) as transacciones_dia,
        ROUND(SUM(monto), 2) as dinero_dia
    FROM transactions_clean 
    GROUP BY fecha_dia
)
SELECT 'TOP_CAJEROS' as tipo, 
       CAST(ROW_NUMBER() OVER (ORDER BY dinero_total_movido DESC) as STRING) as ranking,
       id_atm, ubicacion,
       CAST(total_transacciones as STRING) as valor1,
       CAST(dinero_total_movido as STRING) as valor2
FROM cajero_stats 
ORDER BY dinero_total_movido DESC 
LIMIT 5
"""

result = spark.sql(combined_stats_query)
result.show(truncate=False)

# OPTIMIZACIÓN 8: Consultas de calidad en una sola pasada
print("\n🚨 Verificaciones de Calidad (Combinadas):")
quality_check_query = """
SELECT 
    COUNT(*) as total_registros,
    COUNT(id_atm) as id_atm_validos,
    COUNT(monto) as montos_validos,
    MIN(monto) as monto_minimo,
    MAX(monto) as monto_maximo,
    ROUND(AVG(monto), 2) as monto_promedio,
    COUNT(CASE WHEN monto <= 0 THEN 1 END) as montos_invalidos,
    COUNT(DISTINCT id_atm) as cajeros_unicos,
    COUNT(DISTINCT fecha_dia) as dias_unicos
FROM transactions_clean
"""
spark.sql(quality_check_query).show()

validation_time = time.time() - validation_start
total_time = time.time() - start_time

print("\n⚡ === MÉTRICAS DE PERFORMANCE ===")
print(f"🕐 Inicialización: {init_time:.2f}s")
print(f"📁 Carga de datos: {load_time:.2f}s") 
print(f"🔍 Validaciones: {validation_time:.2f}s")
print(f"⏱️  TIEMPO TOTAL: {total_time:.2f}s")

print(f"\n✅ Validaciones completadas desde: {data_source.upper()}")

# OPTIMIZACIÓN 9: Limpiar cache al final
df_clean.unpersist()
if 'df_atms' in locals():
    df_atms.unpersist()

job.commit()
