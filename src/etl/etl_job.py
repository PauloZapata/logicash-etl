import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# IMPORTANTE: Agregamos to_date para extraer solo la fecha del timestamp
from pyspark.sql.functions import col, current_timestamp, to_date, broadcast 

# --- PARTE A: BOILERPLATE (Configuración del motor de Glue) ---
print ("--- Iniciando Job Logicash ETL ---")

# 1. SparkContext: Conexión con el cluster.
sc = SparkContext()

# 2. GlueContext: Envoltura de AWS para "superpoderes" de datos.
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("logicash_etl_job", {})

print("✅ Entorno Spark/Glue inicializado correctamente.")

# --- PARTE B: EXTRACT (Lectura de fuentes) ---
print("📁 Leyendo archivos CSV ...")

try:
    # Definimos las rutas para mejor manejo de errores
    df_atms_path = "data/raw/dim_atms.csv"
    df_transactions_path = "data/raw/fact_transactions.csv"
    
    # Leemos Dimension ATMs
    df_atms = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .load(df_atms_path)
    
    # Leemos Fact Transactions
    df_transactions = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .load(df_transactions_path)
    
    print(f" -> ATMs cargados: {df_atms.count()} registros")
    print(f" -> Transactions cargados: {df_transactions.count()} registros")

except Exception as e:
    print(f"❌ Error leyendo archivos CSV: {str(e)}")
    print(f"📁 Verificar que existan: {df_atms_path}, {df_transactions_path}")
    # Log para CloudWatch en producción
    sys.exit(1)

# --- 3. TRANSFORM (Limpieza y Enriquecimiento) ---
print("🧹 Iniciando Transformación...")

# A. JOIN OPTIMIZADO: Broadcast join si dim_atms es pequeña (<200MB)
# Esto evita shuffling innecesario y mejora performance significativamente
print("🔗 Ejecutando JOIN optimizado...")
df_joined = df_transactions.join(broadcast(df_atms), on="id_atm", how="left")

# B. DATA QUALITY: Filtrado de basura según reglas de negocio
# Reglas: id_atm no nulo, monto > 0, fecha pasada, status EXITOSA
count_raw = df_joined.count()

df_clean = df_joined.filter(
    (col("id_atm").isNotNull()) & 
    (col("monto") > 0) & 
    (col("fecha") <= current_timestamp()) &
    (col("status_transaccion") == 'EXITOSA')
)

# --- NUEVO PASO DE OPTIMIZACIÓN ---
# Creamos una columna derivada solo con la fecha (YYYY-MM-DD) para particionar correctamente
# Esto evita el problema de "Small Files" y "High Cardinality"
print("📅 Generando columna de particionado (fecha_dia)...")
df_clean = df_clean.withColumn("fecha_dia", to_date(col("fecha")))

count_clean = df_clean.count()
discarded = count_raw - count_clean

print(f"   📊 Estadísticas de Calidad:")
print(f"      - Entrada: {count_raw} registros")
print(f"      - Salida (Limpia): {count_clean} registros")
print(f"      - Descartados: {discarded} registros")

# Verificación Visual
print("\n--- Muestra de Datos Limpios ---")
df_clean.select("id_transaccion", "fecha_dia", "monto", "ubicacion").show(5, truncate=False)

# --- 4. LOAD (Escribir en Formato Big Data) ---
print("\n💾 Guardando datos en formato Parquet...")

# Ruta de salida
output_path = "data/processed/fact_transactions"

try:
    # AQUI ESTÁ LA CORRECCIÓN CLAVE:
    # Usamos .partitionBy("fecha_dia") en lugar de "fecha".
    # Esto creará una carpeta por día, no por segundo.
    df_clean.write.mode("overwrite") \
        .partitionBy("fecha_dia") \
        .parquet(output_path)
    
    print(f"✅ Data guardada exitosamente en: {output_path}")

except Exception as e:
    print(f"❌ Error guardando data: {str(e)}")
    print(f"📁 Verificar permisos de escritura en: {output_path}")
    print(f"💡 Tip: En S3 verificar que el bucket exista y tenga permisos correctos")
    sys.exit(1)

print("--- Job Finalizado Exitosamente ---")

# --- POST-ETL VALIDACIONES AUTOMÁTICAS ---
print("\n🔍 === VALIDACIONES POST-PROCESAMIENTO ===")
print("Ejecutando validaciones automáticas sobre datos procesados...")

# Registramos df_clean como vista temporal para usar SQL
df_clean.createOrReplaceTempView("transactions_clean")

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

print("\n📅 Total de Dinero por Día (Validación de Particionado):")
print("=" * 55)
daily_summary_query = """
    SELECT 
        fecha_dia,
        COUNT(*) as total_transacciones,
        ROUND(SUM(monto), 2) as dinero_total_dia,
        ROUND(MIN(monto), 2) as monto_minimo,
        ROUND(MAX(monto), 2) as monto_maximo,
        ROUND(AVG(monto), 2) as monto_promedio
    FROM transactions_clean 
    GROUP BY fecha_dia
    ORDER BY fecha_dia DESC
"""
spark.sql(daily_summary_query).show(truncate=False)

print("\n✅ Validaciones automáticas completadas.")

print("--- Job Finalizado Exitosamente ---")