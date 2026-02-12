import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# IMPORTANTE: Agregamos to_date para extraer solo la fecha del timestamp
from pyspark.sql.functions import col, current_timestamp, to_date, broadcast
from pyspark.sql.types import DecimalType 

# --- PARTE A: BOILERPLATE (Configuración del motor de Glue) ---
print ("--- Iniciando Job Logicash ETL ---")

# 1. SparkContext: Conexión con el cluster.
sc = SparkContext()

# 2. GlueContext: Envoltura de AWS para "superpoderes" de datos.
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 3. Capturar argumentos dinámicos del Job de Glue
# Estos valores se configuran en la consola de AWS Glue al crear/ejecutar el Job
# --JOB_NAME: Nombre del job (requerido por Glue)
# --bucket_raw: Nombre del bucket S3 de datos crudos (Bronze)
# --bucket_processed: Nombre del bucket S3 de datos procesados (Silver)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_raw', 'bucket_processed'])

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extraer argumentos en variables legibles
bucket_raw = args['bucket_raw']
bucket_processed = args['bucket_processed']

print(f"✅ Entorno Spark/Glue inicializado correctamente.")
print(f"📦 Bucket Raw: s3://{bucket_raw}")
print(f"📦 Bucket Processed: s3://{bucket_processed}")

# --- PARTE B: EXTRACT (Lectura de fuentes) ---
print("📁 Leyendo archivos CSV ...")

# Rutas dinámicas construidas desde los argumentos del Job
# Los CSVs se encuentran en la raíz del bucket raw
df_atms_path = f"s3://{bucket_raw}/dim_atms.csv"
df_transactions_path = f"s3://{bucket_raw}/fact_transactions.csv"

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

# --- 3. TRANSFORM (Limpieza y Enriquecimiento) ---
print("🧹 Iniciando Transformación...")

# A. JOIN OPTIMIZADO: Broadcast join si dim_atms es pequeña (<200MB)
# Esto evita shuffling innecesario y mejora performance significativamente
print("🔗 Ejecutando JOIN optimizado...")
df_joined = df_transactions.join(broadcast(df_atms), on="id_atm", how="left")

# B. DATA QUALITY: Filtrado de basura según reglas de negocio
# Reglas: id_atm no nulo, monto > 0, fecha pasada, status EXITOSA
count_raw = df_joined.count()

# --- DESGLOSE DE DESCARTE POR REGLA (Observabilidad) ---
# Conteo individual ANTES de aplicar el filtro combinado
# Permite detectar anomalías en la fuente de datos (ej: pico de montos negativos)
null_atm = df_joined.filter(col("id_atm").isNull()).count()
negative_amount = df_joined.filter(col("monto") <= 0).count()
future_dates = df_joined.filter(col("fecha") > current_timestamp()).count()
non_successful = df_joined.filter(col("status_transaccion") != 'EXITOSA').count()

print(f"\n   🔍 Desglose de Descarte por Regla:")
print(f"      - id_atm nulo:          {null_atm} registros ({null_atm/count_raw*100:.2f}%)")
print(f"      - monto <= 0:           {negative_amount} registros ({negative_amount/count_raw*100:.2f}%)")
print(f"      - fecha futura:         {future_dates} registros ({future_dates/count_raw*100:.2f}%)")
print(f"      - status no exitosa:    {non_successful} registros ({non_successful/count_raw*100:.2f}%)")

# Aplicar filtro combinado (un registro puede fallar en múltiples reglas)
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

# --- CASTEO CRÍTICO PARA PRECISIÓN FINANCIERA ---
# Convertimos 'monto' de Double/Float a DecimalType(18, 2) para evitar errores de precisión
# DecimalType(18, 2) = hasta 18 dígitos totales con 2 decimales de precisión
print("💰 Aplicando casteo de precisión financiera (monto -> DecimalType)...")
df_clean = df_clean.withColumn("monto", col("monto").cast(DecimalType(18, 2)))

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

# Ruta de salida dinámica desde argumentos del Job
output_path = f"s3://{bucket_processed}/fact_transactions"

# Escritura en Parquet particionado por fecha_dia (una carpeta por día, no por segundo)
df_clean.write.mode("overwrite") \
    .partitionBy("fecha_dia") \
    .parquet(output_path)

print(f"✅ Data guardada exitosamente en: {output_path}")

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

# --- COMMIT DEL JOB ---
# Señaliza a Glue que el job terminó exitosamente (requerido para bookmarks y métricas)
job.commit()
print("--- Job Finalizado Exitosamente ---")