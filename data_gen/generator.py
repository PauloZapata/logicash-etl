import pandas as pd
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from decimal import Decimal

def setup_seeds():
    """Configurar semillas para reproducibilidad"""
    random.seed(42)
    Faker.seed(42)

def create_output_directory():
    """
    Crear directorio de salida usando pathlib para compatibilidad multiplataforma.
    Detecta la ubicación del script y crea la carpeta hermana ../data/raw
    """
    # Obtener la ruta del script actual
    current_script_path = Path(__file__).parent
    
    # Navegar a la carpeta padre y luego a data/raw
    output_dir = current_script_path.parent / 'data' / 'raw'
    
    # Crear directorio si no existe
    output_dir.mkdir(parents=True, exist_ok=True)
    
    return output_dir

def generate_lima_coordinates():
    """
    Generar coordenadas aleatorias dentro del bounding box de Lima, Perú.
    
    Returns:
        tuple: (latitud, longitud) dentro de Lima metropolitana
    """
    # Bounding box de Lima, Perú
    # Latitud: -12.20 (sur) a -11.90 (norte)
    # Longitud: -77.15 (oeste) a -76.90 (este)
    
    lat_min, lat_max = -12.20, -11.90
    lon_min, lon_max = -77.15, -76.90
    
    # Generar coordenadas aleatorias con precisión de 6 decimales (~111m de precisión)
    latitud = round(random.uniform(lat_min, lat_max), 6)
    longitud = round(random.uniform(lon_min, lon_max), 6)
    
    return latitud, longitud

def generate_dim_atms(num_records=50):
    """
    Generar tabla de dimensión de ATMs con georreferenciación para Lima, Perú
    
    Args:
        num_records (int): Número de registros a generar (default: 50)
    
    Returns:
        pd.DataFrame: DataFrame con datos de ATMs incluyendo coordenadas y estado
    """
    fake = Faker('es')  # Localización español genérico (es_PE no existe en Faker)
    
    atms_data = []
    
    for i in range(1, num_records + 1):
        # ID secuencial con formato ATM-XXX
        id_atm = f"ATM-{i:03d}"
        
        # Generar dirección realista en Lima
        ubicacion = f"{fake.street_address()}, {fake.city_suffix()} {fake.city()}, Lima"
        
        # Generar coordenadas dentro del bounding box de Lima
        latitud, longitud = generate_lima_coordinates()
        
        # Capacidad máxima aleatoria
        capacidad_maxima = random.choice([100000, 500000, 1000000])
        
        # Modelo aleatorio
        modelo = random.choice(['NCR', 'Diebold', 'Hyosung', 'Wincor Nixdorf'])
        
        # Estado del cajero con probabilidades ponderadas
        estado = random.choices(
            ['OPERATIVO', 'MANTENIMIENTO', 'FUERA_DE_SERVICIO'],
            weights=[92, 5, 3]  # 92% operativo, 5% mantenimiento, 3% fuera de servicio
        )[0]
        
        atms_data.append({
            'id_atm': id_atm,
            'ubicacion': ubicacion,
            'latitud': latitud,
            'longitud': longitud,
            'capacidad_maxima': capacidad_maxima,
            'modelo': modelo,
            'estado': estado
        })
    
    return pd.DataFrame(atms_data)

def generate_fact_transactions(atm_ids, num_records=10000):
    """
    Generar tabla de hechos de transacciones
    
    Args:
        atm_ids (list): Lista de IDs de ATMs disponibles
        num_records (int): Número de registros a generar (default: 10000)
    
    Returns:
        pd.DataFrame: DataFrame con datos de transacciones
    """
    fake = Faker('es_ES')
    
    transactions_data = []
    
    # Fechas de referencia (últimos 24 meses)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # ~24 meses
    
    for _ in range(num_records):
        # ID único de transacción
        id_transaccion = str(uuid.uuid4())
        
        # ID de ATM (normalmente válido, pero con probabilidad de ser nulo)
        if random.random() < 0.01:  # 1% probabilidad de integridad corrupta
            id_atm = None
        else:
            id_atm = random.choice(atm_ids)
        
        # Fecha aleatoria en los últimos 24 meses
        if random.random() < 0.01:  # 1% probabilidad de fecha en el futuro
            # Fecha incorrecta en el futuro
            fecha = fake.date_time_between(
                start_date=datetime(2030, 1, 1),
                end_date=datetime(2035, 12, 31)
            )
        else:
            # Fecha normal en el rango correcto
            fecha = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        # Monto de la transacción - Precisión financiera con Decimal
        # Usa aritmética entera + Decimal para evitar errores de punto flotante IEEE 754
        # Genera un entero entre 1000 y 800000 y lo divide entre 100 → rango: 10.00 a 8000.00
        if random.random() < 0.02:  # 2% probabilidad de monto negativo (error de negocio)
            monto = -(Decimal(random.randint(1000, 800000)) / 100)
        else:
            monto = Decimal(random.randint(1000, 800000)) / 100
        
        # Tipo de movimiento con probabilidades especificadas
        tipo_movimiento = random.choices(
            ['RETIRO', 'DEPOSITO'],
            weights=[80, 20]  # 80% retiros, 20% depósitos
        )[0]
        
        # Status de transacción con probabilidades especificadas
        status_transaccion = random.choices(
            ['EXITOSA', 'FALLIDA', 'REVERSADA'],
            weights=[90, 5, 5]  # 90% exitosas, 5% fallidas, 5% reversadas
        )[0]
        
        transactions_data.append({
            'id_transaccion': id_transaccion,
            'id_atm': id_atm,
            'fecha': fecha,
            'monto': monto,
            'tipo_movimiento': tipo_movimiento,
            'status_transaccion': status_transaccion
        })
    
    return pd.DataFrame(transactions_data)

def save_to_csv(df, filename, output_dir):
    """
    Guardar DataFrame a CSV usando pathlib
    
    Args:
        df (pd.DataFrame): DataFrame a guardar
        filename (str): Nombre del archivo
        output_dir (Path): Directorio de salida (objeto Path)
    """
    filepath = output_dir / filename
    df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"✅ Archivo generado: {filepath}")
    print(f"   📊 Registros: {len(df):,}")
    print(f"   📝 Columnas: {list(df.columns)}")
    print()

def generate_data_quality_report(dim_atms_df, fact_transactions_df):
    """
    Generar reporte de calidad de datos
    
    Args:
        dim_atms_df (pd.DataFrame): DataFrame de ATMs
        fact_transactions_df (pd.DataFrame): DataFrame de transacciones
    """
    print("📋 REPORTE DE CALIDAD DE DATOS")
    print("=" * 50)
    
    # Estadísticas de dim_atms
    print(f"🏧 ATMs generados: {len(dim_atms_df):,}")
    print(f"   Modelos únicos: {dim_atms_df['modelo'].nunique()}")
    print(f"   Distribución de modelos:")
    for modelo, count in dim_atms_df['modelo'].value_counts().items():
        print(f"     - {modelo}: {count} ({count/len(dim_atms_df)*100:.1f}%)")
    
    # Distribución de estados de ATMs
    print(f"   Estado de cajeros:")
    for estado, count in dim_atms_df['estado'].value_counts().items():
        print(f"     - {estado}: {count} ({count/len(dim_atms_df)*100:.1f}%)")
    
    # Estadísticas geoespaciales
    print(f"   📍 Coordenadas (Lima, Perú):")
    print(f"     - Latitud: {dim_atms_df['latitud'].min():.6f} a {dim_atms_df['latitud'].max():.6f}")
    print(f"     - Longitud: {dim_atms_df['longitud'].min():.6f} a {dim_atms_df['longitud'].max():.6f}")
    print()
    
    # Estadísticas de fact_transactions
    print(f"💳 Transacciones generadas: {len(fact_transactions_df):,}")
    
    # Verificar integridad referencial
    null_atm_count = fact_transactions_df['id_atm'].isnull().sum()
    print(f"   🔴 Registros con id_atm nulo: {null_atm_count} ({null_atm_count/len(fact_transactions_df)*100:.2f}%)")
    
    # Verificar montos negativos
    negative_amount_count = (fact_transactions_df['monto'] < 0).sum()
    print(f"   🔴 Transacciones con monto negativo: {negative_amount_count} ({negative_amount_count/len(fact_transactions_df)*100:.2f}%)")
    
    # Verificar fechas futuras
    future_date_count = (fact_transactions_df['fecha'] > datetime.now()).sum()
    print(f"   🔴 Transacciones con fecha futura: {future_date_count} ({future_date_count/len(fact_transactions_df)*100:.2f}%)")
    
    # Distribución de tipos de movimiento
    print(f"   💰 Distribución de tipos de movimiento:")
    for tipo, count in fact_transactions_df['tipo_movimiento'].value_counts().items():
        print(f"     - {tipo}: {count:,} ({count/len(fact_transactions_df)*100:.1f}%)")
    
    # Distribución de status
    print(f"   📊 Distribución de status:")
    for status, count in fact_transactions_df['status_transaccion'].value_counts().items():
        print(f"     - {status}: {count:,} ({count/len(fact_transactions_df)*100:.1f}%)")
    
    # Rangos de montos
    valid_amounts = fact_transactions_df[fact_transactions_df['monto'] > 0]['monto']
    print(f"   💵 Rango de montos válidos:")
    print(f"     - Mínimo: ${valid_amounts.min():.2f}")
    print(f"     - Máximo: ${valid_amounts.max():.2f}")
    print(f"     - Promedio: ${valid_amounts.mean():.2f}")
    print(f"     - Mediana: ${valid_amounts.median():.2f}")

def main():
    """Función principal para generar todos los datos"""
    print("🚀 GENERADOR DE DATOS MOCK - ANALÍTICA GEOESPACIAL ATMs LIMA")
    print("=" * 60)
    print()
    
    # Configurar semillas para reproducibilidad
    setup_seeds()
    print("🎲 Semillas configuradas (random.seed=42, Faker.seed=42)")
    
    # Crear directorio de salida
    output_dir = create_output_directory()
    print(f"📁 Directorio de salida: {output_dir}")
    print()
    
    # Generar dimensión de ATMs con georreferenciación
    print("🏧 Generando dimensión de ATMs (Lima, Perú)...")
    dim_atms_df = generate_dim_atms(num_records=50)
    save_to_csv(dim_atms_df, 'dim_atms.csv', output_dir)
    
    # Obtener lista de IDs de ATMs para referencias
    atm_ids = dim_atms_df['id_atm'].tolist()
    
    # Generar tabla de hechos de transacciones
    print("💳 Generando tabla de transacciones...")
    fact_transactions_df = generate_fact_transactions(atm_ids, num_records=10000)
    save_to_csv(fact_transactions_df, 'fact_transactions.csv', output_dir)
    
    # Generar reporte de calidad
    generate_data_quality_report(dim_atms_df, fact_transactions_df)
    
    print("\n✨ Generación de datos completada exitosamente!")
    print(f"📂 Archivos generados en: {output_dir.resolve()}")
    print("🗺️  Listo para análisis geoespacial con coordenadas de Lima, Perú")

if __name__ == "__main__":
    main()