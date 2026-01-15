import pandas as pd
from faker import Faker
import random
import os
import uuid
from datetime import datetime, timedelta

def setup_seeds():
    """Configurar semillas para reproducibilidad"""
    random.seed(42)
    Faker.seed(42)

def create_output_directory():
    """Crear directorio de salida si no existe"""
    output_dir = os.path.join('data', 'raw')
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def generate_dim_atms(num_records=50):
    """
    Generar tabla de dimensión de ATMs
    
    Args:
        num_records (int): Número de registros a generar (default: 50)
    
    Returns:
        pd.DataFrame: DataFrame con datos de ATMs
    """
    fake = Faker('es_ES')  # Usar localización española
    
    atms_data = []
    
    for i in range(1, num_records + 1):
        # ID secuencial con formato ATM-XXX
        id_atm = f"ATM-{i:03d}"
        
        # Generar dirección realista
        ubicacion = f"{fake.street_address()}, {fake.city()}, {fake.state()}"
        
        # Capacidad máxima aleatoria
        capacidad_maxima = random.choice([100000, 500000, 1000000])
        
        # Modelo aleatorio
        modelo = random.choice(['NCR', 'Diebold', 'Hyosung'])
        
        atms_data.append({
            'id_atm': id_atm,
            'ubicacion': ubicacion,
            'capacidad_maxima': capacidad_maxima,
            'modelo': modelo
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
        
        # Monto de la transacción
        if random.random() < 0.02:  # 2% probabilidad de monto negativo (error de negocio)
            monto = round(random.uniform(-8000.00, -10.00), 2)
        else:
            monto = round(random.uniform(10.00, 8000.00), 2)
        
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
    Guardar DataFrame a CSV
    
    Args:
        df (pd.DataFrame): DataFrame a guardar
        filename (str): Nombre del archivo
        output_dir (str): Directorio de salida
    """
    filepath = os.path.join(output_dir, filename)
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
    print("🚀 GENERADOR DE DATOS MOCK - PROYECTO ETL BANCARIO")
    print("=" * 55)
    print()
    
    # Configurar semillas para reproducibilidad
    setup_seeds()
    print("🎲 Semillas configuradas (random.seed=42, Faker.seed=42)")
    
    # Crear directorio de salida
    output_dir = create_output_directory()
    print(f"📁 Directorio de salida: {output_dir}")
    print()
    
    # Generar dimensión de ATMs
    print("🏧 Generando dimensión de ATMs...")
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
    print("📂 Archivos generados en:", os.path.abspath(output_dir))

if __name__ == "__main__":
    main()