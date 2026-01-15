# 🚛 LogiCash: Optimización Logística de Efectivo

**Enfoque:** Data Engineering Batch, Hybrid Cloud (AWS & Local) & FinOps.

## 📋 Caso de Negocio
La "Financiera LogiCash" busca optimizar el reabastecimiento de sus cajeros automáticos (ATMs). Actualmente sufren de:
1.  **Sobrecostos:** Camiones visitando oficinas que no necesitan dinero.
2.  **Cash-out:** Oficinas quedándose sin efectivo en días críticos.

Este proyecto migra la inteligencia logística a una arquitectura de Big Data en AWS.

## 🏗 Arquitectura Técnica
* **Lenguaje:** Python (PySpark).
* **Infraestructura Local:** Docker (simulando AWS Glue).
* **Infraestructura Nube:** AWS Glue, S3, Redshift Serverless.
* **Orquestación:** Event-driven (S3 Events).

## 🚀 Quick Start (Entorno Local)

### 1. Generación de Data (Mock)
Genera datos de prueba con errores intencionales (data sucia) para probar el pipeline.
```bash
python src/data_gen/data_generator.py
```

### 2. Levantar Entorno Docker (AWS Glue Local)
Este proyecto usa la imagen oficial de AWS Glue para desarrollo local costo cero.

**Paso 1: Descargar Imagen**
```bash
docker pull amazon/aws-glue-libs:glue_libs_4.0.0_image_01
```

**Paso 2: Iniciar Contenedor**

En Windows (PowerShell):
```powershell
docker run -it -v ${PWD}:/home/glue_user/workspace/ -p 8888:8888 -p 4040:4040 --name logicash_glue amazon/aws-glue-libs:glue_libs_4.0.0_image_01
```

En Mac/Linux:
```bash
docker run -it -v $(pwd):/home/glue_user/workspace/ -p 8888:8888 -p 4040:4040 --name logicash_glue amazon/aws-glue-libs:glue_libs_4.0.0_image_01
```

**Paso 3: Comandos Útiles**
```bash
# Volver a iniciar (si reinicias la PC)
docker start -ai logicash_glue

# Abrir Spark Shell (dentro del contenedor)
pyspark
```

---

### 3. Retomando: Tu "Hola Mundo" en Spark ⚡

Una vez que guardes ese `README.md` (y si quieres haz un commit/push para asegurar), volvamos a la terminal negra donde eres el usuario `glue_user`.

**Tu Misión:**
1.  Asegúrate de estar dentro del contenedor (el prompt debe decir `bash-4.2$` o similar, no tu ruta de Windows).
2.  Escribe el comando:
    ```bash
    pyspark
    ```
3.  Espera unos segundos. Verás muchos mensajes de carga (INFO).
4.  Si todo sale bien, verás un arte ASCII gigante que dice **Spark**.

**Cuando veas ese logo de Spark, cópialo y pégamelo aquí (o confírmame).** ¡Ese es el momento en que oficialmente tienes un cluster de Big Data corriendo en tu laptop!
