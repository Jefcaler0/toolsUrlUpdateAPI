# toolsUrlUpdateAPI

Herramienta para actualizar imágenes almacenadas en Azure Storage, desarrollada en Python con procesamiento asíncrono mediante hilos.

## Descripción

Este script automatiza el proceso de descarga y carga de imágenes de productos desde una base de datos SQL Server hacia Azure Storage. Utiliza **procesamiento asíncrono con aiohttp** para manejar múltiples descargas y cargas de imágenes de forma concurrente, mejorando significativamente el rendimiento.

### Características principales

- **Procesamiento asíncrono**: Utiliza `asyncio` y `aiohttp` para procesar múltiples imágenes simultáneamente
- **Reintentos automáticos**: Implementa lógica de reintentos con backoff exponencial (hasta 3 intentos)
- **Logging detallado**: Registro de todas las operaciones en archivo y consola
- **Gestión de errores**: Manejo robusto de errores en descargas y cargas
- **Conexión a SQL Server**: Obtiene información de productos e imágenes desde base de datos
- **Generación de reportes**: Exporta resultados del proceso en formato Excel

### Arquitectura

El script está organizado en clases modulares:

- **ConfigLoader**: Carga configuraciones desde variables de entorno
- **DatabaseConnection**: Maneja conexiones y consultas a SQL Server
- **FileUploader**: Gestiona descargas y cargas asíncronas de imágenes
- **ProcessManager**: Coordina el flujo completo del proceso

## Configuración del Entorno Virtual

### 1. Crear entorno virtual

**Windows:**
```bash
python -m venv venv
```

**Linux/Mac:**
```bash
python3 -m venv venv
```

### 2. Activar entorno virtual

**Windows (PowerShell):**
```bash
.\venv\Scripts\Activate.ps1
```

**Windows (CMD):**
```bash
.\venv\Scripts\activate.bat
```

**Linux/Mac:**
```bash
source venv/bin/activate
```

### 3. Instalar dependencias

**Opción 1: Usando requirements.txt (recomendado)**
```bash
pip install -r requirements.txt
```

**Opción 2: Instalación manual**
```bash
pip install aiohttp pandas pyodbc python-dotenv requests openpyxl
```

## Configuración

### Archivo .env

Crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

```env
# Base de datos SQL Server
DB_SERVER=tu_servidor.database.windows.net
DB_DATABASE=tu_base_de_datos
DB_USERNAME=tu_usuario
DB_PASSWORD=tu_contraseña

# API de Azure Storage
UPLOAD_URL=https://tu-api-azure.com/upload
API_KEY=tu_api_key

# Configuración adicional
REQUEST_DELAY=1.0
IMAGE_DOWNLOAD_PATH=./images
```

### Requisitos del sistema

- Python 3.7 o superior
- Driver ODBC para SQL Server
- Conexión a Internet para descargas y cargas

## Uso

### Ejecutar el script

```bash
python storageImageUpdater.py
```

### Salidas generadas

- **process.log**: Archivo de log con detalles de todas las operaciones
- **output.xlsx**: Reporte Excel con resultados del procesamiento
- **images/**: Directorio con imágenes descargadas (nomenclatura: `ProductId_MediaId_nombreoriginal`)

## Funcionamiento

1. **Conexión a BD**: Se conecta a SQL Server y obtiene registros de productos e imágenes
2. **Descarga asíncrona**: Descarga imágenes desde URLs de forma concurrente
3. **Carga a Azure**: Sube las imágenes a Azure Storage mediante API REST
4. **Registro**: Documenta cada operación (éxito/error) en logs
5. **Reporte**: Genera archivo Excel con resultados finales

## Procesamiento con Hilos

El script utiliza **asyncio** para crear tareas asíncronas que se ejecutan concurrentemente. Esto permite:

- Procesar múltiples imágenes al mismo tiempo
- Reducir tiempo total de ejecución significativamente
- Mejor utilización de recursos de red
- Manejo eficiente de operaciones I/O

La clase `FileUploader` crea tareas asíncronas que se ejecutan mediante `asyncio.gather()`, permitiendo procesar todas las imágenes en paralelo.

## Notas importantes

- El script procesa los primeros 100 registros por defecto (modificar `TOP 100` en la consulta SQL si se requiere más)
- Las imágenes se almacenan localmente antes de subirse a Azure
- Se recomienda monitorear el uso de memoria con grandes volúmenes de imágenes
- El timeout por petición es de 10 segundos

## Licencia

Ver archivo LICENSE para más detalles. 
