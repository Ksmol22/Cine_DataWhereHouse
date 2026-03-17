import os

# =============================================================================
# SECCIÓN 1: RUTAS DEL PROYECTO
# =============================================================================

# Directorio raíz del proyecto (un nivel arriba de la carpeta ETL/)
DIRECTORIO_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Carpeta donde el usuario deposita los archivos a procesar (CSV, Excel, JSON)
DIRECTORIO_DATASETS = os.path.join(DIRECTORIO_RAIZ, "Datasets")

# Subcarpeta dentro de Datasets donde se mueven los archivos ya procesados
# Esto evita procesar el mismo archivo dos veces en ejecuciones futuras.
DIRECTORIO_PROCESADOS = os.path.join(DIRECTORIO_DATASETS, "procesados")

# Carpeta donde se guardan los archivos de log (se crea automáticamente)
DIRECTORIO_LOGS = os.path.join(DIRECTORIO_RAIZ, "ETL", "logs")

# Extensiones de archivo que el extractor reconoce y puede procesar
EXTENSIONES_SOPORTADAS = [".csv", ".xlsx", ".xls", ".json"]


# =============================================================================
# SECCIÓN 2: CONFIGURACIÓN DE CONEXIÓN A MYSQL
# =============================================================================

MYSQL_CONFIG = {
    # IP o nombre del host donde corre MySQL
    "HOST": "127.0.0.1",

    # Puerto de MySQL (por defecto 3306)
    "PORT": 3306,

    # Nombre de la base de datos destino
    "DATABASE": "cine",

    # Usuario de MySQL
    "USER": "root",

    # Contraseña de MySQL
    "PASSWORD": "123456asdfg**",

    # Tiempo máximo de espera para establecer la conexión (segundos)
    "TIMEOUT_CONEXION": 30,
}


# =============================================================================
# SECCIÓN 3: CONFIGURACIÓN DEL COMPORTAMIENTO DEL LOADER
# =============================================================================

LOADER_CONFIG = {
    # Modo de carga al insertar datos en MySQL.
    # Opciones:
    #   "append"  → Agrega filas a la tabla existente (sin borrar datos previos)
    #   "replace" → Borra la tabla y la recrea con los nuevos datos
    #   "fail"    → Lanza error si la tabla ya existe (útil para proteger datos)
    "IF_EXISTS": "append",

    # Tamaño del lote (batch) de filas que se insertan por cada transacción.
    # Un valor entre 500 y 1000 es un buen equilibrio entre velocidad y memoria.
    "CHUNKSIZE": 500,

    # Si True, la primera columna del DataFrame se usa como índice de la tabla SQL.
    # Generalmente se deja en False para que SQL Server maneje su propio índice.
    "INDEX": False,

    # Prefijo que se agrega al nombre de la tabla en SQL Server.
    # Ejemplo: con prefijo "stg_", el archivo ventas.csv se carga en la tabla "stg_ventas"
    # Dejar vacío "" si no se quiere prefijo.
    "PREFIJO_TABLA": "stg_",
}


# =============================================================================
# SECCIÓN 4: CONFIGURACIÓN DE TRANSFORMACIÓN
# =============================================================================

TRANSFORM_CONFIG = {
    # Si True, se eliminan filas donde TODOS los valores son nulos.
        "ELIMINAR_FILAS_COMPLETAMENTE_NULAS": False,

        # Si True, se eliminan filas duplicadas exactas.
        # Para no descartar información histórica se recomienda False.
        "ELIMINAR_FILAS_DUPLICADAS": False,
    # Si True, se eliminan columnas duplicadas (mismo nombre de columna).
    "ELIMINAR_COLUMNAS_DUPLICADAS": True,

    # Si True, los nombres de columna se normalizan:
    #   - Se pasan a minúsculas
    #   - Se reemplazan espacios y caracteres especiales por guión bajo (_)
    #   Ejemplo: "Nombre Cliente" → "nombre_cliente"
    "NORMALIZAR_NOMBRES_COLUMNAS": True,

    # Si True, se registra en el log un reporte de valores nulos por columna.
    "REPORTAR_NULOS": True,

    # Si True, se registra en el log estadísticas descriptivas básicas del dataset.
    "REPORTAR_ESTADISTICAS": False,
}
