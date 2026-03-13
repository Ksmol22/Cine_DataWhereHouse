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
# SECCIÓN 2: CONFIGURACIÓN DE CONEXIÓN A SQL SERVER
#
# INSTRUCCIONES PARA COMPLETAR:
#   - SERVER:   Nombre del servidor o instancia de SQL Server.
#               Si es local Community Edition, suele ser: LOCALHOST o NOMBRE_PC\SQLEXPRESS
#               Ejemplo: "DESKTOP-ABC123\SQLEXPRESS" o simplemente "localhost"
#   - DATABASE: Nombre de la base de datos destino (debe existir en SQL Server).
#   - Autenticación: Se usa Windows Authentication por defecto (recomendado
#               para SQL Server Community local). Si usas usuario/contraseña SQL,
#               cambia USAR_WINDOWS_AUTH a False y completa SQL_USER y SQL_PASSWORD.
# =============================================================================

SQL_SERVER_CONFIG = {
    # Nombre del servidor/instancia de SQL Server
    # Ejemplos: "localhost", "MIPC\\SQLEXPRESS", "192.168.1.100"
    "SERVER": "localhost",

    # Nombre de la base de datos donde se cargarán los datos
    "DATABASE": "DataWhereHouse",

    # Driver ODBC instalado en el sistema.
    # Para verificar cuál tienes, ejecuta en PowerShell:
    #   Get-OdbcDriver | Select-Object Name | Where-Object {$_.Name -like "*SQL*"}
    # Opciones comunes: "ODBC Driver 17 for SQL Server" / "ODBC Driver 18 for SQL Server"
    "DRIVER": "ODBC Driver 17 for SQL Server",

    # True = usa Autenticación de Windows (recomendado para instalaciones locales)
    # False = usa usuario y contraseña SQL Server
    "USAR_WINDOWS_AUTH": True,

    # Solo se usan si USAR_WINDOWS_AUTH = False
    "SQL_USER": "",
    "SQL_PASSWORD": "",

    # Tiempo máximo de espera para establecer la conexión (segundos)
    "TIMEOUT_CONEXION": 30,
}


# =============================================================================
# SECCIÓN 3: CONFIGURACIÓN DEL COMPORTAMIENTO DEL LOADER
# =============================================================================

LOADER_CONFIG = {
    # Modo de carga al insertar datos en SQL Server.
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
    "ELIMINAR_FILAS_COMPLETAMENTE_NULAS": True,

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
