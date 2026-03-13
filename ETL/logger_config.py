
import logging
import os
from datetime import datetime

def configurar_logger(nombre: str = "ETL_Pipeline") -> logging.Logger:
    """
    Crea y devuelve un logger configurado con dos salidas:
      1. Consola (stdout) — para ver el progreso en tiempo real
      2. Archivo de log — para auditoría y revisión posterior

    Args:
        nombre (str): Nombre identificador del logger (aparece en cada línea del log).

    Returns:
        logging.Logger: Instancia del logger lista para usar.
    """

    # -------------------------------------------------------------------------
    # PASO 1: Definir el directorio donde se guardarán los archivos de log.
    # Se crea automáticamente si no existe.
    # -------------------------------------------------------------------------
    directorio_logs = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(directorio_logs, exist_ok=True)

    # -------------------------------------------------------------------------
    # PASO 2: Generar el nombre del archivo de log con fecha y hora de ejecución.
    # Ejemplo: ETL_2026-03-12_14-30-00.log
    # Esto permite tener un historial separado por cada ejecución del ETL.
    # -------------------------------------------------------------------------
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    ruta_archivo_log = os.path.join(directorio_logs, f"ETL_{timestamp}.log")

    # -------------------------------------------------------------------------
    # PASO 3: Crear el logger con el nombre recibido.
    # El nivel DEBUG captura TODOS los mensajes (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    # -------------------------------------------------------------------------
    logger = logging.getLogger(nombre)
    logger.setLevel(logging.DEBUG)

    # Evitar duplicar handlers si la función se llama varias veces
    if logger.hasHandlers():
        logger.handlers.clear()

    # -------------------------------------------------------------------------
    # PASO 4: Definir el formato estándar de los mensajes de log.
    # Ejemplo de salida:
    #   [2026-03-12 14:30:05] [INFO] [ETL_Pipeline] Iniciando proceso ETL...
    # -------------------------------------------------------------------------
    formato = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # -------------------------------------------------------------------------
    # PASO 5: Handler de CONSOLA — muestra mensajes de nivel INFO o superior.
    # (No muestra DEBUG para no saturar la consola con detalles técnicos.)
    # -------------------------------------------------------------------------
    handler_consola = logging.StreamHandler()
    handler_consola.setLevel(logging.INFO)
    handler_consola.setFormatter(formato)

    # -------------------------------------------------------------------------
    # PASO 6: Handler de ARCHIVO — guarda TODOS los mensajes (nivel DEBUG).
    # El archivo queda como registro permanente de la ejecución.
    # -------------------------------------------------------------------------
    handler_archivo = logging.FileHandler(ruta_archivo_log, encoding="utf-8")
    handler_archivo.setLevel(logging.DEBUG)
    handler_archivo.setFormatter(formato)

    # -------------------------------------------------------------------------
    # PASO 7: Registrar ambos handlers en el logger.
    # -------------------------------------------------------------------------
    logger.addHandler(handler_consola)
    logger.addHandler(handler_archivo)

    logger.info(f"Logger inicializado. Archivo de log: {ruta_archivo_log}")
    return logger
