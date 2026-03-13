import sys
import time
import traceback
from datetime import datetime

# Importar todos los módulos del ETL
from logger_config import configurar_logger
from extractor import Extractor
from transformer import Transformer
from loader import Loader


def ejecutar_etl() -> int:
    """
    Función principal que orquesta el pipeline ETL completo.

    Returns:
        int: Código de salida del proceso.
               0 = todo OK
               1 = error crítico (no se procesó nada)
               2 = éxito parcial (al menos un archivo falló)
    """

    # =========================================================================
    # INICIALIZACIÓN
    # =========================================================================

    # Crear el logger compartido que usarán todos los módulos
    logger = configurar_logger("ETL_Pipeline")

    # Marcar el inicio del pipeline
    inicio_pipeline = time.time()
    logger.info("=" * 70)
    logger.info("  INICIANDO PIPELINE ETL — DataWhereHouse")
    logger.info(f"  Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    # Contadores para el resumen final
    archivos_procesados = 0
    archivos_fallidos = 0
    total_filas_cargadas = 0

    # =========================================================================
    # SECCIÓN 1: INSTANCIAR LAS CAPAS DEL ETL
    # Cada capa recibe el logger para que todo el registro sea coherente.
    # =========================================================================

    extractor = Extractor(logger)
    transformer = Transformer(logger)

    # =========================================================================
    # SECCIÓN 2: VERIFICAR CONEXIÓN Y EJECUTAR EL PIPELINE
    #
    # Usamos el Loader como context manager (with ...) para garantizar que la
    # conexión a SQL Server se cierre correctamente al finalizar, incluso si
    # ocurre un error inesperado en medio del proceso.
    # =========================================================================

    try:
        with Loader(logger) as loader:

            # =================================================================
            # SECCIÓN 3: CICLO PRINCIPAL DEL ETL
            #
            # El extractor es un GENERADOR: produce un archivo a la vez.
            # Esto significa que:
            #   1. Se lee el archivo A
            #   2. Se transforma el archivo A
            #   3. Se carga el archivo A a SQL Server
            #   4. Se pasa al archivo B (sin tener A en memoria)
            #
            # Este enfoque es eficiente en memoria para datasets grandes.
            # =================================================================

            for nombre_tabla, df_crudo in extractor.extraer_todos():

                logger.info("-" * 60)
                logger.info(f"  PROCESANDO: {nombre_tabla}")
                logger.info("-" * 60)

                try:
                    # ---------------------------------------------------------
                    # PASO E (Extract): ya ocurrió dentro del generador.
                    # df_crudo contiene los datos tal como están en el archivo.
                    # ---------------------------------------------------------

                    # ---------------------------------------------------------
                    # PASO T (Transform): limpiar y normalizar los datos.
                    # df_limpio es el DataFrame listo para SQL Server.
                    # ---------------------------------------------------------
                    df_limpio = transformer.transformar(df_crudo, nombre_tabla)

                    # Validación: si el DataFrame queda vacío tras la
                    # transformación, no tiene sentido cargar 0 filas.
                    if df_limpio.empty:
                        logger.warning(
                            f"El DataFrame '{nombre_tabla}' quedó vacío después de la "
                            f"transformación. Se omite la carga a SQL Server."
                        )
                        archivos_fallidos += 1
                        continue

                    # ---------------------------------------------------------
                    # PASO L (Load): insertar el DataFrame en SQL Server.
                    # El nombre de la tabla en SQL incluirá el prefijo 'stg_'
                    # (configurable en config.py → LOADER_CONFIG["PREFIJO_TABLA"])
                    # ---------------------------------------------------------
                    filas = loader.cargar(df_limpio, nombre_tabla)
                    total_filas_cargadas += filas
                    archivos_procesados += 1

                except Exception as error_archivo:
                    # Error en un archivo específico: se registra y se continúa
                    # con los demás archivos del lote.
                    archivos_fallidos += 1
                    logger.error(
                        f"Fallo al procesar '{nombre_tabla}': {error_archivo}",
                        exc_info=True
                    )

    except ConnectionError as error_conexion:
        # Error crítico: no se pudo conectar a SQL Server.
        # No tiene sentido continuar si no hay base de datos disponible.
        logger.critical(
            f"ERROR CRÍTICO DE CONEXIÓN: No se pudo establecer conexión con SQL Server.\n"
            f"Detalle: {error_conexion}\n\n"
            f"SOLUCIÓN: Verifica en config.py:\n"
            f"  - SQL_SERVER_CONFIG['SERVER'] → nombre del servidor\n"
            f"  - SQL_SERVER_CONFIG['DATABASE'] → base de datos destino\n"
            f"  - SQL_SERVER_CONFIG['DRIVER'] → driver ODBC instalado\n"
            f"  - SQL_SERVER_CONFIG['USAR_WINDOWS_AUTH'] → tipo de autenticación"
        )
        _imprimir_resumen(logger, inicio_pipeline, archivos_procesados, archivos_fallidos, total_filas_cargadas)
        return 1

    except Exception as error_critico:
        # Error inesperado no clasificado
        logger.critical(
            f"ERROR CRÍTICO INESPERADO: {error_critico}\n"
            f"{traceback.format_exc()}"
        )
        _imprimir_resumen(logger, inicio_pipeline, archivos_procesados, archivos_fallidos, total_filas_cargadas)
        return 1

    # =========================================================================
    # SECCIÓN 4: RESUMEN FINAL
    # =========================================================================

    _imprimir_resumen(logger, inicio_pipeline, archivos_procesados, archivos_fallidos, total_filas_cargadas)

    # Determinar el código de salida según el resultado
    if archivos_procesados == 0 and archivos_fallidos == 0:
        logger.warning("No se encontraron archivos para procesar en Datasets/.")
        return 0
    elif archivos_fallidos > 0 and archivos_procesados == 0:
        return 1   # Todo falló
    elif archivos_fallidos > 0:
        return 2   # Parcialmente exitoso
    else:
        return 0   # Todo OK


def _imprimir_resumen(
    logger,
    inicio: float,
    procesados: int,
    fallidos: int,
    filas_totales: int
) -> None:
    """
    Imprime el resumen final del pipeline en el log.
    Se llama siempre al terminar, ya sea con éxito o con error.

    Args:
        logger: Logger del pipeline.
        inicio (float): Timestamp de inicio (time.time()).
        procesados (int): Archivos cargados exitosamente.
        fallidos (int): Archivos que fallaron.
        filas_totales (int): Total de filas cargadas en SQL Server.
    """
    duracion = time.time() - inicio
    minutos = int(duracion // 60)
    segundos = duracion % 60

    logger.info("=" * 70)
    logger.info("  RESUMEN FINAL — PIPELINE ETL")
    logger.info("=" * 70)
    logger.info(f"  Archivos procesados exitosamente : {procesados}")
    logger.info(f"  Archivos con errores             : {fallidos}")
    logger.info(f"  Total filas cargadas en SQL Server: {filas_totales:,}")
    logger.info(f"  Duración total                   : {minutos}m {segundos:.2f}s")
    logger.info("=" * 70)

    if fallidos > 0:
        logger.warning(
            f"  ATENCIÓN: {fallidos} archivo(s) fallaron. "
            f"Revisa el archivo de log en ETL/logs/ para ver los detalles."
        )


# =============================================================================
# PUNTO DE ENTRADA
# Se ejecuta solo cuando el script se llama directamente con:
#     python etl_pipeline.py
# No se ejecuta si este módulo es importado desde otro script.
# =============================================================================

if __name__ == "__main__":
    codigo_salida = ejecutar_etl()
    sys.exit(codigo_salida)
