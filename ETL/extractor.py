
import os
import shutil
import logging
import pandas as pd
from typing import Generator, Tuple

from config import DIRECTORIO_DATASETS, DIRECTORIO_PROCESADOS, EXTENSIONES_SOPORTADAS


class Extractor:
    """
    Clase responsable de la fase de EXTRACCIÓN del ETL.

    Uso típico:
        extractor = Extractor(logger)
        for nombre_tabla, dataframe in extractor.extraer_todos():
            # procesar cada dataframe...
    """

    def __init__(self, logger: logging.Logger):
        """
        Inicializa el extractor con el logger compartido del pipeline.

        Args:
            logger: Logger configurado desde logger_config.py
        """
        self.logger = logger

        # Crear la carpeta de archivos procesados si no existe.
        # exist_ok=True evita error si la carpeta ya fue creada antes.
        os.makedirs(DIRECTORIO_PROCESADOS, exist_ok=True)
        self.logger.debug(f"Directorio de datasets: {DIRECTORIO_DATASETS}")
        self.logger.debug(f"Directorio de procesados: {DIRECTORIO_PROCESADOS}")

    # -------------------------------------------------------------------------
    # MÉTODO PRINCIPAL: detectar y recorrer todos los archivos soportados
    # -------------------------------------------------------------------------

    def extraer_todos(self) -> Generator[Tuple[str, pd.DataFrame, str, str], None, None]:
        """
        Generador que itera sobre todos los archivos válidos en Datasets/,
        los lee como DataFrames y los va devolviendo uno a uno.

        Usar un generador (yield) en lugar de una lista es más eficiente en
        memoria: solo carga un archivo a la vez en lugar de todos a la vez.

        Yields:
            Tuple[str, pd.DataFrame, str, str]:
                - str: nombre de la tabla destino (nombre del archivo sin extensión)
                - pd.DataFrame: datos del archivo listos para transformar
                - str: ruta absoluta del archivo de origen
                - str: nombre del archivo de origen

        Example:
            for tabla, df in extractor.extraer_todos():
                print(tabla, df.shape)
        """
        # -----------------------------------------------------------------
        # PASO 1: Listar todos los archivos en la carpeta Datasets/
        # Se filtran solo los que tienen extensiones soportadas.
        # Los archivos dentro de la subcarpeta "procesados/" se ignoran.
        # -----------------------------------------------------------------
        archivos_encontrados = self._listar_archivos()

        if not archivos_encontrados:
            self.logger.warning(
                f"No se encontraron archivos para procesar en: {DIRECTORIO_DATASETS}\n"
                f"Extensiones soportadas: {EXTENSIONES_SOPORTADAS}"
            )
            return  # Salir del generador sin producir ningún valor

        self.logger.info(f"Archivos a procesar: {len(archivos_encontrados)}")

        # -----------------------------------------------------------------
        # PASO 2: Iterar sobre cada archivo detectado
        # -----------------------------------------------------------------
        for ruta_archivo in archivos_encontrados:
            nombre_archivo = os.path.basename(ruta_archivo)
            extension = os.path.splitext(nombre_archivo)[1].lower()

            # El nombre de la tabla SQL será el nombre del archivo sin extensión
            # Ejemplo: "ventas_2026.csv" → tabla "stg_ventas_2026"
            nombre_tabla = os.path.splitext(nombre_archivo)[0]

            self.logger.info(f"Extrayendo: {nombre_archivo}")

            try:
                # ---------------------------------------------------------
                # PASO 3: Leer el archivo según su tipo/extensión
                # ---------------------------------------------------------
                df = self._leer_archivo(ruta_archivo, extension)

                self.logger.info(
                    f"  Extraído correctamente | Filas: {len(df):,} | "
                    f"Columnas: {df.shape[1]} | Tabla destino: '{nombre_tabla}'"
                )
                self.logger.debug(f"  Columnas detectadas: {list(df.columns)}")

                # ---------------------------------------------------------
                # PASO 4: Ceder el DataFrame al caller (orquestador)
                # ---------------------------------------------------------
                yield nombre_tabla, df, ruta_archivo, nombre_archivo

            except Exception as error:
                # Si un archivo falla, se registra el error y se continúa
                # con los demás archivos (no se detiene todo el pipeline).
                self.logger.error(
                    f"Error al extraer '{nombre_archivo}': {error}",
                    exc_info=True  # incluye el traceback completo en el log
                )

    # -------------------------------------------------------------------------
    # MÉTODOS PRIVADOS (uso interno de la clase)
    # -------------------------------------------------------------------------

    def _listar_archivos(self) -> list:
        """
        Busca en DIRECTORIO_DATASETS todos los archivos con extensiones
        soportadas. No entra en subdirectorios (ignora 'procesados/').

        Returns:
            list: Lista de rutas absolutas de archivos a procesar.
        """
        archivos = []

        for nombre in os.listdir(DIRECTORIO_DATASETS):
            ruta = os.path.join(DIRECTORIO_DATASETS, nombre)

            # Solo procesar archivos (no carpetas)
            if not os.path.isfile(ruta):
                continue

            extension = os.path.splitext(nombre)[1].lower()

            if extension in EXTENSIONES_SOPORTADAS:
                archivos.append(ruta)
                self.logger.debug(f"  Detectado: {nombre}")
            else:
                self.logger.debug(f"  Ignorado (extensión no soportada): {nombre}")

        return archivos

    def _leer_archivo(self, ruta: str, extension: str) -> pd.DataFrame:
        """
        Llama al método de lectura correcto según la extensión del archivo.

        Args:
            ruta (str): Ruta completa al archivo.
            extension (str): Extensión en minúsculas, ej. ".csv"

        Returns:
            pd.DataFrame: Contenido del archivo como DataFrame.

        Raises:
            ValueError: Si la extensión no tiene un método de lectura asignado.
        """
        lectores = {
            ".csv":  self._leer_csv,
            ".xlsx": self._leer_excel,
            ".xls":  self._leer_excel,
            ".json": self._leer_json,
        }

        if extension not in lectores:
            raise ValueError(f"Extensión '{extension}' no tiene lector configurado.")

        return lectores[extension](ruta)

    def _leer_csv(self, ruta: str) -> pd.DataFrame:
        """
        Lee un archivo CSV.
        Usa el engine 'python' y sep=None para detectar automáticamente
        el delimitador (coma, punto y coma, tabulación, etc.).
        """
        df = pd.read_csv(
            ruta,
            sep=None,           # Detección automática del separador
            engine="python",    # Necesario para sep=None
            encoding="utf-8-sig",  # utf-8-sig maneja el BOM de archivos Windows
            on_bad_lines="warn"    # En lugar de fallar, advierte sobre líneas malformadas
        )
        return df

    def _leer_excel(self, ruta: str) -> pd.DataFrame:
        """
        Lee un archivo Excel (.xlsx o .xls).
        Por defecto lee la primera hoja (sheet_name=0).
        Si necesitas múltiples hojas, se puede extender aquí.
        """
        df = pd.read_excel(
            ruta,
            sheet_name=0,    # Primera hoja
            engine=None      # Pandas elige automáticamente openpyxl o xlrd
        )
        return df

    def _leer_json(self, ruta: str) -> pd.DataFrame:
        """
        Lee un archivo JSON.
        Soporta dos orientaciones comunes:
          - Lista de objetos: [{"col": val}, {"col": val}]  → orient="records"
          - Un objeto por línea (JSON Lines / NDJSON)        → lines=True
        Intenta orient="records" primero; si falla, intenta lines=True.
        """
        try:
            df = pd.read_json(ruta, orient="records", encoding="utf-8")
        except Exception:
            self.logger.debug("JSON: Reintentando con formato JSON Lines (una línea por registro).")
            df = pd.read_json(ruta, lines=True, encoding="utf-8")
        return df

    def _mover_a_procesados(self, ruta_origen: str, nombre_archivo: str) -> None:
        """
        Mueve el archivo ya procesado a la carpeta 'procesados/'.
        Si ya existe un archivo con el mismo nombre en destino, se sobreescribe.

        Args:
            ruta_origen (str): Ruta actual del archivo.
            nombre_archivo (str): Nombre del archivo (sin ruta).
        """
        ruta_destino = os.path.join(DIRECTORIO_PROCESADOS, nombre_archivo)
        shutil.move(ruta_origen, ruta_destino)
        self.logger.debug(f"  Movido a procesados/: {nombre_archivo}")

    def marcar_como_procesado(self, ruta_origen: str, nombre_archivo: str) -> None:
        """
        Marca un archivo como procesado moviéndolo a 'procesados/'.
        Debe llamarse solo después de completar transformación y carga con éxito.
        """
        self._mover_a_procesados(ruta_origen, nombre_archivo)
