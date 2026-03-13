"""
=============================================================================
MÓDULO: transformer.py
CAPA:   T — Transform (Transformación)
PROPÓSITO: Limpiar, normalizar y enriquecer los DataFrames extraídos antes
           de cargarlos en SQL Server.

RESPONSABILIDADES:
  - Normalizar nombres de columnas (sin espacios, sin acentos, en minúsculas)
  - Eliminar filas y columnas completamente vacías
  - Detectar y reportar valores nulos por columna
  - Eliminar filas duplicadas
  - Inferir y corregir tipos de dato (fechas, números, texto)
  - Agregar columnas de auditoría (fecha de carga, nombre del archivo fuente)
  - Limpiar strings (espacios extra, caracteres de control)

POR QUÉ TRANSFORMAR ANTES DE CARGAR:
  SQL Server es estricto con los tipos de datos. Si intentamos insertar
  strings donde espera fechas, o valores nulos donde hay NOT NULL,
  el insert fallará. La capa de transformación garantiza que los datos
  sean compatibles con el esquema destino.
=============================================================================
"""

import re
import logging
import unicodedata
import pandas as pd
from datetime import datetime

from config import TRANSFORM_CONFIG


class Transformer:
    """
    Clase responsable de la fase de TRANSFORMACIÓN del ETL.

    Aplica todas las reglas de limpieza y normalización configuradas
    en config.py → TRANSFORM_CONFIG.

    Uso típico:
        transformer = Transformer(logger)
        df_limpio = transformer.transformar(df_crudo, "ventas_2026")
    """

    def __init__(self, logger: logging.Logger):
        """
        Args:
            logger: Logger compartido del pipeline.
        """
        self.logger = logger

    # =========================================================================
    # MÉTODO PRINCIPAL
    # =========================================================================

    def transformar(self, df: pd.DataFrame, nombre_tabla: str) -> pd.DataFrame:
        """
        Orquesta todas las transformaciones sobre el DataFrame recibido.
        Cada transformación se aplica en secuencia y se registra en el log.

        Args:
            df (pd.DataFrame): DataFrame crudo tal como salió del extractor.
            nombre_tabla (str): Nombre de la tabla destino (para logs y auditoría).

        Returns:
            pd.DataFrame: DataFrame limpio y listo para cargar en SQL Server.
        """
        self.logger.info(f"Iniciando transformación para: '{nombre_tabla}'")
        filas_iniciales = len(df)

        # -----------------------------------------------------------------
        # TRANSFORMACIÓN 1: Normalizar nombres de columnas
        # "Nombre Cliente" → "nombre_cliente"
        # "Fecha Venta" → "fecha_venta"
        # Esto es crítico porque SQL Server no acepta bien espacios en nombres.
        # -----------------------------------------------------------------
        if TRANSFORM_CONFIG["NORMALIZAR_NOMBRES_COLUMNAS"]:
            df = self._normalizar_columnas(df)

        # -----------------------------------------------------------------
        # TRANSFORMACIÓN 2: Eliminar columnas duplicadas
        # Si el archivo CSV tiene dos columnas con el mismo nombre,
        # se elimina la segunda para evitar errores al crear la tabla SQL.
        # -----------------------------------------------------------------
        if TRANSFORM_CONFIG["ELIMINAR_COLUMNAS_DUPLICADAS"]:
            df = self._eliminar_columnas_duplicadas(df)

        # -----------------------------------------------------------------
        # TRANSFORMACIÓN 3: Eliminar filas completamente vacías
        # Una fila donde TODOS los valores son NaN no aporta datos y
        # puede causar problemas con restricciones NOT NULL en SQL Server.
        # -----------------------------------------------------------------
        if TRANSFORM_CONFIG["ELIMINAR_FILAS_COMPLETAMENTE_NULAS"]:
            df = self._eliminar_filas_vacias(df)

        # -----------------------------------------------------------------
        # TRANSFORMACIÓN 4: Eliminar filas duplicadas exactas
        # Dos filas idénticas en todos sus valores se reducen a una sola.
        # -----------------------------------------------------------------
        df = self._eliminar_duplicados(df)

        # -----------------------------------------------------------------
        # TRANSFORMACIÓN 5: Limpiar valores de texto (strings)
        # Quita espacios al inicio/final y caracteres de control invisibles.
        # Ejemplo: "  Juan\t" → "Juan"
        # -----------------------------------------------------------------
        df = self._limpiar_strings(df)

        # -----------------------------------------------------------------
        # TRANSFORMACIÓN 6: Inferir tipos de dato correctos
        # Pandas a veces lee todo como texto. Aquí intentamos convertir
        # automáticamente columnas numéricas y de fecha al tipo correcto.
        # -----------------------------------------------------------------
        df = self._inferir_tipos(df)

        # -----------------------------------------------------------------
        # TRANSFORMACIÓN 7: Reportar valores nulos por columna
        # No se eliminan los nulos (pueden ser válidos), solo se reportan
        # en el log para que el analista esté informado.
        # -----------------------------------------------------------------
        if TRANSFORM_CONFIG["REPORTAR_NULOS"]:
            self._reportar_nulos(df, nombre_tabla)

        # -----------------------------------------------------------------
        # TRANSFORMACIÓN 8: Agregar columnas de auditoría
        # Estas columnas adicionales permiten rastrear el origen y momento
        # de cada registro en SQL Server. Son muy útiles para debugging
        # y para construir dashboards de calidad de datos en Power BI.
        # -----------------------------------------------------------------
        df = self._agregar_columnas_auditoria(df, nombre_tabla)

        # -----------------------------------------------------------------
        # RESUMEN FINAL DE LA TRANSFORMACIÓN
        # -----------------------------------------------------------------
        filas_finales = len(df)
        filas_eliminadas = filas_iniciales - filas_finales
        self.logger.info(
            f"Transformación completada para '{nombre_tabla}' | "
            f"Filas entrada: {filas_iniciales:,} | "
            f"Filas salida: {filas_finales:,} | "
            f"Filas eliminadas: {filas_eliminadas:,}"
        )

        return df

    # =========================================================================
    # MÉTODOS PRIVADOS DE TRANSFORMACIÓN
    # =========================================================================

    def _normalizar_columnas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza los nombres de las columnas:
          1. Elimina acentos y caracteres especiales (ñ → n, é → e)
          2. Convierte a minúsculas
          3. Reemplaza espacios y guiones por guión bajo (_)
          4. Elimina cualquier carácter que no sea letra, número o guión bajo

        Ejemplo:
          "Año de Venta"  → "ano_de_venta"
          "% Descuento"   → "descuento"
          "ID Cliente"    → "id_cliente"
        """
        nuevos_nombres = {}
        for col in df.columns:
            nombre = str(col)

            # Eliminar acentos: descompone el carácter en base + diacrítico,
            # luego filtra solo los caracteres ASCII básicos
            nombre = unicodedata.normalize("NFKD", nombre)
            nombre = nombre.encode("ascii", "ignore").decode("ascii")

            # A minúsculas
            nombre = nombre.lower()

            # Espacios, guiones y puntos → guión bajo
            nombre = re.sub(r"[\s\-\.]+", "_", nombre)

            # Eliminar todo lo que no sea alfanumérico o guión bajo
            nombre = re.sub(r"[^\w]", "", nombre)

            # Eliminar guiones bajos repetidos
            nombre = re.sub(r"_+", "_", nombre)

            # Eliminar guiones bajos al inicio o final
            nombre = nombre.strip("_")

            nuevos_nombres[col] = nombre or f"columna_{list(df.columns).index(col)}"

        df = df.rename(columns=nuevos_nombres)
        self.logger.debug(f"  Columnas normalizadas: {list(df.columns)}")
        return df

    def _eliminar_columnas_duplicadas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Si existen dos o más columnas con el mismo nombre, conserva solo
        la primera ocurrencia y elimina las demás.
        """
        cols_duplicadas = df.columns[df.columns.duplicated()].tolist()
        if cols_duplicadas:
            self.logger.warning(f"  Columnas duplicadas eliminadas: {cols_duplicadas}")
            df = df.loc[:, ~df.columns.duplicated()]
        return df

    def _eliminar_filas_vacias(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas donde TODOS los valores son NaN o están vacíos.
        Una fila parcialmente nula NO se elimina aquí (es un dato válido,
        simplemente incompleto).
        """
        antes = len(df)
        df = df.dropna(how="all")
        eliminadas = antes - len(df)
        if eliminadas > 0:
            self.logger.debug(f"  Filas completamente vacías eliminadas: {eliminadas:,}")
        return df

    def _eliminar_duplicados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas exactamente idénticas en TODAS sus columnas.
        keep="first" conserva la primera ocurrencia.
        """
        antes = len(df)
        df = df.drop_duplicates(keep="first")
        eliminadas = antes - len(df)
        if eliminadas > 0:
            self.logger.warning(f"  Filas duplicadas eliminadas: {eliminadas:,}")
        return df

    def _limpiar_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Para cada columna de tipo texto (object):
          - Quita espacios en blanco al inicio y al final (.strip())
          - Reemplaza strings vacíos "" por NaN (valor nulo real de Pandas)
          - Elimina caracteres de control (tabulaciones, saltos de línea, etc.)

        Esto previene problemas como registros con " Juan " (espacio incluido)
        que no coincidirían con "Juan" en joins de SQL.
        """
        for col in df.select_dtypes(include=["object"]).columns:
            # Strip de espacios
            df[col] = df[col].astype(str).str.strip()

            # Eliminar caracteres de control (\t, \n, \r)
            df[col] = df[col].str.replace(r"[\t\n\r]", " ", regex=True)

            # Convertir el string literal "nan" (generado por astype(str) sobre NaN) a NaN real
            df[col] = df[col].replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})

        return df

    def _inferir_tipos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Intenta convertir columnas a sus tipos de dato correctos.

        Pandas por defecto lee todo como texto (object). Esta función:
          1. Intenta convertir a numérico con pd.to_numeric (errors="ignore")
          2. Intenta convertir a fecha con pd.to_datetime (errors="ignore")

        Si la conversión falla, la columna mantiene su tipo original.
        `downcast` optimiza el uso de memoria eligiendo el tipo entero más pequeño.
        """
        for col in df.columns:
            # Saltar columnas que ya son numéricas o de fecha
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                continue

            # Intento 1: convertir a numérico
            convertido = pd.to_numeric(df[col], errors="coerce")
            # Si más del 70% de los valores se convirtieron exitosamente, aplicamos
            tasa_exito = convertido.notna().sum() / max(len(df), 1)
            if tasa_exito >= 0.7:
                df[col] = convertido
                self.logger.debug(f"  Columna '{col}' convertida a numérico.")
                continue

            # Intento 2: convertir a fecha (solo si el nombre sugiere que es fecha)
            palabras_fecha = ["fecha", "date", "fec", "dt", "time", "dia", "mes", "ano"]
            if any(p in col.lower() for p in palabras_fecha):
                try:
                    convertido_fecha = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    tasa_exito_fecha = convertido_fecha.notna().sum() / max(len(df), 1)
                    if tasa_exito_fecha >= 0.7:
                        df[col] = convertido_fecha
                        self.logger.debug(f"  Columna '{col}' convertida a datetime.")
                except Exception:
                    pass  # Si falla, dejar el tipo original

        return df

    def _reportar_nulos(self, df: pd.DataFrame, nombre_tabla: str) -> None:
        """
        Registra en el log un reporte de columnas con valores nulos.
        No modifica el DataFrame — es solo informativo para el analista.

        Ejemplo de salida en el log:
          Reporte de nulos para 'ventas':
            - precio_unitario: 12 nulos (4.5%)
            - descripcion: 3 nulos (1.1%)
        """
        nulos = df.isnull().sum()
        columnas_con_nulos = nulos[nulos > 0]

        if columnas_con_nulos.empty:
            self.logger.info(f"  Sin valores nulos detectados en '{nombre_tabla}'.")
        else:
            self.logger.warning(f"  Reporte de nulos en '{nombre_tabla}':")
            for col, cantidad in columnas_con_nulos.items():
                porcentaje = (cantidad / len(df)) * 100
                self.logger.warning(f"    - {col}: {cantidad:,} nulos ({porcentaje:.1f}%)")

    def _agregar_columnas_auditoria(
        self, df: pd.DataFrame, nombre_tabla: str
    ) -> pd.DataFrame:
        """
        Agrega columnas de metadatos a cada registro para trazabilidad:

          - etl_fecha_carga:  Fecha y hora exacta en que se ejecutó el ETL.
                              Útil para saber qué tan recientes son los datos en Power BI.
          - etl_archivo_fuente: Nombre del archivo de origen del registro.
                              Permite rastrear de qué dataset provino cada fila.

        Estas columnas son invisibles para el negocio pero invaluables para
        el equipo de datos cuando hay que debuggear discrepancias.
        """
        ahora = datetime.now()
        df["etl_fecha_carga"] = ahora
        df["etl_archivo_fuente"] = nombre_tabla
        self.logger.debug(f"  Columnas de auditoría agregadas (etl_fecha_carga, etl_archivo_fuente).")
        return df
