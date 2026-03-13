
import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import SQL_SERVER_CONFIG, LOADER_CONFIG


class Loader:
    """
    Clase responsable de la fase de CARGA del ETL.

    Mantiene una única conexión activa durante toda la ejecución del pipeline
    (patrón singleton de conexión) para no abrir/cerrar la conexión con cada tabla.

    Uso típico:
        loader = Loader(logger)
        with loader:
            loader.cargar(df, "ventas_2026")
    """

    def __init__(self, logger: logging.Logger):
        """
        Args:
            logger: Logger compartido del pipeline.
        """
        self.logger = logger
        self._engine: Engine = None   # Se inicializa al conectar

    # =========================================================================
    # GESTIÓN DE CONTEXTO (with statement)
    # Permite usar: with Loader(logger) as loader:
    # Garantiza que la conexión se cierre aunque ocurra un error.
    # =========================================================================

    def __enter__(self):
        """Al entrar al bloque 'with', establece la conexión."""
        self.conectar()
        return self

    def __exit__(self, tipo_exc, valor_exc, traceback):
        """Al salir del bloque 'with', cierra la conexión sin importar si hubo error."""
        self.desconectar()
        return False  # No suprime excepciones; se propagan normalmente

    # =========================================================================
    # MÉTODOS DE CONEXIÓN
    # =========================================================================

    def conectar(self) -> None:
        """
        Construye el connection string y crea el Engine de SQLAlchemy.

        El Engine es un pool de conexiones reutilizables, no una conexión
        física directa. La conexión real se establece solo cuando se ejecuta
        una query (lazy connection).

        Connection string para Windows Authentication:
            mssql+pyodbc://servidor/base_de_datos?driver=...&trusted_connection=yes

        Connection string para SQL Authentication:
            mssql+pyodbc://usuario:contraseña@servidor/base_de_datos?driver=...
        """
        self.logger.info("Conectando a SQL Server...")
        self.logger.debug(
            f"  Servidor: {SQL_SERVER_CONFIG['SERVER']} | "
            f"Base de datos: {SQL_SERVER_CONFIG['DATABASE']} | "
            f"Driver: {SQL_SERVER_CONFIG['DRIVER']}"
        )

        try:
            connection_string = self._construir_connection_string()
            self.logger.debug(f"  Connection string (sin credenciales): {self._connection_string_segura()}")

            # create_engine crea el pool de conexiones.
            # fast_executemany=True acelera enormemente las inserciones masivas
            # al enviar todos los registros en un solo batch al servidor.
            self._engine = create_engine(
                connection_string,
                fast_executemany=True,          # Inserción masiva optimizada
                connect_args={
                    "timeout": SQL_SERVER_CONFIG["TIMEOUT_CONEXION"]
                }
            )

            # Verificar que la conexión funciona ejecutando una query trivial
            self._verificar_conexion()
            self.logger.info(
                f"Conexión exitosa a SQL Server: "
                f"[{SQL_SERVER_CONFIG['SERVER']}] → [{SQL_SERVER_CONFIG['DATABASE']}]"
            )

        except Exception as error:
            self.logger.error(f"Error al conectar a SQL Server: {error}", exc_info=True)
            raise  # Re-lanzar el error para que el orquestador lo capture

    def desconectar(self) -> None:
        """
        Cierra el pool de conexiones y libera los recursos del Engine.
        Es importante llamarlo al finalizar para no dejar conexiones abiertas en SQL Server.
        """
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self.logger.info("Conexión a SQL Server cerrada correctamente.")

    # =========================================================================
    # MÉTODO PRINCIPAL DE CARGA
    # =========================================================================

    def cargar(self, df: pd.DataFrame, nombre_tabla: str) -> int:
        """
        Carga un DataFrame a una tabla de SQL Server.

        Usa pandas.DataFrame.to_sql() que internamente:
          1. Infiere los tipos de columna del DataFrame
          2. Genera el CREATE TABLE si no existe
          3. Hace INSERT en lotes (chunksize) usando fast_executemany

        Args:
            df (pd.DataFrame): DataFrame transformado listo para insertar.
            nombre_tabla (str): Nombre base de la tabla (sin prefijo).

        Returns:
            int: Número de filas insertadas exitosamente.

        Raises:
            RuntimeError: Si se llama sin haber establecido conexión primero.
            Exception: Si la inserción en SQL Server falla.
        """
        if self._engine is None:
            raise RuntimeError("No hay conexión activa. Llama a conectar() primero o usa 'with Loader(logger) as loader:'")

        # Construir nombre final con prefijo configurado
        nombre_completo = f"{LOADER_CONFIG['PREFIJO_TABLA']}{nombre_tabla}"

        self.logger.info(
            f"Cargando tabla '{nombre_completo}' | "
            f"Filas: {len(df):,} | Modo: {LOADER_CONFIG['IF_EXISTS']}"
        )

        inicio = time.time()

        try:
            # -----------------------------------------------------------------
            # INSERCIÓN PRINCIPAL
            # pandas.to_sql convierte el DataFrame a SQL automáticamente.
            #
            # Parámetros clave:
            #   name         → nombre de la tabla en SQL Server
            #   con          → engine de SQLAlchemy (pool de conexiones)
            #   if_exists    → comportamiento si la tabla ya existe:
            #                    "append"  = agregar filas
            #                    "replace" = borrar y recrear tabla
            #                    "fail"    = lanzar error
            #   index        → si True, escribe el índice de Pandas como columna
            #   chunksize    → cantidad de filas por batch de inserción
            #   method       → "multi" usa INSERT múltiple (más eficiente)
            # -----------------------------------------------------------------
            filas_insertadas = df.to_sql(
                name=nombre_completo,
                con=self._engine,
                if_exists=LOADER_CONFIG["IF_EXISTS"],
                index=LOADER_CONFIG["INDEX"],
                chunksize=LOADER_CONFIG["CHUNKSIZE"],
                method="multi",          # INSERT INTO tabla VALUES (...), (...), ...
                schema="dbo",            # Esquema dbo es el default de SQL Server
            )

            duracion = time.time() - inicio
            filas = filas_insertadas if filas_insertadas is not None else len(df)

            self.logger.info(
                f"Carga exitosa → [{SQL_SERVER_CONFIG['DATABASE']}].[dbo].[{nombre_completo}] | "
                f"Filas insertadas: {filas:,} | "
                f"Tiempo: {duracion:.2f}s"
            )
            return filas

        except Exception as error:
            self.logger.error(
                f"Error al cargar la tabla '{nombre_completo}': {error}",
                exc_info=True
            )
            raise

    # =========================================================================
    # MÉTODOS DE UTILIDAD
    # =========================================================================

    def _construir_connection_string(self) -> str:
        """
        Construye el connection string de SQLAlchemy para SQL Server + ODBC.

        Formato para Windows Authentication (trusted_connection=yes):
            mssql+pyodbc://servidor/base_datos?driver=DRIVER&trusted_connection=yes&TrustServerCertificate=yes

        Formato para SQL Authentication:
            mssql+pyodbc://user:password@servidor/base_datos?driver=DRIVER&TrustServerCertificate=yes

        TrustServerCertificate=yes es necesario para SQL Server con certificados
        auto-firmados (común en instalaciones locales Community Edition).
        """
        driver_encoded = SQL_SERVER_CONFIG["DRIVER"].replace(" ", "+")
        servidor = SQL_SERVER_CONFIG["SERVER"]
        base_datos = SQL_SERVER_CONFIG["DATABASE"]

        if SQL_SERVER_CONFIG["USAR_WINDOWS_AUTH"]:
            # Windows Authentication: usa las credenciales del usuario de Windows actual
            conn_str = (
                f"mssql+pyodbc://{servidor}/{base_datos}"
                f"?driver={driver_encoded}"
                f"&trusted_connection=yes"
                f"&TrustServerCertificate=yes"
            )
        else:
            # SQL Server Authentication: usuario y contraseña de SQL
            import urllib.parse
            usuario = urllib.parse.quote_plus(SQL_SERVER_CONFIG["SQL_USER"])
            password = urllib.parse.quote_plus(SQL_SERVER_CONFIG["SQL_PASSWORD"])
            conn_str = (
                f"mssql+pyodbc://{usuario}:{password}@{servidor}/{base_datos}"
                f"?driver={driver_encoded}"
                f"&TrustServerCertificate=yes"
            )

        return conn_str

    def _connection_string_segura(self) -> str:
        """
        Devuelve el connection string con la contraseña enmascarada para el log.
        NUNCA registrar credenciales reales en archivos de log.
        """
        servidor = SQL_SERVER_CONFIG["SERVER"]
        base_datos = SQL_SERVER_CONFIG["DATABASE"]
        driver = SQL_SERVER_CONFIG["DRIVER"]
        auth = "Windows Auth" if SQL_SERVER_CONFIG["USAR_WINDOWS_AUTH"] else f"SQL User: {SQL_SERVER_CONFIG['SQL_USER']}"
        return f"Server={servidor} | DB={base_datos} | Driver={driver} | Auth={auth}"

    def _verificar_conexion(self) -> None:
        """
        Ejecuta una query mínima (SELECT 1) para confirmar que la conexión
        a SQL Server es válida antes de intentar cargar datos.

        Lanza excepción si la conexión falla, con un mensaje de error claro.
        """
        try:
            with self._engine.connect() as conn:
                resultado = conn.execute(text("SELECT 1 AS test")).fetchone()
                if resultado[0] != 1:
                    raise ConnectionError("La verificación de conexión devolvió un resultado inesperado.")
        except Exception as error:
            raise ConnectionError(
                f"No se pudo verificar la conexión a SQL Server. "
                f"Verifica el servidor, base de datos y driver ODBC.\n"
                f"Detalle: {error}"
            )

    def obtener_tablas_existentes(self) -> list:
        """
        Devuelve una lista de todas las tablas existentes en el esquema 'dbo'
        de la base de datos destino. Útil para diagnóstico y validación.

        Returns:
            list: Lista de nombres de tablas (strings).
        """
        query = text(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' "
            "ORDER BY TABLE_NAME"
        )
        with self._engine.connect() as conn:
            resultado = conn.execute(query)
            return [fila[0] for fila in resultado.fetchall()]
