
import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import MYSQL_CONFIG, LOADER_CONFIG


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

        Connection string para MySQL:
            mysql+mysqlconnector://usuario:contraseña@host:puerto/base_datos
        """
        self.logger.info("Conectando a MySQL...")
        self.logger.debug(
            f"  Host: {MYSQL_CONFIG['HOST']}:{MYSQL_CONFIG['PORT']} | "
            f"Base de datos: {MYSQL_CONFIG['DATABASE']} | "
            f"Usuario: {MYSQL_CONFIG['USER']}"
        )

        try:
            connection_string = self._construir_connection_string()
            self.logger.debug(f"  Connection string (sin credenciales): {self._connection_string_segura()}")

            # create_engine crea el pool de conexiones.
            self._engine = create_engine(
                connection_string,
                connect_args={
                    "connection_timeout": MYSQL_CONFIG["TIMEOUT_CONEXION"]
                }
            )

            # Verificar que la conexión funciona ejecutando una query trivial
            self._verificar_conexion()
            self.logger.info(
                f"Conexión exitosa a MySQL: "
                f"[{MYSQL_CONFIG['HOST']}:{MYSQL_CONFIG['PORT']}] → [{MYSQL_CONFIG['DATABASE']}]"
            )

        except Exception as error:
            self.logger.error(f"Error al conectar a MySQL: {error}", exc_info=True)
            raise  # Re-lanzar el error para que el orquestador lo capture

    def desconectar(self) -> None:
        """
        Cierra el pool de conexiones y libera los recursos del Engine.
        Es importante llamarlo al finalizar para no dejar conexiones abiertas en SQL Server.
        """
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self.logger.info("Conexión a MySQL cerrada correctamente.")

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

        # Proteger cargas acumulativas: nunca reemplazar tablas existentes.
        if LOADER_CONFIG.get("IF_EXISTS") != "append":
            self.logger.warning(
                "IF_EXISTS distinto de 'append' detectado. Se fuerza modo 'append' "
                "para no eliminar información histórica."
            )

        # Resolver llaves foráneas si el dataset corresponde a FACT y viene con claves naturales.
        if "fact_ventas_boletos" in nombre_tabla.lower():
            df = self._resolver_fk_fact_ventas_boletos(df)

        # Evitar insertar columnas generadas por MySQL.
        df = df.drop(columns=["ingreso_total"], errors="ignore")

        # Carga incremental automática: insertar solo filas no existentes.
        df = self._filtrar_registros_nuevos(df, nombre_completo)

        if df.empty:
            self.logger.info(f"Sin registros nuevos para '{nombre_completo}'.")
            return 0

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
            with self._engine.begin() as conn:
                filas_insertadas = df.to_sql(
                    name=nombre_completo,
                    con=conn,
                    if_exists="append",
                    index=LOADER_CONFIG["INDEX"],
                    chunksize=LOADER_CONFIG["CHUNKSIZE"],
                    method="multi",          # INSERT INTO tabla VALUES (...), (...), ...
                )

            duracion = time.time() - inicio
            if filas_insertadas is None or filas_insertadas <= 0:
                # En algunos drivers de MySQL rowcount puede venir en negativo con method='multi'.
                filas = len(df)
            else:
                filas = filas_insertadas

            self.logger.info(
                f"Carga exitosa → [{MYSQL_CONFIG['DATABASE']}].[{nombre_completo}] | "
                f"Filas insertadas: {filas:,} | "
                f"Tiempo: {duracion:.2f}s"
            )

            # Tras cargar staging, sincronizar automáticamente al modelo DW final.
            if nombre_completo.startswith("stg_"):
                self._sincronizar_dw_desde_staging(nombre_completo)

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
        Construye el connection string de SQLAlchemy para MySQL.

        Formato:
            mysql+mysqlconnector://usuario:contraseña@host:puerto/base_datos
        """
        import urllib.parse
        usuario = urllib.parse.quote_plus(MYSQL_CONFIG["USER"])
        password = urllib.parse.quote_plus(MYSQL_CONFIG["PASSWORD"])
        host = MYSQL_CONFIG["HOST"]
        port = MYSQL_CONFIG["PORT"]
        base_datos = MYSQL_CONFIG["DATABASE"]

        return f"mysql+mysqlconnector://{usuario}:{password}@{host}:{port}/{base_datos}"

    def _connection_string_segura(self) -> str:
        """
        Devuelve el connection string con la contraseña enmascarada para el log.
        NUNCA registrar credenciales reales en archivos de log.
        """
        host = MYSQL_CONFIG["HOST"]
        port = MYSQL_CONFIG["PORT"]
        base_datos = MYSQL_CONFIG["DATABASE"]
        usuario = MYSQL_CONFIG["USER"]
        return f"Host={host}:{port} | DB={base_datos} | User={usuario} | Password=***"

    def _verificar_conexion(self) -> None:
        """
        Ejecuta una query mínima (SELECT 1) para confirmar que la conexión
        a MySQL es válida antes de intentar cargar datos.

        Lanza excepción si la conexión falla, con un mensaje de error claro.
        """
        try:
            with self._engine.connect() as conn:
                resultado = conn.execute(text("SELECT 1 AS test")).fetchone()
                if resultado[0] != 1:
                    raise ConnectionError("La verificación de conexión devolvió un resultado inesperado.")
        except Exception as error:
            raise ConnectionError(
                f"No se pudo verificar la conexión a MySQL. "
                f"Verifica el host, puerto, usuario y contraseña.\n"
                f"Detalle: {error}"
            )

    def obtener_tablas_existentes(self) -> list:
        """
        Devuelve una lista de todas las tablas existentes en la base de datos
        destino de MySQL. Útil para diagnóstico y validación.

        Returns:
            list: Lista de nombres de tablas (strings).
        """
        query = text(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = :schema "
            "ORDER BY TABLE_NAME"
        )
        with self._engine.connect() as conn:
            resultado = conn.execute(query, {"schema": MYSQL_CONFIG["DATABASE"]})
            return [fila[0] for fila in resultado.fetchall()]

    def _resolver_fk_fact_ventas_boletos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Resuelve IDs de FACT_VENTAS_BOLETOS a partir de columnas naturales,
        solo cuando el ID no viene en el archivo.
        """
        # id_pelicula desde titulo
        if "id_pelicula" not in df.columns and "titulo" in df.columns:
            dim = pd.read_sql("SELECT id_pelicula, titulo FROM DIM_PELICULA", self._engine)
            df = df.merge(dim, on="titulo", how="left")

        # id_teatro desde nombre_teatro (+ ciudad si viene en el archivo)
        if "id_teatro" not in df.columns and "nombre_teatro" in df.columns:
            dim = pd.read_sql("SELECT id_teatro, nombre_teatro, ciudad FROM DIM_TEATRO", self._engine)
            if "ciudad" in df.columns:
                df = df.merge(dim, on=["nombre_teatro", "ciudad"], how="left")
            else:
                dim = dim.sort_values("id_teatro").drop_duplicates(subset=["nombre_teatro"], keep="first")
                df = df.merge(dim[["id_teatro", "nombre_teatro"]], on="nombre_teatro", how="left")

        # id_sala desde numero_sala (+ titulo si viene en el archivo)
        if "id_sala" not in df.columns and "numero_sala" in df.columns:
            dim = pd.read_sql("SELECT id_sala, numero_sala, titulo FROM DIM_SALA", self._engine)
            if "titulo" in df.columns:
                df = df.merge(dim, on=["numero_sala", "titulo"], how="left")
            else:
                dim = dim.sort_values("id_sala").drop_duplicates(subset=["numero_sala"], keep="first")
                df = df.merge(dim[["id_sala", "numero_sala"]], on="numero_sala", how="left")

        # id_tipo_sala desde tipo
        if "id_tipo_sala" not in df.columns and "tipo" in df.columns:
            dim = pd.read_sql("SELECT id_tipo_sala, tipo FROM DIM_TIPO_SALA", self._engine)
            df = df.merge(dim, on="tipo", how="left")

        # id_fecha desde fecha (si aún no existe)
        if "id_fecha" not in df.columns and "fecha" in df.columns:
            serie_fecha = pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True)
            df["id_fecha"] = pd.to_numeric(serie_fecha.dt.strftime("%Y%m%d"), errors="coerce").astype("Int64")

        return df

    def _filtrar_registros_nuevos(self, df: pd.DataFrame, nombre_tabla: str) -> pd.DataFrame:
        """
        Filtra filas ya existentes en destino usando una clave natural mínima.
        No elimina datos históricos; solo evita reinsertar duplicados exactos
        por las columnas de clave detectadas.
        """
        if not self._tabla_existe(nombre_tabla):
            return df

        claves = self._detectar_claves_incrementales(df)
        if not claves:
            self.logger.debug(
                f"No se detectó clave incremental para '{nombre_tabla}'. Se cargan todas las filas."
            )
            return df

        columnas = ", ".join(claves)
        existentes = pd.read_sql(f"SELECT {columnas} FROM {nombre_tabla}", self._engine)
        if existentes.empty:
            return df

        df_comp = df.copy()
        for col in claves:
            df_comp[col] = df_comp[col].astype(str)
            existentes[col] = existentes[col].astype(str)

        marcados = df_comp.merge(existentes.drop_duplicates(), on=claves, how="left", indicator=True)
        nuevos = marcados[marcados["_merge"] == "left_only"].drop(columns=["_merge"])
        self.logger.info(
            f"Filtro incremental '{nombre_tabla}' | claves: {claves} | "
            f"entraron: {len(df):,} | nuevos: {len(nuevos):,} | existentes: {len(df) - len(nuevos):,}"
        )
        return nuevos

    def _detectar_claves_incrementales(self, df: pd.DataFrame) -> list:
        """
        Detecta una combinación de columnas de negocio para incremental.
        Prioriza esquemas del DW de cine.
        """
        candidatos = [
            ["id_pelicula", "id_teatro", "id_fecha", "id_tipo_sala", "id_sala"],
            ["titulo", "nombre_teatro", "numero_sala", "fecha", "tipo"],
            ["id_fecha", "fecha"],
        ]

        columnas_df = set(df.columns)
        for combo in candidatos:
            if all(col in columnas_df for col in combo):
                return combo
        return []

    def _tabla_existe(self, nombre_tabla: str) -> bool:
        query = text(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :tabla LIMIT 1"
        )
        with self._engine.connect() as conn:
            fila = conn.execute(
                query,
                {"schema": MYSQL_CONFIG["DATABASE"], "tabla": nombre_tabla}
            ).fetchone()
        return fila is not None

    def _tabla_tiene_columnas(self, nombre_tabla: str, columnas: list) -> bool:
        """Valida que una tabla tenga todas las columnas requeridas."""
        if not columnas:
            return True

        placeholders = ", ".join([f":c{i}" for i in range(len(columnas))])
        params = {"schema": MYSQL_CONFIG["DATABASE"], "tabla": nombre_tabla}
        for i, col in enumerate(columnas):
            params[f"c{i}"] = col

        query = text(
            "SELECT COUNT(*) "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = :schema "
            "  AND TABLE_NAME = :tabla "
            f"  AND COLUMN_NAME IN ({placeholders})"
        )
        with self._engine.connect() as conn:
            total = conn.execute(query, params).scalar()
        return int(total) == len(columnas)

    def _sincronizar_dw_desde_staging(self, tabla_staging: str) -> None:
        """
        Inserta de forma incremental desde una tabla staging al modelo
        DIM/FACT del DW de cine.
        """
        columnas_necesarias = [
            "titulo_pelicula", "genero", "clasificacion", "duracion_minutos",
            "fecha_estreno_pelicula", "nombre_teatro", "ciudad", "zona",
            "numero_sala", "capacidad_sala", "tipo_sala", "fecha_funcion",
            "dia_semana", "boletos_vendidos", "precio_boleto_cop"
        ]

        if not self._tabla_tiene_columnas(tabla_staging, columnas_necesarias):
            self.logger.warning(
                f"Staging '{tabla_staging}' no tiene la estructura esperada para sincronizar a DIM/FACT. "
                f"Se omite sincronización DW para esta tabla."
            )
            return

        self.logger.info(f"Sincronizando DW desde staging: {tabla_staging}")

        with self._engine.begin() as conn:
            # DIM_TIPO_SALA
            conn.execute(text(
                f"""
                INSERT IGNORE INTO DIM_TIPO_SALA (id_tipo_sala, tipo)
                SELECT
                    CASE
                        WHEN UPPER(TRIM(tipo_sala)) = '2D' THEN 1
                        WHEN UPPER(TRIM(tipo_sala)) = '3D' THEN 2
                        WHEN UPPER(TRIM(tipo_sala)) = 'VIP' THEN 3
                        WHEN UPPER(TRIM(tipo_sala)) = 'XD' THEN 4
                    END AS id_tipo_sala,
                    CASE
                        WHEN UPPER(TRIM(tipo_sala)) = '2D' THEN '2D'
                        WHEN UPPER(TRIM(tipo_sala)) = '3D' THEN '3D'
                        WHEN UPPER(TRIM(tipo_sala)) = 'VIP' THEN 'VIP'
                        WHEN UPPER(TRIM(tipo_sala)) = 'XD' THEN 'XD'
                    END AS tipo
                FROM `{tabla_staging}`
                WHERE tipo_sala IS NOT NULL
                  AND UPPER(TRIM(tipo_sala)) IN ('2D', '3D', 'VIP', 'XD')
                """
            ))

            # DIM_FECHA
            conn.execute(text(
                f"""
                INSERT IGNORE INTO DIM_FECHA (id_fecha, fecha, anio, mes, dia, dia_semana)
                SELECT DISTINCT
                                        CAST(DATE_FORMAT(CAST(fecha_funcion AS DATE), '%Y%m%d') AS UNSIGNED) AS id_fecha,
                                        CAST(fecha_funcion AS DATE) AS fecha,
                                        YEAR(CAST(fecha_funcion AS DATE)) AS anio,
                                        MONTH(CAST(fecha_funcion AS DATE)) AS mes,
                                        DAY(CAST(fecha_funcion AS DATE)) AS dia,
                                        COALESCE(NULLIF(TRIM(dia_semana), ''), DAYNAME(CAST(fecha_funcion AS DATE))) AS dia_semana
                FROM `{tabla_staging}`
                WHERE fecha_funcion IS NOT NULL
                                    AND CAST(fecha_funcion AS DATE) IS NOT NULL
                """
            ))

            # DIM_PELICULA
            conn.execute(text(
                f"""
                INSERT INTO DIM_PELICULA (titulo, genero, clasificacion, duracion, fecha_estreno)
                SELECT DISTINCT
                    TRIM(titulo_pelicula) AS titulo,
                    TRIM(genero) AS genero,
                    TRIM(clasificacion) AS clasificacion,
                    CAST(duracion_minutos AS UNSIGNED) AS duracion,
                    CAST(fecha_estreno_pelicula AS DATE) AS fecha_estreno
                FROM `{tabla_staging}` s
                WHERE titulo_pelicula IS NOT NULL
                  AND genero IS NOT NULL
                  AND clasificacion IS NOT NULL
                  AND duracion_minutos IS NOT NULL
                                    AND TRIM(CAST(duracion_minutos AS CHAR)) REGEXP '^[0-9]+$'
                                    AND CAST(duracion_minutos AS UNSIGNED) BETWEEN 1 AND 600
                  AND fecha_estreno_pelicula IS NOT NULL
                  AND CAST(fecha_estreno_pelicula AS DATE) IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM DIM_PELICULA d
                      WHERE d.titulo = TRIM(s.titulo_pelicula)
                        AND d.genero = TRIM(s.genero)
                        AND d.clasificacion = TRIM(s.clasificacion)
                        AND d.duracion = CAST(s.duracion_minutos AS UNSIGNED)
                                                AND d.fecha_estreno = CAST(s.fecha_estreno_pelicula AS DATE)
                  )
                """
            ))

            # DIM_TEATRO
            conn.execute(text(
                f"""
                INSERT IGNORE INTO DIM_TEATRO (nombre_teatro, ciudad, zona)
                SELECT DISTINCT
                    TRIM(nombre_teatro) AS nombre_teatro,
                    TRIM(ciudad) AS ciudad,
                    TRIM(zona) AS zona
                FROM `{tabla_staging}`
                WHERE nombre_teatro IS NOT NULL
                  AND ciudad IS NOT NULL
                  AND zona IS NOT NULL
                """
            ))

            # DIM_SALA
            conn.execute(text(
                f"""
                INSERT INTO DIM_SALA (titulo, numero_sala, capacidad)
                SELECT DISTINCT
                    TRIM(titulo_pelicula) AS titulo,
                    CAST(numero_sala AS UNSIGNED) AS numero_sala,
                    CAST(capacidad_sala AS UNSIGNED) AS capacidad
                FROM `{tabla_staging}` s
                WHERE titulo_pelicula IS NOT NULL
                  AND numero_sala IS NOT NULL
                  AND capacidad_sala IS NOT NULL
                                    AND TRIM(CAST(numero_sala AS CHAR)) REGEXP '^[0-9]+$'
                                    AND TRIM(CAST(capacidad_sala AS CHAR)) REGEXP '^[0-9]+$'
                                    AND CAST(numero_sala AS UNSIGNED) > 0
                                    AND CAST(capacidad_sala AS UNSIGNED) BETWEEN 1 AND 1000
                  AND NOT EXISTS (
                      SELECT 1
                      FROM DIM_SALA d
                      WHERE d.titulo = TRIM(s.titulo_pelicula)
                        AND d.numero_sala = CAST(s.numero_sala AS UNSIGNED)
                        AND d.capacidad = CAST(s.capacidad_sala AS UNSIGNED)
                  )
                """
            ))

            # FACT_VENTAS_BOLETOS
            conn.execute(text(
                f"""
                INSERT INTO FACT_VENTAS_BOLETOS (
                    id_pelicula, id_teatro, id_fecha, id_tipo_sala, id_sala,
                    boletos_vendidos, precio_boleto
                )
                SELECT
                    dp.id_pelicula,
                    dt.id_teatro,
                    df.id_fecha,
                    dts.id_tipo_sala,
                    ds.id_sala,
                    CAST(s.boletos_vendidos AS UNSIGNED) AS boletos_vendidos,
                    CAST(s.precio_boleto_cop AS DECIMAL(10,2)) AS precio_boleto
                FROM `{tabla_staging}` s
                INNER JOIN DIM_PELICULA dp
                    ON dp.titulo = TRIM(s.titulo_pelicula)
                   AND dp.genero = TRIM(s.genero)
                   AND dp.clasificacion = TRIM(s.clasificacion)
                   AND dp.duracion = CAST(s.duracion_minutos AS UNSIGNED)
                   AND dp.fecha_estreno = CAST(s.fecha_estreno_pelicula AS DATE)
                INNER JOIN DIM_TEATRO dt
                    ON dt.nombre_teatro = TRIM(s.nombre_teatro)
                   AND dt.ciudad = TRIM(s.ciudad)
                INNER JOIN DIM_FECHA df
                    ON df.fecha = CAST(s.fecha_funcion AS DATE)
                INNER JOIN DIM_TIPO_SALA dts
                    ON dts.tipo = UPPER(TRIM(s.tipo_sala))
                INNER JOIN DIM_SALA ds
                    ON ds.titulo = TRIM(s.titulo_pelicula)
                   AND ds.numero_sala = CAST(s.numero_sala AS UNSIGNED)
                   AND ds.capacidad = CAST(s.capacidad_sala AS UNSIGNED)
                WHERE s.boletos_vendidos IS NOT NULL
                  AND s.precio_boleto_cop IS NOT NULL
                                    AND TRIM(CAST(s.boletos_vendidos AS CHAR)) REGEXP '^[0-9]+$'
                                    AND TRIM(CAST(s.precio_boleto_cop AS CHAR)) REGEXP '^[0-9]+(\\.[0-9]+)?$'
                  AND CAST(s.boletos_vendidos AS UNSIGNED) > 0
                                    AND CAST(s.boletos_vendidos AS UNSIGNED) <= 2147483647
                  AND CAST(s.precio_boleto_cop AS DECIMAL(10,2)) > 0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM FACT_VENTAS_BOLETOS f
                      WHERE f.id_pelicula = dp.id_pelicula
                        AND f.id_teatro = dt.id_teatro
                        AND f.id_fecha = df.id_fecha
                        AND f.id_tipo_sala = dts.id_tipo_sala
                        AND f.id_sala = ds.id_sala
                        AND f.boletos_vendidos = CAST(s.boletos_vendidos AS UNSIGNED)
                        AND f.precio_boleto = CAST(s.precio_boleto_cop AS DECIMAL(10,2))
                  )
                """
            ))

        self.logger.info(f"Sincronización DW completada para staging: {tabla_staging}")
