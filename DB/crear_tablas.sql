-- =============================================================================
-- SCRIPT: crear_tablas.sql
-- PROYECTO: DataWhereHouse — Analítica de Aforo en Cine
-- MODELO: Data Warehouse — Esquema Estrella
-- =============================================================================

USE Cine;
GO

-- =============================================================================
-- ELIMINAR TABLAS SI YA EXISTEN
-- Se eliminan en orden inverso para respetar las llaves foráneas
-- =============================================================================

IF OBJECT_ID('dbo.FACT_VENTAS_BOLETOS', 'U') IS NOT NULL DROP TABLE dbo.FACT_VENTAS_BOLETOS;
IF OBJECT_ID('dbo.DIM_SALA',            'U') IS NOT NULL DROP TABLE dbo.DIM_SALA;
IF OBJECT_ID('dbo.DIM_FECHA',           'U') IS NOT NULL DROP TABLE dbo.DIM_FECHA;
IF OBJECT_ID('dbo.DIM_PELICULA',        'U') IS NOT NULL DROP TABLE dbo.DIM_PELICULA;
IF OBJECT_ID('dbo.DIM_TEATRO',          'U') IS NOT NULL DROP TABLE dbo.DIM_TEATRO;
IF OBJECT_ID('dbo.DIM_TIPO_SALA',       'U') IS NOT NULL DROP TABLE dbo.DIM_TIPO_SALA;
GO

-- =============================================================================
-- DIM_TIPO_SALA
-- Catálogo de tipos de sala: 2D, 3D, VIP, XD
-- =============================================================================

CREATE TABLE dbo.DIM_TIPO_SALA (
    id_tipo_sala    INT          NOT NULL,
    tipo            VARCHAR(10)  NOT NULL,

    CONSTRAINT PK_DIM_TIPO_SALA     PRIMARY KEY (id_tipo_sala),
    CONSTRAINT UQ_TIPO_SALA_TIPO    UNIQUE (tipo),
    CONSTRAINT CK_TIPO_SALA_VALOR   CHECK (tipo IN ('2D', '3D', 'VIP', 'XD'))
);
GO

-- =============================================================================
-- DIM_PELICULA
-- Catálogo maestro de películas proyectadas
-- =============================================================================

CREATE TABLE dbo.DIM_PELICULA (
    id_pelicula             INT             NOT NULL IDENTITY(1,1),
    titulo                  NVARCHAR(200)   NOT NULL,
    genero                  VARCHAR(60)     NOT NULL,
    clasificacion           VARCHAR(10)     NOT NULL,
    duracion                INT             NOT NULL,
    fecha_estreno           DATE            NOT NULL,
    fecha_creacion          DATETIME        NOT NULL DEFAULT GETDATE(),
    usuario_creacion        VARCHAR(50)     NOT NULL DEFAULT SYSTEM_USER,
    fecha_modificacion      DATETIME        NULL,
    usuario_modificacion    VARCHAR(50)     NULL,

    CONSTRAINT PK_DIM_PELICULA      PRIMARY KEY (id_pelicula),
    CONSTRAINT CK_PELICULA_DURACION CHECK (duracion > 0 AND duracion <= 600)
);
GO

-- =============================================================================
-- DIM_TEATRO
-- Catálogo de complejos cinematográficos por ciudad
-- =============================================================================

CREATE TABLE dbo.DIM_TEATRO (
    id_teatro               INT             NOT NULL IDENTITY(1,1),
    nombre_teatro           NVARCHAR(200)   NOT NULL,
    ciudad                  NVARCHAR(100)   NOT NULL,
    zona                    NVARCHAR(100)   NOT NULL,
    fecha_creacion          DATETIME        NOT NULL DEFAULT GETDATE(),
    usuario_creacion        VARCHAR(50)     NOT NULL DEFAULT SYSTEM_USER,
    fecha_modificacion      DATETIME        NULL,
    usuario_modificacion    VARCHAR(50)     NULL,

    CONSTRAINT PK_DIM_TEATRO                PRIMARY KEY (id_teatro),
    CONSTRAINT UQ_TEATRO_NOMBRE_CIUDAD      UNIQUE (nombre_teatro, ciudad)
);
GO

-- =============================================================================
-- DIM_SALA
-- Salas físicas dentro de cada teatro
-- =============================================================================

CREATE TABLE dbo.DIM_SALA (
    id_sala                 INT             NOT NULL IDENTITY(1,1),
    id_teatro               INT             NOT NULL,
    titulo                  NVARCHAR(100)   NOT NULL,
    numero_sala             INT             NOT NULL,
    capacidad               INT             NOT NULL,
    fecha_creacion          DATETIME        NOT NULL DEFAULT GETDATE(),
    usuario_creacion        VARCHAR(50)     NOT NULL DEFAULT SYSTEM_USER,
    fecha_modificacion      DATETIME        NULL,
    usuario_modificacion    VARCHAR(50)     NULL,

    CONSTRAINT PK_DIM_SALA              PRIMARY KEY (id_sala),
    CONSTRAINT FK_SALA_TEATRO           FOREIGN KEY (id_teatro) REFERENCES dbo.DIM_TEATRO(id_teatro),
    CONSTRAINT CK_SALA_CAPACIDAD        CHECK (capacidad > 0 AND capacidad <= 1000),
    CONSTRAINT CK_SALA_NUMERO           CHECK (numero_sala > 0),
    CONSTRAINT UQ_SALA_NUMERO_TEATRO    UNIQUE (id_teatro, numero_sala)
);
GO

-- =============================================================================
-- DIM_FECHA
-- Dimensión de tiempo: un registro por cada día del calendario
-- id_fecha usa formato YYYYMMDD (ej: 20260312)
-- =============================================================================

CREATE TABLE dbo.DIM_FECHA (
    id_fecha        INT          NOT NULL,
    fecha           DATE         NOT NULL,
    ano             INT          NOT NULL,
    mes             INT          NOT NULL,
    nombre_mes      VARCHAR(20)  NOT NULL,
    dia             INT          NOT NULL,
    dia_semana      VARCHAR(20)  NOT NULL,
    numero_semana   INT          NOT NULL,
    es_fin_semana   BIT          NOT NULL,
    trimestre       INT          NOT NULL,

    CONSTRAINT PK_DIM_FECHA     PRIMARY KEY (id_fecha),
    CONSTRAINT UQ_FECHA_FECHA   UNIQUE (fecha),
    CONSTRAINT CK_FECHA_MES     CHECK (mes BETWEEN 1 AND 12),
    CONSTRAINT CK_FECHA_DIA     CHECK (dia BETWEEN 1 AND 31),
    CONSTRAINT CK_FECHA_TRIM    CHECK (trimestre BETWEEN 1 AND 4)
);
GO

-- =============================================================================
-- FACT_VENTAS_BOLETOS
-- Tabla de hechos central: cada fila es una función de cine vendida
-- ingreso_total es columna calculada y persistida (boletos × precio)
-- =============================================================================

CREATE TABLE dbo.FACT_VENTAS_BOLETOS (
    id_fact             INT             NOT NULL IDENTITY(1,1),
    id_pelicula         INT             NOT NULL,
    id_teatro           INT             NOT NULL,
    id_sala             INT             NOT NULL,
    id_fecha            INT             NOT NULL,
    id_tipo_sala        INT             NOT NULL,
    boletos_vendidos    INT             NOT NULL,
    precio_boleto       DECIMAL(10,2)   NOT NULL,
    ingreso_total       AS (boletos_vendidos * precio_boleto) PERSISTED,
    fecha_carga         DATETIME        NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_FACT_VENTAS_BOLETOS   PRIMARY KEY (id_fact),
    CONSTRAINT FK_FACT_PELICULA         FOREIGN KEY (id_pelicula)   REFERENCES dbo.DIM_PELICULA(id_pelicula),
    CONSTRAINT FK_FACT_TEATRO           FOREIGN KEY (id_teatro)     REFERENCES dbo.DIM_TEATRO(id_teatro),
    CONSTRAINT FK_FACT_SALA             FOREIGN KEY (id_sala)       REFERENCES dbo.DIM_SALA(id_sala),
    CONSTRAINT FK_FACT_FECHA            FOREIGN KEY (id_fecha)      REFERENCES dbo.DIM_FECHA(id_fecha),
    CONSTRAINT FK_FACT_TIPO_SALA        FOREIGN KEY (id_tipo_sala)  REFERENCES dbo.DIM_TIPO_SALA(id_tipo_sala),
    CONSTRAINT CK_FACT_PRECIO           CHECK (precio_boleto > 0),
    CONSTRAINT CK_FACT_BOLETOS          CHECK (boletos_vendidos > 0)
);
GO
