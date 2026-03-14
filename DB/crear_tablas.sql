-- =============================================================================
-- SCRIPT: crear_tablas.sql
-- PROYECTO: DataWhereHouse — Analítica de Aforo en Cine
-- MODELO: Data Warehouse — Esquema Estrella
-- BASE DE DATOS: MySQL
-- =============================================================================

CREATE DATABASE IF NOT EXISTS Cine
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE Cine;

-- =============================================================================
-- ELIMINAR TABLAS SI YA EXISTEN
-- Se eliminan en orden inverso para respetar las llaves foráneas
-- =============================================================================

DROP TABLE IF EXISTS FACT_VENTAS_BOLETOS;
DROP TABLE IF EXISTS DIM_SALA;
DROP TABLE IF EXISTS DIM_FECHA;
DROP TABLE IF EXISTS DIM_PELICULA;
DROP TABLE IF EXISTS DIM_TEATRO;
DROP TABLE IF EXISTS DIM_TIPO_SALA;

-- =============================================================================
-- DIM_TIPO_SALA
-- Catálogo de tipos de sala: 2D, 3D, VIP, XD
-- =============================================================================

CREATE TABLE DIM_TIPO_SALA (
    id_tipo_sala    INT         NOT NULL,
    tipo            VARCHAR(10) NOT NULL,

    CONSTRAINT PK_DIM_TIPO_SALA   PRIMARY KEY (id_tipo_sala),
    CONSTRAINT UQ_TIPO_SALA_TIPO  UNIQUE (tipo),
    CONSTRAINT CK_TIPO_SALA_VALOR CHECK (tipo IN ('2D', '3D', 'VIP', 'XD'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- DIM_PELICULA
-- Catálogo maestro de películas proyectadas
-- =============================================================================

CREATE TABLE DIM_PELICULA (
    id_pelicula          INT          NOT NULL AUTO_INCREMENT,
    titulo               VARCHAR(200) NOT NULL,
    genero               VARCHAR(60)  NOT NULL,
    clasificacion        VARCHAR(10)  NOT NULL,
    duracion             INT          NOT NULL,
    fecha_estreno        DATE         NOT NULL,
    fecha_creacion       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    usuario_creacion     VARCHAR(50)  NOT NULL DEFAULT (CURRENT_USER()),
    fecha_modificacion   DATETIME     NULL,
    usuario_modificacion VARCHAR(50)  NULL,

    CONSTRAINT PK_DIM_PELICULA      PRIMARY KEY (id_pelicula),
    CONSTRAINT CK_PELICULA_DURACION CHECK (duracion > 0 AND duracion <= 600)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- DIM_TEATRO
-- Catálogo de complejos cinematográficos por ciudad
-- Orden de columnas según modelo de imagen
-- =============================================================================

CREATE TABLE DIM_TEATRO (
    id_teatro            INT          NOT NULL AUTO_INCREMENT,
    nombre_teatro        VARCHAR(200) NOT NULL,
    ciudad               VARCHAR(100) NOT NULL,
    zona                 VARCHAR(100) NOT NULL,
    usuario_modificacion VARCHAR(50)  NULL,
    fecha_modificacion   DATETIME     NULL,
    usuario_creacion     VARCHAR(50)  NOT NULL DEFAULT (CURRENT_USER()),
    fecha_creacion       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_DIM_TEATRO           PRIMARY KEY (id_teatro),
    CONSTRAINT UQ_TEATRO_NOMBRE_CIUDAD UNIQUE (nombre_teatro, ciudad)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- DIM_SALA
-- Salas físicas — sin FK a teatro (según modelo de imagen)
-- Orden de columnas según modelo de imagen
-- =============================================================================

CREATE TABLE DIM_SALA (
    id_sala              INT          NOT NULL AUTO_INCREMENT,
    titulo               VARCHAR(100) NOT NULL,
    numero_sala          INT          NOT NULL,
    capacidad            INT          NOT NULL,
    fecha_creacion       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    usuario_modificacion VARCHAR(50)  NULL,
    fecha_modificacion   DATETIME     NULL,
    usuario_creacion     VARCHAR(50)  NOT NULL DEFAULT (CURRENT_USER()),

    CONSTRAINT PK_DIM_SALA       PRIMARY KEY (id_sala),
    CONSTRAINT CK_SALA_CAPACIDAD CHECK (capacidad > 0 AND capacidad <= 1000),
    CONSTRAINT CK_SALA_NUMERO    CHECK (numero_sala > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- DIM_FECHA
-- Dimensión de tiempo — columnas exactas según modelo de imagen
-- id_fecha usa formato YYYYMMDD (ej: 20260313)
-- =============================================================================

CREATE TABLE DIM_FECHA (
    id_fecha    INT         NOT NULL,
    fecha       DATE        NOT NULL,
    anio        INT         NOT NULL,
    mes         INT         NOT NULL,
    dia         INT         NOT NULL,
    dia_semana  VARCHAR(20) NOT NULL,

    CONSTRAINT PK_DIM_FECHA   PRIMARY KEY (id_fecha),
    CONSTRAINT UQ_FECHA_FECHA UNIQUE (fecha),
    CONSTRAINT CK_FECHA_MES   CHECK (mes BETWEEN 1 AND 12),
    CONSTRAINT CK_FECHA_DIA   CHECK (dia BETWEEN 1 AND 31)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- FACT_VENTAS_BOLETOS
-- Tabla de hechos central — columnas y FK según modelo de imagen
-- ingreso_total es columna generada y persistida (boletos × precio)
-- =============================================================================

CREATE TABLE FACT_VENTAS_BOLETOS (
    id_fact          INT            NOT NULL AUTO_INCREMENT,
    id_pelicula      INT            NOT NULL,
    id_teatro        INT            NOT NULL,
    id_fecha         INT            NOT NULL,
    id_tipo_sala     INT            NOT NULL,
    id_sala          INT            NOT NULL,
    boletos_vendidos INT            NOT NULL,
    precio_boleto    DECIMAL(10,2)  NOT NULL,
    ingreso_total    DECIMAL(12,2)  GENERATED ALWAYS AS (boletos_vendidos * precio_boleto) STORED,
    fecha_carga      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_FACT_VENTAS_BOLETOS PRIMARY KEY (id_fact),
    CONSTRAINT FK_FACT_PELICULA       FOREIGN KEY (id_pelicula)  REFERENCES DIM_PELICULA(id_pelicula),
    CONSTRAINT FK_FACT_TEATRO         FOREIGN KEY (id_teatro)    REFERENCES DIM_TEATRO(id_teatro),
    CONSTRAINT FK_FACT_FECHA          FOREIGN KEY (id_fecha)     REFERENCES DIM_FECHA(id_fecha),
    CONSTRAINT FK_FACT_TIPO_SALA      FOREIGN KEY (id_tipo_sala) REFERENCES DIM_TIPO_SALA(id_tipo_sala),
    CONSTRAINT FK_FACT_SALA           FOREIGN KEY (id_sala)      REFERENCES DIM_SALA(id_sala),
    CONSTRAINT CK_FACT_PRECIO         CHECK (precio_boleto > 0),
    CONSTRAINT CK_FACT_BOLETOS        CHECK (boletos_vendidos > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
