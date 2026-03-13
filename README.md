# DataWhereHouse — Pipeline ETL

Pipeline ETL en Python que carga archivos desde la carpeta `Datasets/` hacia SQL Server Community Edition, listos para ser consumidos por Power BI.

---

## Estructura del Proyecto

```
DataWhereHouse/
├── Datasets/                  ← Deposita aquí tus archivos (CSV, Excel, JSON)
│   └── procesados/            ← Los archivos se mueven aquí tras cargarse
├── ETL/
│   ├── config.py              ← ⚙️  CONFIGURA AQUÍ tu servidor SQL y comportamiento
│   ├── extractor.py           ← Capa E: lee archivos como DataFrames
│   ├── transformer.py         ← Capa T: limpia y normaliza los datos
│   ├── loader.py              ← Capa L: inserta datos en SQL Server
│   ├── etl_pipeline.py        ← 🚀 Punto de entrada — ejecuta esto
│   ├── logger_config.py       ← Sistema de logging y auditoría
│   ├── requirements.txt       ← Dependencias de Python
│   └── logs/                  ← Archivos de log por ejecución (auto-generado)
├── PowerBi/                   ← Coloca aquí tus archivos .pbix
└── README.md
```

---

## Requisitos Previos

### 1. Python
- Python 3.10 o superior
- Verifica con: `python --version`

### 2. Driver ODBC para SQL Server
- Descarga e instala desde Microsoft: [ODBC Driver 17 for SQL Server](https://aka.ms/downloadmsodbcsql)
- Verifica los drivers instalados en PowerShell:
  ```powershell
  Get-OdbcDriver | Select-Object Name | Where-Object {$_.Name -like "*SQL*"}
  ```

### 3. SQL Server Community Edition
- Debe estar activo y accesible
- Crea la base de datos destino antes de ejecutar:
  ```sql
  CREATE DATABASE DataWhereHouse;
  ```

---

## Instalación

```powershell
# 1. Ir a la carpeta ETL
cd "C:\Users\kmolina\Documents\DataWhereHouse\ETL"

# 2. (Recomendado) Crear entorno virtual
python -m venv .venv
.venv\Scripts\Activate.ps1

# 3. Instalar dependencias
pip install -r requirements.txt
```

---

## Configuración

Abre `ETL/config.py` y edita la sección `SQL_SERVER_CONFIG`:

```python
SQL_SERVER_CONFIG = {
    "SERVER":   "localhost",          # ← Cambia por tu servidor
    "DATABASE": "DataWhereHouse",     # ← Cambia por tu base de datos
    "DRIVER":   "ODBC Driver 17 for SQL Server",  # ← Según tu instalación
    "USAR_WINDOWS_AUTH": True,        # ← True para Windows Auth (recomendado)
}
```

---

## Ejecución

```powershell
# 1. Copia tus archivos a la carpeta Datasets/
#    Formatos soportados: .csv, .xlsx, .xls, .json

# 2. Ejecutar el ETL
cd "C:\Users\kmolina\Documents\DataWhereHouse\ETL"
python etl_pipeline.py
```

---

## Flujo de Datos

```
Datasets/ventas.csv
        │
        ▼
┌─────────────────┐
│   EXTRACTOR     │  Lee el archivo → pandas DataFrame
│  extractor.py   │  (CSV / Excel / JSON)
└────────┬────────┘
         │ df_crudo
         ▼
┌─────────────────┐
│  TRANSFORMER    │  • Normaliza nombres de columnas
│ transformer.py  │  • Elimina filas/columnas vacías
│                 │  • Deduplica filas
│                 │  • Limpia strings
│                 │  • Infiere tipos de dato
│                 │  • Agrega columnas de auditoría
└────────┬────────┘
         │ df_limpio
         ▼
┌─────────────────┐
│    LOADER       │  Inserta a SQL Server
│   loader.py     │  Tabla: [dbo].[stg_ventas]
└─────────────────┘
         │
         ▼
  SQL Server → Power BI
```

---

## Tablas Generadas en SQL Server

Cada archivo genera una tabla con el prefijo `stg_` (staging):

| Archivo en Datasets/     | Tabla en SQL Server        |
|--------------------------|----------------------------|
| `ventas.csv`             | `[dbo].[stg_ventas]`       |
| `clientes.xlsx`          | `[dbo].[stg_clientes]`     |
| `productos_2026.json`    | `[dbo].[stg_productos_2026]` |

Columnas de auditoría agregadas a cada tabla:
- `etl_fecha_carga` — Fecha y hora de la carga
- `etl_archivo_fuente` — Nombre del archivo de origen

---

## Conectar Power BI a SQL Server

1. En Power BI Desktop → **Inicio → Obtener datos → SQL Server**
2. Servidor: `localhost` (o el nombre de tu instancia)
3. Base de datos: `DataWhereHouse`
4. Seleccionar las tablas `stg_*` que necesites
5. (Opcional) Usar DirectQuery para que Power BI siempre lea datos frescos

---

## Logs

Cada ejecución genera un archivo de log en `ETL/logs/`:
```
ETL/logs/ETL_2026-03-12_14-30-00.log
```

El log registra:
- Archivos detectados y leídos
- Transformaciones aplicadas
- Filas cargadas por tabla
- Errores con traceback completo
- Resumen de duración y resultado

---

## Solución de Problemas Frecuentes

| Problema | Solución |
|---|---|
| `No module named 'pyodbc'` | Ejecuta `pip install -r requirements.txt` |
| `Can't open lib 'ODBC Driver 17...'` | Instala el driver ODBC desde Microsoft |
| `Login failed for user` | Verifica credenciales en `config.py` |
| `Cannot open database` | Crea la base de datos en SQL Server primero |
| `No se encontraron archivos` | Coloca archivos en `Datasets/` (no en subcarpetas) |
| Excel no se lee | Verifica que openpyxl esté instalado (`pip install openpyxl`) |
