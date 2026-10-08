# Meetup Data Pipeline

Prueba técnica de Data Engineering: carga el dataset de Meetup en Snowflake, prepara tablas analíticas y deja una base reproducible para incorporar orquestación y exportación.

## Estado

| # | Requisito | Estado |
|---:|---|---|
| 1 | Cuenta y conexión a Snowflake | ✅ Validadas en `RAPPI_MEETUP_TEST` |
| 2 | Carga de los 9 CSV en tablas RAW | ✅ Validada |
| 3 | Tablas físicas auxiliares | ✅ `GROUPS_CLEAN`, `EVENTS_CLEAN` y `GROUPS_BY_CITY_CATEGORY` creadas y verificadas |
| 4 | DAG de Airflow cada 15 minutos (`MERGE`, `CREATE`, `REPLACE`) | ⏳ Pendiente |
| 5 | Alertas de Airflow hacia Slack | ⏳ Pendiente |
| 6 | Exportación de tablas procesadas a S3 | ⏳ Pendiente |
| 7 | Entrega de código, evidencias y repositorio | 🔄 En curso |

## Arquitectura

```text
Dataset Meetup → stage Snowflake → RAW_DATA (9 tablas)
                                 → STAGING (grupos y eventos limpios)
                                 → AUX (resumen por ciudad y categoría)
                                 → Airflow / Slack / S3 (pendiente)
```

Las capas viven en la base configurada en Snowflake. En la base de prueba `RAPPI_MEETUP_TEST`:

| Esquema | Contenido | Filas validadas |
|---|---|---:|
| `RAW_DATA` | Datos originales de los nueve CSV | Conteos abajo |
| `STAGING.GROUPS_CLEAN` | Grupos tipados y enriquecidos con ciudad/categoría | 16,330 |
| `STAGING.EVENTS_CLEAN` | Eventos tipados (IDs alfanuméricos conservados como texto) | 5,807 |
| `AUX.GROUPS_BY_CITY_CATEGORY` | Métricas de grupos y eventos por ciudad/categoría | 129 |

Los 16,330 grupos y los 5,807 eventos quedaron representados en el resumen; no se encontraron IDs duplicados ni eventos sin grupo asociado. Las métricas de miembros y RSVP son sumas reportadas por grupo/evento, no conteos de personas únicas. El dataset es una fotografía histórica.

## Requisitos

- Python 3.10 o posterior y Snowflake CLI.
- Una conexión Snowflake CLI con acceso a la base de datos, warehouse y esquemas usados.
- Los nueve CSV del [dataset Meetup en Kaggle](https://www.kaggle.com/megelon/meetup), extraídos en `data/`.

`members.csv` pesa aproximadamente 1.2 GB. Los datos, credenciales y logs locales están excluidos de Git. No uses `ACCOUNTADMIN` para la ejecución normal ni subas claves privadas al repositorio.

## Ejecución en Windows

Desde PowerShell, en la raíz del proyecto:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install snowflake-cli cryptography
```

Descarga los nueve CSV desde Kaggle y colócalos directamente en `data\`. Configura Snowflake CLI con una conexión llamada `meetup_test_etl_conn` o cambia ese nombre en los comandos:

```powershell
snow connection test -c meetup_test_etl_conn
python -m unittest discover -s tests -v
python scripts/run_sql.py --connection meetup_test_etl_conn --file sql/01_setup_stage.sql
python scripts/upload_dataset.py --connection meetup_test_etl_conn
python scripts/run_sql.py --connection meetup_test_etl_conn --file sql/02_verify_stage.sql
python scripts/run_sql.py --connection meetup_test_etl_conn --file sql/03_create_and_load_raw.sql
python scripts/run_sql.py --connection meetup_test_etl_conn --file sql/05_create_auxiliary_tables.sql
```

El rol necesita permisos de uso en la base de datos y warehouse y permisos para crear los objetos en `RAW_DATA`, `STAGING` y `AUX`. Para una instalación nueva, un administrador debe crear/conceder acceso a la base y permitir `CREATE SCHEMA` al rol ETL. La conexión real usada para validar fue `meetup_test_etl_conn` (`SVC_ETL` / `ETL_ROLE`); su configuración y clave privada son locales y no forman parte del repositorio.

Los scripts `01`–`03` preparan el stage, suben los CSV y cargan las tablas RAW. El script `05` crea las tres tablas analíticas; al repetirse, las reemplaza sin modificar RAW. El script `04_fix_encoding_and_reload.sql` es una reparación específica para cargas anteriores, no parte del flujo normal.

## Validación de la carga RAW

Conteos verificados en Snowflake:

| Tabla | Filas |
|---|---:|
| `CATEGORIES` | 33 |
| `CITIES` | 13 |
| `EVENTS` | 5,807 |
| `GROUPS` | 16,330 |
| `GROUPS_TOPICS` | 31,212 |
| `MEMBERS` | 5,893,886 |
| `MEMBERS_TOPICS` | 3,195,245 |
| `TOPICS` | 2,509 |
| `VENUES` | 107,093 |

El diseño carga primero en tablas `_LOAD` y usa `SWAP` para no reemplazar RAW hasta que la carga nueva finalice. Los CSV con bytes UTF-8 inválidos (`groups_topics.csv`, `members.csv`, `topics.csv`) usan un file format que reemplaza esos caracteres; las cargas conservan `ON_ERROR = 'ABORT_STATEMENT'` para evitar omitir filas silenciosamente.

## Estructura

```text
sql/       Scripts de stage, carga RAW, reparación y tablas auxiliares
scripts/   Subida de CSV, ejecución SQL y logging
tests/     Pruebas unitarias locales
data/      CSV descargados (no versionados)
logs/      Logs de ejecución (no versionados)
dags/      Próxima etapa: orquestación con Airflow
```

## Próximas etapas

1. Implementar y probar el DAG idempotente de Airflow con frecuencia de 15 minutos.
2. Configurar alertas de fallos en Slack.
3. Exportar resultados a S3 mediante una integración de almacenamiento segura.
4. Adjuntar evidencias de ejecución y completar la entrega.
