# Meetup Data Pipeline

Prueba técnica de Data Engineering: carga el dataset de Meetup en Snowflake, prepara tablas analíticas y deja una base reproducible para incorporar orquestación y exportación.

## Estado

| # | Requisito | Estado |
|---:|---|---|
| 1 | Cuenta y conexión a Snowflake | ✅ Validadas en `RAPPI_MEETUP_TEST` |
| 2 | Carga de los 9 CSV en tablas RAW | ✅ Validada |
| 3 | Tablas físicas auxiliares | ✅ `GROUPS_CLEAN`, `EVENTS_CLEAN` y `GROUPS_BY_CITY_CATEGORY` creadas y verificadas |
| 4 | DAG de Airflow cada 15 minutos (`MERGE`, `CREATE OR REPLACE`) | ✅ Validado localmente en Docker |
| 5 | Notificaciones de éxito de Airflow hacia Slack | ✅ Entregadas vía Incoming Webhook |
| 6 | Exportación de tablas procesadas a S3 | ⏳ Pendiente |
| 7 | Entrega de código, evidencias y repositorio | 🔄 En curso |

## Arquitectura

```text
Dataset Meetup → stage Snowflake → RAW_DATA (9 tablas)
                                 → STAGING (grupos y eventos limpios)
                                 → AUX (resumen por ciudad y categoría)
                                 → Airflow Orchestration & Slack Alerts
                                 → S3 (pendiente)
```

Las capas viven en la base configurada en Snowflake. En la base de prueba `RAPPI_MEETUP_TEST`:

| Esquema | Contenido | Filas validadas en la carga inicial |
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

## Orquestación con Apache Airflow (Puntos 4 y 5)

Airflow se ejecuta localmente con Docker Compose (Airflow 2.10.2, CeleryExecutor, PostgreSQL y Redis). Los proveedores Snowflake y Slack se instalan en los contenedores mediante `_PIP_ADDITIONAL_REQUIREMENTS` en el `.env` local; el archivo `.env` no se versiona.

### DAG: `meetup_incremental_etl`
El DAG definido en [`dags/meetup_incremental_etl.py`](./dags/meetup_incremental_etl.py) usa el cron `*/15 * * * *`, `catchup=False` y una cadena secuencial de tres tareas:

1. **`simulate_new_data`** genera actividad demostrativa en Snowflake: incrementa aleatoriamente RSVP de algunos eventos e inserta un evento sintético asociado a un grupo.
2. **`refresh_staging_events`** transforma `RAW_DATA.EVENTS` y aplica un `MERGE` en `STAGING.EVENTS_CLEAN`: inserta eventos nuevos y actualiza los que tienen un `updated_at` más reciente.
3. **`refresh_aux_table`** vuelve a calcular `AUX.GROUPS_BY_CITY_CATEGORY` usando `CREATE OR REPLACE TABLE` y las tablas de staging.

La ejecución se probó en Snowflake con el rol `ETL_ROLE`; las tres tareas terminaron en estado `success`. Una ejecución confirmó el evento adicional en `EVENTS_CLEAN` y que sus identificadores seguían siendo únicos.

> **Importante sobre los datos:** `simulate_new_data` modifica directamente `RAPPI_MEETUP_TEST.RAW_DATA.EVENTS` e inserta una fila sintética nueva en cada ejecución exitosa. Esta simulación sirve para demostrar cambios periódicos, pero hace que RAW deje de ser una copia inmutable del dataset de Kaggle. El DAG puede pausarse desde Airflow si se quiere detener la simulación. No ejecutes la tarea repetidamente en un entorno donde RAW deba permanecer intacto.

### Alertas en Slack
El callback `on_success_callback` usa el proveedor oficial `apache-airflow-providers-slack` y `SlackWebhookOperator`. Está definido en los argumentos por defecto de las tareas, por lo que envía una notificación por cada tarea exitosa (tres mensajes por una ejecución completa), con DAG, tarea y fecha de ejecución. La entrega fue comprobada en el canal configurado. El DAG no define actualmente un callback de fallo; los fallos se consultan en Airflow.

### Ejecución de Airflow
Desde la raíz del proyecto, asegúrate de tener Docker Desktop iniciado. Configura el `.env` local con los proveedores requeridos y con la ruta absoluta, en el host, a la clave privada RSA de solo lectura utilizada por el usuario de servicio:

```dotenv
AIRFLOW_UID=<UID del usuario local>
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-snowflake apache-airflow-providers-slack
AIRFLOW_SNOWFLAKE_KEY_PATH=<ruta absoluta a rsa_key.p8>
```

En macOS, por ejemplo, `AIRFLOW_SNOWFLAKE_KEY_PATH=/Users/<usuario>/.snowflake/keys/rsa_key.p8`. La clave privada debe corresponder a una clave pública registrada en Snowflake para `SVC_ETL`; no la guardes en el repositorio. Compose la monta como solo lectura en `/opt/airflow/keys/svc_etl_mac.p8`.

Inicia los servicios:

```bash
docker compose up -d
```

Abre `http://localhost:8080` (usuario y contraseña por defecto `airflow`; solo para desarrollo local). En **Admin → Connections**, configura:

1. La conexión `snowflake_default` de tipo **Snowflake**: login `SVC_ETL`, esquema `RAW_DATA`, sin contraseña y los siguientes valores en **Extra**:

   ```json
   {
     "account": "KPMVEXI-LE96351",
     "warehouse": "SNOWFLAKE_LEARNING_WH",
     "database": "RAPPI_MEETUP_TEST",
     "role": "ETL_ROLE",
     "authenticator": "SNOWFLAKE_JWT",
     "private_key_file": "/opt/airflow/keys/svc_etl_mac.p8"
   }
   ```

2. La conexión `slack_connection` de tipo **Slack Incoming Webhook**, con el token/ruta parcial del webhook almacenado en el campo correspondiente de la conexión. Mantén el webhook privado; no lo incluyas en `.env` versionado, el DAG ni capturas públicas.

Antes de habilitar el DAG, valida la conexión Snowflake con una consulta de solo lectura desde Airflow y confirma el contexto `SVC_ETL`, `ETL_ROLE`, `RAPPI_MEETUP_TEST`, `RAW_DATA` y `SNOWFLAKE_LEARNING_WH`. En la interfaz de Airflow, despausa `meetup_incremental_etl` para permitir las ejecuciones cada 15 minutos; pausa el DAG para detenerlas.

Una ejecución local validada completó `simulate_new_data`, `refresh_staging_events` y `refresh_aux_table`; los mensajes correspondientes llegaron a Slack. Conserva capturas de la vista de ejecuciones y del canal como evidencia de la prueba.

## Estructura

```text
sql/       Scripts de stage, carga RAW, reparación y tablas auxiliares
scripts/   Subida de CSV, ejecución SQL y logging
tests/     Pruebas unitarias locales
data/      CSV descargados (no versionados)
logs/      Logs de ejecución (no versionados)
dags/      DAGs de Apache Airflow para orquestación incremental
```

## Próximas etapas

1. Exportar resultados a S3 mediante una integración de almacenamiento segura.
2. Adjuntar evidencias de ejecución y completar la entrega.
