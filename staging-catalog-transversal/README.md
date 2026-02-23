l

Job Transversal – Staging Catalog

Descripción general:
El job-analytics-transversal-staging-catalog-v2 es un job transversal desarrollado en AWS Glue que tiene como objetivo catalogar de forma automatizada los datasets generados en la capa de staging dentro del Data Lake de la organización.
Este job es reutilizable por diferentes equipos o pipelines, ya que no está limitado a un dominio de negocio específico, sino que actúa de manera transversal sobre cualquier conjunto de datos que requiera ser catalogado para su posterior consumo analítico.

Objetivo principal:
Garantizar que todos los datasets ubicados en la capa staging sean visibles y accesibles desde el catálogo de datos (Glue Data Catalog), permitiendo:

Consultas mediante Athena u otros servicios de análisis.

Trazabilidad y consistencia en la nomenclatura de las tablas.

Estandarización del proceso de catalogación a través de un flujo único.

Responsabilidades del job:

Leer parámetros de ejecución desde un JSON de entrada.

Transformar la estructura de los datos según las reglas definidas.

Catalogar los datasets resultantes en Glue (base de datos y tabla correspondientes).

Registrar logs de ejecución y errores.




Arquitectura general

El job sigue un patrón de arquitectura modular y parametrizable, lo que permite su ejecución con distintos contextos de entrada sin necesidad de modificar el código fuente.
Se apoya en los siguientes componentes principales:

Componente

Descripción

AWS Glue Job

Motor principal de ejecución del proceso.

Amazon S3 (capa staging)

Origen y destino de los datos a catalogar.

AWS Glue Data Catalog

Registro estructurado de las tablas y metadatos.

Amazon CloudWatch Logs

Monitoreo y trazabilidad de la ejecución.

AWS IAM Roles

Control de permisos para acceder a Glue, S3 y Secrets Manager.

 Flujo general del proceso

El job recibe un JSON de entrada con los parámetros necesarios.

Se conecta al origen (S3) para identificar los archivos a catalogar.

Realiza la transformación mínima requerida (por ejemplo, normalización de nombres o inferencia de schema).

Publica los metadatos en Glue Data Catalog.

Registra la salida o errores en CloudWatch Logs.

Parámetros esperados

El job recibe un JSON de entrada con la siguiente estructura:

{
    "bucket_name_raw": "s3-663154812980-datalake-dev-raw",
    "path_name_raw": "testbdc/bdc/rf-transacciones-cuenta",
    "bucket_name_staging": "s3-663154812980-datalake-dev-staging",
    "path_name_staging": "bdc/tblbdctest",
    "catalog_database": "database_glue_fuentes_bdc",
    "catalog_table": "tblbdctest",
    "merge_keys": "None",
    "partition_key_staging": "fecha_liquidacion",
    "partition_key_raw": "FechaLiquidacion",
    "process_type": "FULL",
    "date_type": "DAY_PRECISION",
    "start_date": "2025-01-01",
    "end_date": "2025-01-03"
}

Descripción de parámetros

Parámetro

Descripción

Ejemplo

--bucket_name_raw

Bucket de Raw que contiene los archivos fuente.

s3-663154812980-datalake-dev-raw

--path_name_raw

Ruta dentro del bucket Raw donde se encuentran los datos de entrada.

INC, FULL

--bucket_name_staging

Bucket de Staging donde se guardarán los archivos procesados.

2025-01

--path_name_staging

Ruta dentro del bucket Staging donde se guardarán los archivos procesados.

2025-02

--catalog_database

Base de datos en el Glue Catalog asociada a la tabla.

s3-663154812980-datalake-dev-raw

--catalog_table

Nombre de la tabla en el Glue Catalog donde se registrará la metadata procesada.

tblmonedas

--merge_keys

Campo o lista de campos usados como llave para realizar el merge o actualización de registros.

int_id_moneda, None, int_id_moneda,fecha_corte

--partition_key_staging

Nombre de la columna cuyo valor se usa para crear las particiones (por día o por mes) en la stage de Staging

add_fecha_carga

--partition_key_raw

Nombre de la columna cuyo valor se usa para crear las particiones (por día o por mes) en la stage de Raw

fecha_corte

--process_type

Es el tipo de proceso, FULL para cargas históricas e INC para cargas incrementales.

FULL, INC

--date_type

Corresponde al tipo de fecha, MONTH_PRECISION o DAY_PRECISION.

MONTH_PRECISION, DAY_PRECISION.

--start_date

Es la fecha inicio para la ejecución, puede enviarse yyyy-MM para MONTH_PRECISION o yyyy-MM-dd para DAY_PRECISION.

2025-12, 2025-12-01

--end_date

Es la fecha fin para la ejecución puede enviarse yyyy-MM para MONTH_PRECISION o yyyy-MM-dd para DAY_PRECISION.

2025-12, 2025-12-01

