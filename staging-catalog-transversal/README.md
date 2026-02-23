**Markdown** para pegarlo directamente en el `README.md` de tu repo en **GitHub**:

---

# Job Transversal – Staging Catalog

##  Descripción general

El **job-analytics-transversal-staging-catalog-v2** es un job transversal desarrollado en **AWS Glue** cuyo objetivo es catalogar de forma automatizada los datasets generados en la capa **Staging** dentro del Data Lake de la organización.

Este job es reutilizable por diferentes equipos o pipelines, ya que no está limitado a un dominio de negocio específico. Actúa de manera transversal sobre cualquier conjunto de datos que requiera ser catalogado para su posterior consumo analítico.

---

##  Objetivo principal

Garantizar que todos los datasets ubicados en la capa **Staging** sean visibles y accesibles desde el **Glue Data Catalog**, permitiendo:

* Consultas mediante Athena u otros servicios de análisis.
* Trazabilidad y consistencia en la nomenclatura de las tablas.
* Estandarización del proceso de catalogación a través de un flujo único.

---

##  Responsabilidades del Job

* Leer parámetros de ejecución desde un JSON de entrada.
* Transformar la estructura de los datos según reglas definidas.
* Catalogar los datasets resultantes en Glue (base de datos y tabla correspondientes).
* Registrar logs de ejecución y errores.

---

#  Arquitectura General

El job sigue un patrón de arquitectura **modular y parametrizable**, lo que permite su ejecución con distintos contextos de entrada sin necesidad de modificar el código fuente.

### Componentes principales

| Componente             | Descripción                                                   |
| ---------------------- | ------------------------------------------------------------- |
| AWS Glue Job           | Motor principal de ejecución del proceso                      |
| Amazon S3 (Staging)    | Origen y destino de los datos a catalogar                     |
| AWS Glue Data Catalog  | Registro estructurado de las tablas y metadatos               |
| Amazon CloudWatch Logs | Monitoreo y trazabilidad de la ejecución                      |
| AWS IAM Roles          | Control de permisos para acceder a Glue, S3 y Secrets Manager |

---

#  Flujo General del Proceso

1. El job recibe un JSON de entrada con los parámetros necesarios.
2. Se conecta al origen (S3) para identificar los archivos a catalogar.
3. Realiza la transformación mínima requerida (ej. normalización de nombres o inferencia de schema).
4. Publica los metadatos en Glue Data Catalog.
5. Registra resultados o errores en CloudWatch Logs.

---

#  Parámetros Esperados

El job recibe un JSON con la siguiente estructura:

```json
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
```

---

#  Descripción de Parámetros

| Parámetro               | Descripción                                                               | Ejemplo                              |
| ----------------------- | ------------------------------------------------------------------------- | ------------------------------------ |
| `bucket_name_raw`       | Bucket de Raw que contiene los archivos fuente                            | s3-663154812980-datalake-dev-raw     |
| `path_name_raw`         | Ruta dentro del bucket Raw donde se encuentran los datos de entrada       | testbdc/bdc/rf-transacciones-cuenta  |
| `bucket_name_staging`   | Bucket de Staging donde se guardarán los archivos procesados              | s3-663154812980-datalake-dev-staging |
| `path_name_staging`     | Ruta dentro del bucket Staging donde se guardarán los archivos procesados | bdc/tblbdctest                       |
| `catalog_database`      | Base de datos en el Glue Catalog asociada a la tabla                      | database_glue_fuentes_bdc            |
| `catalog_table`         | Nombre de la tabla en el Glue Catalog donde se registrará la metadata     | tblbdctest                           |
| `merge_keys`            | Campo o lista de campos usados como llave para realizar el merge          | int_id_moneda,fecha_corte            |
| `partition_key_staging` | Columna usada para crear particiones en Staging                           | fecha_liquidacion                    |
| `partition_key_raw`     | Columna usada para crear particiones en Raw                               | FechaLiquidacion                     |
| `process_type`          | Tipo de proceso: FULL (histórico) o INC (incremental)                     | FULL                                 |
| `date_type`             | Tipo de fecha: MONTH_PRECISION o DAY_PRECISION                            | DAY_PRECISION                        |
| `start_date`            | Fecha inicio (yyyy-MM o yyyy-MM-dd) según `date_type`                     | 2025-01-01                           |
| `end_date`              | Fecha fin (yyyy-MM o yyyy-MM-dd) según `date_type`                        | 2025-01-03                           |

---

#  Tipos de Ejecución

## FULL

Carga histórica completa dentro del rango de fechas indicado.

## INC

Carga incremental basada en particiones nuevas o rango de fechas específico.

---

# Beneficios del Diseño

* Arquitectura reutilizable y desacoplada.
* Parametrización completa vía JSON.
* Estandarización de catalogación.
* Escalable a múltiples dominios.
* Observabilidad mediante logs centralizados.

---

