# Descripción del proceso de la Lambda Transversal

## Input de Payload Builder Transversal

La lambda recibe a través del evento los siguientes parámetros:

* **project_name**: nombre del proyecto que ejecuta el job.
* **date_type**: corresponde al tipo de fecha, `MONTH_PRECISION` o `DAY_PRECISION`.
* **process_type**: es el tipo de proceso, `FULL` para cargas históricas e `INC` para cargas incrementales.
* **job_type**: es el tipo de job a ejecutar. Valores permitidos:

  * `raw`: para ejecutar solo el job de la etapa raw
  * `staging`: para ejecutar solo el job de la etapa staging
  * `raw-to-staging`: para ejecutar primero el job de la etapa raw y después el job de la etapa staging
* **job_parameters**: es un diccionario que incluye los parámetros generales para el job de la etapa raw y el job de la etapa staging.
* **secret_db**: es el secreto para conectarse a la base de datos en donde se encuentra el SP o la tabla.
* **db_type**: es el tipo de base de datos de origen, puede ser `sqlserver`, `mysql` o `postgresql`.
* **datalake_name**: es el nombre del bucket de S3 donde se almacenará la información.
* **notification_emails**: correo electrónico de la persona a la que llegarían las notificaciones en caso de presentarse un error durante la ejecución del job correspondiente.

Una vez recibidos estos parámetros se valida internamente qué tipo de proceso es (FULL o INC) y la precisión (DAY_PRECISION o MONTH_PRECISION):

### FULL - DAY_PRECISION

Se toman los parámetros `fecha_inicial` y `fecha_final` de `job_parameters` para realizar la carga iterando desde la fecha inicio hasta la fecha fin.

### FULL - MONTH_PRECISION

Calcula el primer día del mes de `fecha_inicial` y el último día del mes de `fecha_final` de los `job_parameters`.

### INC - DAY_PRECISION

Calcula la fecha anterior a la ejecución (t-1) y se realiza la carga de esta fecha.

### INC - MONTH_PRECISION

Calcula el primer y último día del mes anterior a la fecha de ejecución y se realiza la carga de este rango.

Posteriormente valida el tipo de job, y crea el payload de acuerdo al valor de entrada `raw`, `staging` o `raw-to-staging`.

---

# Output de Payload Builder Transversal

Finalmente, la función retorna los siguientes payloads:

## Para el job de la etapa raw

* **project_name**: es `project_name`.
* **name**: es el nombre de la query o el stored procedure a ejecutar.
* **arn_secret_db**: es `secret_db`.
* **db_type**: es `db_type`.
* **bucket**: es el `datalake_name` agregando el stage.
* **partition_key_raw**: nombre de la columna cuyo valor se usa para crear las particiones (por día o por mes) en función del `date_type` en la stage de raw.
* **file_name**: nombre del archivo en donde se guardará la información.
* **path_name_input**: es una ruta que se usa en:

  * bucket de raw: es la ruta en donde se van a cargar los resultados del job de la etapa raw.
  * bucket de parameters: es la ruta en donde se encuentran los archivos `.sql` con las consultas y archivos `.json` con los esquemas.
* **params**: es un diccionario de diccionarios que contiene los parámetros de la consulta o del SP a ejecutar.
* **notification_emails**: es `notification_emails`.

## Para el job de la etapa staging

* **project_name**: es `project_name`.
* **bucket_name_raw**: es el `datalake_name` agregando el stage raw.
* **path_name_raw**: es `path_name_input`, donde están las cargas de raw.
* **bucket_name_staging**: es el `datalake_name` agregando el stage staging.
* **path_name_staging**: es el prefijo que corresponde al `path_name_output` que se envía desde el diccionario `job_parameters`.
* **catalog_database**: es el nombre de la base de datos que se envía desde el diccionario `job_parameters`.
* **catalog_table**: es el nombre de la tabla que se envía desde el diccionario `job_parameters`.
* **merge_keys**: es la llave o llaves únicas para realizar el merge (generalmente la llave primaria) que se envía desde el diccionario `job_parameters`.
* **partition_key_staging**: es la llave o llaves de partición que se envía desde el diccionario `job_parameters`.
* **start_date**: es `curr_start_date`.
* **end_date**: es `curr_end_date`.
* **partition_key_raw**: nombre de la columna cuyo valor se usa para crear las particiones (por día o por mes) en función del `date_type` en la stage de raw.
* **notification_emails**: es `notification_emails`.

