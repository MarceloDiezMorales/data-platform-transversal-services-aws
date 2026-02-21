"""
Module that contains the staging glue job for loading data into an Iceberg table.
"""
import sys

from awsglue.utils import getResolvedOptions

from datafoundation.iceberg.manager import IcebergTableManager
from datafoundation.observability.emitter import JobRunEventEmitter

from src.config.decorators import log_decorator, raise_decorator
from src.config.logger import logger
from src.config.spark_setup import initialize_spark
from src.utils.constants import REQUIRED_STAGING_ARGS
from src.utils.dates import get_previous_date, get_date_ranges, get_today_frozen_date
from src.utils.schemas import build_struct_from_schema, get_non_nullable_fields, get_schema_definition
from src.utils.validations import validate_dataframe
from src.resources.lambda_invoke import LambdaClient
from src.resources.s3_v2 import read_data_from_s3
from src.transformations.clean import (
    rename_columns_to_snake_case,
    parse_list_param,
    cast_columns_from_schema,
    add_partition_column,
    normalize_column_names
)

class StagingJob:
    """
    Class to handle the staging process for loading data into an Iceberg table.
    """
    def __init__(self, args):
        """
        Initialize job configuration parameters and Spark context.

        Args:
            args (dict): Dictionary with Glue job arguments.
        """
        logger.info("# [INFO] Initializing job parameters")
        self.job_name = args["job_name"]
        self.region = args["region"]
        self.project_name = args["project_name"]
        self.bucket_name_raw = args["bucket_name_raw"]
        self.path_name_raw = args["path_name_raw"]

        self.key_raw = self.path_name_raw.strip('/')
        self.table_raw_path = f"s3://{self.bucket_name_raw}/{self.key_raw}"
        self.bucket_name_schema = args["bucket_name_schema"]
        self.bucket_name_staging = args["bucket_name_staging"]
        self.excel_sheet = args.get("excel_sheet", "").strip() or None

        self.path_name_staging =  args["path_name_staging"]
        self.key_staging = self.path_name_staging.strip('/')
        self.table_staging_path = f"s3://{self.bucket_name_staging}/{self.key_staging}"

        self.catalog_database = args["catalog_database"]
        self.catalog_table_name = args["catalog_table"]
        self.catalog_table = f"glue_catalog.{self.catalog_database}.{self.catalog_table_name}"
        self.merge_keys_str = args["merge_keys"]
        self.partition_key_staging = [x.strip() for x in args["partition_key_staging"].split(',') if x.strip()]

        self.partition_key_raw = args["partition_key_raw"]
        self.partition_key_raw_path = args.get("partition_key_raw_path", "").strip() or None
        self.file_name_excel = args.get("file_name_excel", "").strip() or None
        self.process_type = args["process_type"]
        self.date_type = args["date_type"]
        self.start_date = args["start_date"]
        self.end_date = args["end_date"]

        self.notification_emails = args["notification_emails"]

        self.notifications_transversal_lambda_name = args["notifications_transversal_lambda_name"]
        
        self.glue_context, self.spark, self.job = initialize_spark(self.key_staging)
        self.iceberg_manager = IcebergTableManager(self.spark)
        self.job.init(args["job_name"], args)
        logger.info("[INFO] Spark session initialized successfully")


    def run(self):
        """
        Execute the ETL workflow for FULL or INC process types.

        Steps:
            1. Determine partitions to process.
            2. Read and clean source data.
            3. Validate and cast DataFrame.
            4. Create or synchronize Iceberg table.
            5. Merge data into target Iceberg table.
        """
        logger.info(f"# [START] Running process type ? {self.process_type}")

        # --- [1] Preparar listas de particiones ---
        file_name_excel = self.file_name_excel
        use_single_file = bool(file_name_excel)
        if use_single_file:
            partitions = [file_name_excel]
        elif self.partition_key_raw.lower().startswith('add_'):
            if self.process_type not in ["FULL", "INC"]:
                raise ValueError(f"Invalid process_type: {self.process_type}")
            self.partition_key_raw = self.partition_key_raw[4:]
            if self.date_type == "DAY_PRECISION":
                partitions = [get_today_frozen_date().strftime('%Y-%m-%d')]
            elif self.date_type == "MONTH_PRECISION":
                partitions = [get_today_frozen_date().strftime('%Y-%m')]
            else:
                raise ValueError(f"Invalid date_type: {self.date_type}")
        elif self.process_type == "FULL":
            if not (self.start_date and self.end_date):
                raise ValueError("start_date and end_date must be provided for FULL process_type")
            partitions = get_date_ranges(self.start_date, self.end_date, self.date_type) 
        elif self.process_type == "INC":
            partitions = [get_previous_date(self.date_type)]
        else:
            raise ValueError(f"Invalid process_type: {self.process_type}")
        if not self.partition_key_raw_path and not use_single_file:
            self.partition_key_raw_path = self.partition_key_raw
        logger.info(f"[INFO] Processing {self.process_type} load for partitions: {partitions}")
            
        # --- [2] Cargar esquema una sola vez ---
        schema_fields = get_schema_definition(self.bucket_name_schema, self.key_raw, self.spark, excel_sheet=self.excel_sheet)
        df_schema = build_struct_from_schema(schema_fields)
        required_fields = get_non_nullable_fields(schema_fields)
        expects_entity = any(f.get("name", "").lower() == "entidad" for f in schema_fields)

        # --- [3] Procesar partition_key_staging una sola vez ---
        partition_columns_to_add = []
        cleaned_partition_keys = []
        for key in self.partition_key_staging:
            if key.startswith("add_"):
                cleaned_key = key.replace("add_", "")
                cleaned_partition_keys.append(cleaned_key)
                partition_columns_to_add.append(cleaned_key)
            else:
                cleaned_partition_keys.append(key)
        self.partition_key_staging = cleaned_partition_keys
        logger.info(f"[INFO] Partition keys for staging: {self.partition_key_staging}")
        logger.info(f"[INFO] Columns to add dynamically: {partition_columns_to_add}")

        use_overwrite = self.merge_keys_str.strip().lower() == "none"
        merge_keys = None if use_overwrite else parse_list_param(self.merge_keys_str)

        # --- [4] Crear tabla Iceberg una sola vez ---
        table_created = False

        # --- [5] Procesar cada partici�n ---
        empty_partitions = []
        for partition_date in partitions:
            if use_single_file:
                file_name_excel = self.file_name_excel.strip()
                if file_name_excel.startswith("s3://"):
                    partition_path = file_name_excel
                else:
                    key = file_name_excel.lstrip("/")
                    key_raw = self.key_raw.rstrip("/")
                    if key.startswith(f"{key_raw}/") or key == key_raw:
                        full_key = key
                    else:
                        full_key = f"{key_raw}/{key}"
                    partition_path = f"s3://{self.bucket_name_raw}/{full_key}"
            else:
                partition_path = f"{self.table_raw_path}/{self.partition_key_raw_path}={partition_date}"
            logger.info(f"[STEP 1] Processing partition: {partition_path}")

            df = read_data_from_s3(self.spark, partition_path, excel_sheet=self.excel_sheet, add_entity=expects_entity)
            
            if df.rdd.isEmpty():
                empty_partitions.append(partition_date)
                logger.warning(f"[WARN] No data found for partition {partition_date}. Skipping...")
                continue

            logger.info(f"[INFO] Data read successfully. Schema: {df.schema.simpleString()}")

            for col_name in partition_columns_to_add:
                df = add_partition_column(df, col_name, self.start_date)

            validate_dataframe(df, df_schema, required_fields, file_name_excel)
            df = cast_columns_from_schema(df, schema_fields, file_name_excel)
            df_snake = rename_columns_to_snake_case(df, schema_fields)
            df_snake = normalize_column_names(df_snake)

            # --- [6] Crear/verificar tabla Iceberg ---
            if not table_created:
                logger.info("[STEP 2] Creating Iceberg table (first partition)")
                self.iceberg_manager.create_table(self.table_staging_path, df_snake, self.partition_key_staging, self.catalog_table)
                table_created = True

            # --- [7] Sincronizar esquema ---
            logger.info("[STEP 3] Synchronizing Iceberg table schema")
            self.iceberg_manager.synchronize_schema(df_snake, self.catalog_table)
        
            # Evaluar si se debe hacer overwrite o merge
            if use_overwrite:
                logger.info(f"[INFO] Performing OVERWRITE for partition {partition_date}")
                self.iceberg_manager.overwrite_partitions(df_snake, self.catalog_table, self.table_staging_path)
            else:
                logger.info(f"[INFO] Performing MERGE for partition {partition_date} using keys: {merge_keys}")
                self.iceberg_manager.merge_data(df_snake, merge_keys, self.catalog_table)

        logger.info("[DONE] All partitions processed successfully.")
        logger.info("[INFO] Finished processing pipeline successfully")

        if empty_partitions:
            s3_lambda_client =  LambdaClient(self.notifications_transversal_lambda_name, self.region)
            message = "Warning: The following partitions had no data and were skipped: " + ", ".join(empty_partitions)
            payload = {
                'Payload': {
                    'JobStatus': 'WARNING', 
                    'ProcessType': self.process_type, 
                    'ProjectName': self.project_name,
                    'JobName': self.job_name,
                    'Arguments': {
                        '--catalog_table': self.catalog_table_name
                    },
                    'ErrorMessage': message, 
                    'NotificationEmails': self.notification_emails
                }
            }
            response = s3_lambda_client.invoke(payload, async_=False)
            logger.info(f"[INFO] Notification lambda invoked for empty partitions response: {response}")


@log_decorator
@raise_decorator
def main():
    """
    Entry point for the Glue job. Parses arguments, initializes the job, and triggers the processing pipeline.
    """
    logger.info("[INIT] Starting Staging job")

    try:
        def _get_optional_arg(name: str) -> str:
            flag = f"--{name}"
            if flag in sys.argv:
                idx = sys.argv.index(flag)
                if idx + 1 < len(sys.argv):
                    value = sys.argv[idx + 1].strip()
                    if not value or value == "-" or value.startswith("--"):
                        return ""
                    return value
            return ""

        args = getResolvedOptions(sys.argv, REQUIRED_STAGING_ARGS)
        args["excel_sheet"] = _get_optional_arg("excel_sheet")
        args["partition_key_raw_path"] = _get_optional_arg("partition_key_raw_path")
        args["file_name_excel"] = _get_optional_arg("file_name_excel")
    except Exception as e:
        logger.error(f"[ERROR] Error retrieving arguments: {e}")
        raise ValueError(f"Error retrieving arguments: {e}")
    
    emitter = JobRunEventEmitter(
        project_name=args["project_name"],
        job_name=args["job_name"],
        run_id=args["JOB_RUN_ID"],
        job_type="GLUE",
        environment=args["stage"].upper(),
        trigger_source="CRON",
        event_bus_name=args["event_bus_name"],
        input_parameters=args
    )
    
    job = StagingJob(args)

    try:
        job.run()
        emitter.emit("SUCCEEDED")
    except Exception as e:
        emitter.emit("FAILED", str(e))
        logger.error(f"[ERROR] Failure during job execution ? {str(e)}")
        raise

    logger.info("[SUCCESS] Staging job completed successfully")

if __name__ == "__main__":
    main()
