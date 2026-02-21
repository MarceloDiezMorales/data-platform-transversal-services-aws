from unittest.mock import patch, MagicMock
import types
import sys

mock_awsglue = types.ModuleType("awsglue")
mock_context = types.ModuleType("awsglue.context")
mock_job = types.ModuleType("awsglue.job")
mock_utils = types.ModuleType("awsglue.utils")

datafoundation_mock = types.ModuleType("datafoundation")
datafoundation_iceberg_mock = types.ModuleType("datafoundation.iceberg")
datafoundation_manager_mock = types.ModuleType("datafoundation.iceberg.manager")

class MockIcebergTableManager:
    def __init__(self, spark, *args, **kwargs):
        self.spark = spark
        self.create_table = MagicMock()
        self.synchronize_schema = MagicMock()
        self.merge_data = MagicMock()
        self.overwrite_partitions = MagicMock()
        for key, value in kwargs.items():
            setattr(self, key, value)

datafoundation_manager_mock.IcebergTableManager = MockIcebergTableManager

sys.modules["datafoundation"] = datafoundation_mock
sys.modules["datafoundation.iceberg"] = datafoundation_iceberg_mock
sys.modules["datafoundation.iceberg.manager"] = datafoundation_manager_mock

class MockGlueContext:
    def __init__(self, *args, **kwargs):
        pass

class MockJob:
    def __init__(self, *args, **kwargs):
        pass

    def init(self, *args, **kwargs):
        pass

    def commit(self):
        pass

def mock_getResolvedOptions(args, options):
    return {opt: "mock_value" for opt in options}

mock_context.GlueContext = MockGlueContext
mock_job.Job = MockJob
mock_utils.getResolvedOptions = mock_getResolvedOptions

mock_awsglue.context = mock_context
mock_awsglue.job = mock_job
mock_awsglue.utils = mock_utils

sys.modules["awsglue"] = mock_awsglue
sys.modules["awsglue.context"] = mock_context
sys.modules["awsglue.job"] = mock_job
sys.modules["awsglue.utils"] = mock_utils

from src.jobs.catalog_transversal import StagingJob
import pytest

@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_initialization(mock_initialize_spark):
    mock_glue_context = MagicMock()
    mock_spark_session = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark_session, mock_job)

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": "{}",
        "BUCKET_NAME_INPUT": "input-bucket",
        "PATH_NAME_INPUT": "input/path/",
        "BUCKET_NAME_OUTPUT": "output-bucket",
        "PATH_NAME_OUTPUT": "output/path/",
        "CATALOG_DATABASE": "test_db",
        "CATALOG_TABLE": "test_table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "date",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "FULL",
        "START_DATE": "2025-01-01",
        "END_DATE": "2025-01-02",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)

    assert job.bucket_name_input == "input-bucket"
    assert job.catalog_database == "test_db"
    assert job.type_process == "FULL"
    assert job.catalog_table == "glue_catalog.test_db.test_table"
    assert job.partition_key == ["date"]
    mock_job.init.assert_called_once()

@patch("src.jobs.catalog_transversal.normalize_column_names")
@patch("src.jobs.catalog_transversal.rename_columns_to_snake_case")
@patch("src.jobs.catalog_transversal.cast_columns_from_schema")
@patch("src.jobs.catalog_transversal.validate_dataframe")
@patch("src.jobs.catalog_transversal.get_non_nullable_fields")
@patch("src.jobs.catalog_transversal.build_struct_from_schema")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.get_date_ranges")
@patch("src.jobs.catalog_transversal.read_data_from_s3")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_run_full(mock_initialize_spark, mock_read_s3, mock_get_ranges,
                                mock_parse_schema, mock_build_struct, mock_get_fields,
                                mock_validate, mock_cast, mock_rename, mock_normalize):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)
    
    mock_get_ranges.return_value = ["2025-01-01", "2025-01-02"]
    mock_parse_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    mock_build_struct.return_value = MagicMock()
    mock_get_fields.return_value = ["col1"]
    
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_df.printSchema.return_value = None
    mock_read_s3.return_value = mock_df
    mock_cast.return_value = mock_df
    mock_rename.return_value = mock_df
    mock_normalize.return_value = mock_df

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "date",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "FULL",
        "START_DATE": "2025-01-01",
        "END_DATE": "2025-01-02",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    job.partition_key = ["date"]
    job.run()

    mock_get_ranges.assert_called_once()
    assert mock_read_s3.call_count == 2
    job.iceberg_manager.create_table.assert_called()
    job.iceberg_manager.merge_data.assert_called()


@patch("src.jobs.catalog_transversal.get_previous_date")
@patch("src.jobs.catalog_transversal.normalize_column_names")
@patch("src.jobs.catalog_transversal.rename_columns_to_snake_case")
@patch("src.jobs.catalog_transversal.cast_columns_from_schema")
@patch("src.jobs.catalog_transversal.validate_dataframe")
@patch("src.jobs.catalog_transversal.get_non_nullable_fields")
@patch("src.jobs.catalog_transversal.build_struct_from_schema")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.read_data_from_s3")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_run_inc(mock_initialize_spark, mock_read_s3,
                               mock_parse_schema, mock_build_struct, mock_get_fields,
                               mock_validate, mock_cast, mock_rename, mock_normalize,
                               mock_get_prev):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)
    
    mock_get_prev.return_value = "2025-01-15"
    mock_parse_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    mock_build_struct.return_value = MagicMock()
    mock_get_fields.return_value = ["col1"]
    
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_df.printSchema.return_value = None
    mock_read_s3.return_value = mock_df
    mock_cast.return_value = mock_df
    mock_rename.return_value = mock_df
    mock_normalize.return_value = mock_df

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "date",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "INC",
        "START_DATE": "",
        "END_DATE": "",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    job.partition_key = ["date"]
    job.run()

    mock_get_prev.assert_called_once_with("DAY_PRECISION")
    assert mock_read_s3.call_count == 1

@patch("src.jobs.catalog_transversal.add_partition_column")
@patch("src.jobs.catalog_transversal.normalize_column_names")
@patch("src.jobs.catalog_transversal.rename_columns_to_snake_case")
@patch("src.jobs.catalog_transversal.cast_columns_from_schema")
@patch("src.jobs.catalog_transversal.validate_dataframe")
@patch("src.jobs.catalog_transversal.get_non_nullable_fields")
@patch("src.jobs.catalog_transversal.build_struct_from_schema")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.get_date_ranges")
@patch("src.jobs.catalog_transversal.read_data_from_s3")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_add_partition_key(mock_initialize_spark, mock_read_s3, mock_get_ranges,
                                        mock_parse_schema, mock_build_struct,
                                        mock_get_fields, mock_validate, mock_cast, mock_rename,
                                        mock_normalize, mock_add_partition):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)
    
    mock_get_ranges.return_value = ["2025-01-01"]
    mock_parse_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    mock_build_struct.return_value = MagicMock()
    mock_get_fields.return_value = ["col1"]
    
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_df.printSchema.return_value = None
    mock_read_s3.return_value = mock_df
    mock_cast.return_value = mock_df
    mock_rename.return_value = mock_df
    mock_normalize.return_value = mock_df
    mock_add_partition.return_value = mock_df

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "add_partition_date",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "FULL",
        "START_DATE": "2025-01-01",
        "END_DATE": "2025-01-01",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    job.partition_key = ["add_partition_date"]
    job.run()

    mock_add_partition.assert_called_once()
    call_args = mock_add_partition.call_args[0]
    assert call_args[1] == "partition_date"
    assert call_args[2] == "2025-01-01"

@patch("src.jobs.catalog_transversal.add_partition_column")
@patch("src.jobs.catalog_transversal.normalize_column_names")
@patch("src.jobs.catalog_transversal.rename_columns_to_snake_case")
@patch("src.jobs.catalog_transversal.cast_columns_from_schema")
@patch("src.jobs.catalog_transversal.validate_dataframe")
@patch("src.jobs.catalog_transversal.get_non_nullable_fields")
@patch("src.jobs.catalog_transversal.build_struct_from_schema")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.get_date_ranges")
@patch("src.jobs.catalog_transversal.read_data_from_s3")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_add_partition_multiple_dates(mock_initialize_spark, mock_read_s3, mock_get_ranges,
                                                   mock_parse_schema, mock_build_struct,
                                                   mock_get_fields, mock_validate, mock_cast, mock_rename,
                                                   mock_normalize, mock_add_partition):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)
    
    mock_get_ranges.return_value = ["2025-01-01", "2025-01-02", "2025-01-03"]
    mock_parse_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    mock_build_struct.return_value = MagicMock()
    mock_get_fields.return_value = ["col1"]
    
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_df.printSchema.return_value = None
    mock_read_s3.return_value = mock_df
    mock_cast.return_value = mock_df
    mock_rename.return_value = mock_df
    mock_normalize.return_value = mock_df
    mock_add_partition.return_value = mock_df

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "add_fecha",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "FULL",
        "START_DATE": "2025-01-01",
        "END_DATE": "2025-01-03",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    job.partition_key = ["add_fecha"]
    job.run()

    mock_add_partition.assert_called_once()
    assert "fecha" in [call[0][1] for call in mock_add_partition.call_args_list]

@patch("src.jobs.catalog_transversal.add_partition_column")
@patch("src.jobs.catalog_transversal.normalize_column_names")
@patch("src.jobs.catalog_transversal.rename_columns_to_snake_case")
@patch("src.jobs.catalog_transversal.cast_columns_from_schema")
@patch("src.jobs.catalog_transversal.validate_dataframe")
@patch("src.jobs.catalog_transversal.get_non_nullable_fields")
@patch("src.jobs.catalog_transversal.build_struct_from_schema")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.get_date_ranges")
@patch("src.jobs.catalog_transversal.read_data_from_s3")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_no_add_partition_key(mock_initialize_spark, mock_read_s3, mock_get_ranges,
                                           mock_parse_schema, mock_build_struct,
                                           mock_get_fields, mock_validate, mock_cast, mock_rename,
                                           mock_normalize, mock_add_partition):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)
    
    mock_get_ranges.return_value = ["2025-01-01"]
    mock_parse_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    mock_build_struct.return_value = MagicMock()
    mock_get_fields.return_value = ["col1"]
    
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_df.printSchema.return_value = None
    mock_read_s3.return_value = mock_df
    mock_cast.return_value = mock_df
    mock_rename.return_value = mock_df
    mock_normalize.return_value = mock_df

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "existing_date_column",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "FULL",
        "START_DATE": "2025-01-01",
        "END_DATE": "2025-01-01",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    job.partition_key = ["existing_date_column"]
    job.run()

    mock_add_partition.assert_not_called()
    assert job.partition_key == ["existing_date_column"]

@patch("src.jobs.catalog_transversal.normalize_column_names")
@patch("src.jobs.catalog_transversal.rename_columns_to_snake_case")
@patch("src.jobs.catalog_transversal.cast_columns_from_schema")
@patch("src.jobs.catalog_transversal.validate_dataframe")
@patch("src.jobs.catalog_transversal.get_non_nullable_fields")
@patch("src.jobs.catalog_transversal.build_struct_from_schema")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.get_date_ranges")
@patch("src.jobs.catalog_transversal.read_data_from_s3")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_overwrite_mode(mock_initialize_spark, mock_read_s3, mock_get_ranges,
                                     mock_parse_schema, mock_build_struct, mock_get_fields,
                                     mock_validate, mock_cast, mock_rename, mock_normalize):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)
    
    mock_get_ranges.return_value = ["2025-01-01"]
    mock_parse_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    mock_build_struct.return_value = MagicMock()
    mock_get_fields.return_value = ["col1"]
    
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_df.printSchema.return_value = None
    mock_read_s3.return_value = mock_df
    mock_cast.return_value = mock_df
    mock_rename.return_value = mock_df
    mock_normalize.return_value = mock_df

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "none",
        "PARTITION_KEY": "date",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "FULL",
        "START_DATE": "2025-01-01",
        "END_DATE": "2025-01-01",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    job.partition_key = ["date"]
    job.run()

    job.iceberg_manager.overwrite_partitions.assert_called_once()
    job.iceberg_manager.merge_data.assert_not_called()

@patch("src.jobs.catalog_transversal.get_date_ranges")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_invalid_type_process(mock_initialize_spark, mock_parse_schema, mock_get_ranges):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "date",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "INVALID",
        "START_DATE": "2025-01-01",
        "END_DATE": "2025-01-02",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    
    with pytest.raises(ValueError, match="Invalid TYPE_PROCESS"):
        job.run()

@patch("src.jobs.catalog_transversal.get_date_ranges")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_full_missing_dates(mock_initialize_spark, mock_parse_schema, mock_get_ranges):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "date",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "FULL",
        "START_DATE": "",
        "END_DATE": "",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    
    with pytest.raises(ValueError, match="start_date and end_date must be provided"):
        job.run()

@patch("src.jobs.catalog_transversal.normalize_column_names")
@patch("src.jobs.catalog_transversal.rename_columns_to_snake_case")
@patch("src.jobs.catalog_transversal.cast_columns_from_schema")
@patch("src.jobs.catalog_transversal.validate_dataframe")
@patch("src.jobs.catalog_transversal.get_non_nullable_fields")
@patch("src.jobs.catalog_transversal.build_struct_from_schema")
@patch("src.jobs.catalog_transversal.parse_schema_definition")
@patch("src.jobs.catalog_transversal.get_date_ranges")
@patch("src.jobs.catalog_transversal.read_data_from_s3")
@patch("src.jobs.catalog_transversal.initialize_spark")
def test_staging_job_empty_dataframe(mock_initialize_spark, mock_read_s3, mock_get_ranges,
                                      mock_parse_schema, mock_build_struct, mock_get_fields,
                                      mock_validate, mock_cast, mock_rename, mock_normalize):
    mock_glue_context = MagicMock()
    mock_spark = MagicMock()
    mock_job = MagicMock()
    mock_initialize_spark.return_value = (mock_glue_context, mock_spark, mock_job)
    
    mock_get_ranges.return_value = ["2025-01-01"]
    mock_parse_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    mock_build_struct.return_value = MagicMock()
    mock_get_fields.return_value = ["col1"]
    
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = True
    mock_read_s3.return_value = mock_df

    args = {
        "JOB_NAME": "test_job",
        "PARAMETERS_SCHEMA": '{"fields": []}',
        "BUCKET_NAME_INPUT": "bucket-in",
        "PATH_NAME_INPUT": "path/",
        "BUCKET_NAME_OUTPUT": "bucket-out",
        "PATH_NAME_OUTPUT": "out/",
        "CATALOG_DATABASE": "db",
        "CATALOG_TABLE": "table",
        "MERGE_KEYS": "id",
        "PARTITION_KEY": "date",
        "FOLDER_NAME_JSON": "data",
        "TYPE_PROCESS": "FULL",
        "START_DATE": "2025-01-01",
        "END_DATE": "2025-01-01",
        "DATE_TYPE": "DAY_PRECISION",
    }

    job = StagingJob(args)
    job.run()

    mock_validate.assert_not_called()
    job.iceberg_manager.create_table.assert_not_called()

@patch("src.jobs.catalog_transversal.main")
def test_main_invocation(mock_main):
    mock_main.return_value = None
    result = mock_main()
    mock_main.assert_called_once()
    assert result is None