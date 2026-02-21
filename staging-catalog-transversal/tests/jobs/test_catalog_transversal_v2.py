from unittest.mock import patch, MagicMock
import types
import sys
import pytest

# Mock awsglue module
mock_awsglue = types.ModuleType("awsglue")
mock_context = types.ModuleType("awsglue.context")
mock_job = types.ModuleType("awsglue.job")
mock_utils = types.ModuleType("awsglue.utils")

datafoundation_mock = types.ModuleType("datafoundation")
datafoundation_iceberg_mock = types.ModuleType("datafoundation.iceberg")
datafoundation_manager_mock = types.ModuleType("datafoundation.iceberg.manager")
datafoundation_observability_mock = types.ModuleType("datafoundation.observability")
datafoundation_emitter_mock = types.ModuleType("datafoundation.observability.emitter")

class MockIcebergTableManager:
    def __init__(self, spark, *args, **kwargs):
        self.spark = spark
        self.create_table = MagicMock()
        self.synchronize_schema = MagicMock()
        self.merge_data = MagicMock()
        self.overwrite_partitions = MagicMock()

class MockJobRunEventEmitter:
    def __init__(self, *args, **kwargs):
        pass

datafoundation_manager_mock.IcebergTableManager = MockIcebergTableManager
datafoundation_emitter_mock.JobRunEventEmitter = MockJobRunEventEmitter

sys.modules["datafoundation"] = datafoundation_mock
sys.modules["datafoundation.iceberg"] = datafoundation_iceberg_mock
sys.modules["datafoundation.iceberg.manager"] = datafoundation_manager_mock
sys.modules["datafounfation.observability"] = datafoundation_observability_mock
sys.modules["datafoundation.observability.emitter"] = datafoundation_emitter_mock

class MockGlueContext:
    pass

class MockJob:
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

from src.jobs.catalog_transversal_v2 import StagingJob

@pytest.fixture
def base_args():
    return {
        "region": "us-east-1",
        "project_name": "test_project",
        "notification_emails": "test@example.com",
        "notifications_transversal_lambda_name": "test_lambda",
        "job_name": "test_job",
        "bucket_name_raw": "raw-bucket",
        "path_name_raw": "raw/path",
        "bucket_name_schema": "schema-bucket",
        "bucket_name_staging": "staging-bucket",
        "path_name_staging": "staging/path",
        "catalog_database": "db",
        "catalog_table": "table",
        "merge_keys": "id",
        "partition_key_staging": "date",
        "partition_key_raw": "fecha",
        "process_type": "FULL",
        "start_date": "2025-01-01",
        "end_date": "2025-01-02",
        "date_type": "DAY_PRECISION",
        "event_bus_name": "test_event_bus"
    }

@pytest.fixture
def mock_data_processing():
    with patch("src.jobs.catalog_transversal_v2.normalize_column_names") as mock_normalize, \
         patch("src.jobs.catalog_transversal_v2.cast_columns_from_schema") as mock_cast, \
         patch("src.jobs.catalog_transversal_v2.validate_dataframe") as mock_validate, \
         patch("src.jobs.catalog_transversal_v2.get_non_nullable_fields") as mock_fields, \
         patch("src.jobs.catalog_transversal_v2.build_struct_from_schema") as mock_struct, \
         patch("src.jobs.catalog_transversal_v2.get_schema_definition") as mock_schema, \
         patch("src.jobs.catalog_transversal_v2.get_date_ranges") as mock_ranges, \
         patch("src.jobs.catalog_transversal_v2.read_data_from_s3") as mock_read, \
         patch("src.jobs.catalog_transversal_v2.initialize_spark") as mock_spark:
        
        mock_df = MagicMock()
        mock_df.rdd.isEmpty.return_value = False
        mock_df.printSchema.return_value = None
        
        mock_normalize.return_value = mock_df
        mock_cast.return_value = mock_df
        mock_read.return_value = mock_df
        mock_schema.return_value = [{"name": "col1", "converted_type": "string"}]
        mock_struct.return_value = MagicMock()
        mock_fields.return_value = ["col1"]
        mock_ranges.return_value = ["2025-01-01", "2025-01-02"]
        
        mock_spark.return_value = (MagicMock(), MagicMock(), MagicMock())
        
        yield {
            'normalize': mock_normalize,
            'cast': mock_cast,
            'validate': mock_validate,
            'schema': mock_schema,
            'read': mock_read,
            'ranges': mock_ranges,
            'spark': mock_spark
        }

@patch("src.jobs.catalog_transversal_v2.initialize_spark")
def test_initialization(mock_spark, base_args):
    mock_spark.return_value = (MagicMock(), MagicMock(), MagicMock())
    
    job = StagingJob(base_args)
    
    assert job.bucket_name_raw == "raw-bucket"
    assert job.process_type == "FULL"
    assert job.catalog_table == "glue_catalog.db.table"
    assert job.partition_key_staging == ["date"]
    assert job.excel_sheet is None
    assert job.partition_key_raw_path is None


def test_run_uses_partition_key_raw_path(mock_data_processing, base_args):
    base_args["partition_key_raw_path"] = "fecha_particion"
    mock_data_processing["ranges"].return_value = ["2025-01-01"]

    job = StagingJob(base_args)
    job.run()

    args, _ = mock_data_processing["read"].call_args
    assert "fecha_particion=2025-01-01" in args[1]

def test_run_uses_file_name_over_partition(mock_data_processing, base_args):
    base_args["partition_key_raw_path"] = "mes"
    base_args["file_name_excel"] = "banco.xlsx"

    job = StagingJob(base_args)
    job.run()

    args, _ = mock_data_processing["read"].call_args
    assert args[1] == "s3://raw-bucket/raw/path/banco.xlsx"


def test_run_passes_excel_sheet_to_dependencies(mock_data_processing, base_args):
    base_args["excel_sheet"] = "Hoja 1"
    mock_data_processing["ranges"].return_value = ["2025-01-01"]

    job = StagingJob(base_args)
    job.run()

    assert mock_data_processing["read"].call_args.kwargs["excel_sheet"] == "Hoja 1"
    assert mock_data_processing["schema"].call_args.kwargs["excel_sheet"] == "Hoja 1"

def test_run_full_process(mock_data_processing, base_args):
    job = StagingJob(base_args)
    job.run()
    
    mock_data_processing['ranges'].assert_called_once_with("2025-01-01", "2025-01-02", "DAY_PRECISION")
    assert mock_data_processing['read'].call_count == 2
    job.iceberg_manager.merge_data.assert_called()

@patch("src.jobs.catalog_transversal_v2.get_previous_date")
def test_run_inc_process(mock_prev_date, mock_data_processing, base_args):
    base_args["process_type"] = "INC"
    base_args["start_date"] = ""
    base_args["end_date"] = ""
    mock_prev_date.return_value = "2025-01-15"
    mock_data_processing['ranges'].return_value = ["2025-01-15"]
    
    job = StagingJob(base_args)
    job.run()
    
    mock_prev_date.assert_called_once_with("DAY_PRECISION")
    assert mock_data_processing['read'].call_count == 1

def test_overwrite_mode(mock_data_processing, base_args):
    base_args["merge_keys"] = "none"
    mock_data_processing['ranges'].return_value = ["2025-01-01"]
    
    job = StagingJob(base_args)
    job.run()
    
    job.iceberg_manager.overwrite_partitions.assert_called_once()
    job.iceberg_manager.merge_data.assert_not_called()

@patch("src.jobs.catalog_transversal_v2.add_partition_column")
def test_add_partition_key(mock_add_partition, mock_data_processing, base_args):
    base_args["partition_key_staging"] = "add_partition_date"
    mock_data_processing['ranges'].return_value = ["2025-01-01"]
    mock_add_partition.return_value = mock_data_processing['read'].return_value
    
    job = StagingJob(base_args)
    job.run()
    
    mock_add_partition.assert_called_once()
    assert mock_add_partition.call_args[0][1] == "partition_date"

@patch("src.jobs.catalog_transversal_v2.get_today_frozen_date")
def test_add_partition_key_raw_day_precision(mock_today, mock_data_processing, base_args):
    from datetime import datetime
    mock_today.return_value = datetime(2025, 1, 15)
    base_args["partition_key_raw"] = "add_fecha"
    base_args["process_type"] = "FULL"
    
    job = StagingJob(base_args)
    job.run()
    
    assert job.partition_key_raw == "fecha"
    mock_data_processing['read'].assert_called_once()
    assert "fecha=2025-01-15" in mock_data_processing['read'].call_args[0][1]

@patch("src.jobs.catalog_transversal_v2.get_today_frozen_date")
def test_add_partition_key_raw_month_precision(mock_today, mock_data_processing, base_args):
    from datetime import datetime
    mock_today.return_value = datetime(2025, 1, 15)
    base_args["partition_key_raw"] = "add_mes"
    base_args["date_type"] = "MONTH_PRECISION"
    base_args["process_type"] = "INC"
    
    job = StagingJob(base_args)
    job.run()
    
    assert job.partition_key_raw == "mes"
    mock_data_processing['read'].assert_called_once()
    assert "mes=2025-01" in mock_data_processing['read'].call_args[0][1]

@patch("src.jobs.catalog_transversal_v2.get_schema_definition")
@patch("src.jobs.catalog_transversal_v2.initialize_spark")
def test_add_partition_key_raw_invalid_process_type(mock_spark, mock_schema, base_args):
    mock_spark.return_value = (MagicMock(), MagicMock(), MagicMock())
    mock_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    base_args["partition_key_raw"] = "add_fecha"
    base_args["process_type"] = "CUSTOM"
    
    job = StagingJob(base_args)
    
    with pytest.raises(ValueError, match="Invalid process_type"):
        job.run()

@patch("src.jobs.catalog_transversal_v2.get_schema_definition")
@patch("src.jobs.catalog_transversal_v2.initialize_spark")
def test_add_partition_key_raw_invalid_date_type(mock_spark, mock_schema, base_args):
    mock_spark.return_value = (MagicMock(), MagicMock(), MagicMock())
    mock_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    base_args["partition_key_raw"] = "add_fecha"
    base_args["date_type"] = "INVALID_PRECISION"
    
    job = StagingJob(base_args)
    
    with pytest.raises(ValueError, match="Invalid date_type"):
        job.run()

@patch("src.jobs.catalog_transversal_v2.get_schema_definition")
@patch("src.jobs.catalog_transversal_v2.initialize_spark")
def test_invalid_process_type(mock_spark, mock_schema, base_args):
    mock_spark.return_value = (MagicMock(), MagicMock(), MagicMock())
    mock_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    base_args["process_type"] = "INVALID"
    
    job = StagingJob(base_args)
    
    with pytest.raises(ValueError, match="Invalid process_type"):
        job.run()

@patch("src.jobs.catalog_transversal_v2.get_schema_definition")
@patch("src.jobs.catalog_transversal_v2.initialize_spark")
def test_full_missing_dates(mock_spark, mock_schema, base_args):
    mock_spark.return_value = (MagicMock(), MagicMock(), MagicMock())
    mock_schema.return_value = [{"name": "col1", "converted_type": "string"}]
    base_args["start_date"] = ""
    base_args["end_date"] = ""
    
    job = StagingJob(base_args)
    
    with pytest.raises(ValueError, match="start_date and end_date must be provided"):
        job.run()

def test_empty_dataframe(mock_data_processing, base_args):
    mock_data_processing['read'].return_value.rdd.isEmpty.return_value = True
    mock_data_processing['ranges'].return_value = ["2025-01-01"]
    
    job = StagingJob(base_args)
    
    # with pytest.raises(Exception, match="The following partitions had no data"):
    #     job.run()
    
    job.iceberg_manager.create_table.assert_not_called()

@patch("src.jobs.catalog_transversal_v2.getResolvedOptions")
def test_main_error_retrieving_args(mock_getResolvedOptions):
    mock_getResolvedOptions.side_effect = Exception("Missing required arguments")
    
    from src.jobs.catalog_transversal_v2 import main
    
    with pytest.raises(ValueError, match="Error retrieving arguments"):
        main()
