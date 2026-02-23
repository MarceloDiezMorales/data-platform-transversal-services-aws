from src.services.payload_builder_v2 import PayloadBuilderService
from unittest.mock import patch
import pytest
import json


class TestPayloadBuilderService:
    @pytest.fixture
    def sample_raw_job_params(self):
        return [
            {
                "name": "sp_test",
                "path_name_input": "s3://input-path",
                "file_name": "file.json",
                "partition_key_raw": "folder",
                "params": {
                    "fecha_inicial": {"type": "date", "value": "2023-01-01"},
                    "fecha_final": {"type": "date", "value": "2023-01-31"},
                    "param1": {"type": "string", "value": "test_value"}
                }
            }
        ]

    @pytest.fixture
    def sample_staging_job_params(self):
        return [
            {
                "path_name_input": "s3://input-path",
                "path_name_output": "s3://output-path",
                "catalog_database": "db",
                "catalog_table": "table",
                "merge_keys": ["key1", "key2"],
                "partition_key_raw": "folder",
                "partition_key_staging": "date",
                "excel_sheet": "Reporte de Alertas",
                "partition_key_raw_path": "mes",
                "folder_name_json": "folder_json",
                "params": {
                    "fecha_inicial": {"type": "date", "value": "2023-01-01"},
                    "fecha_final": {"type": "date", "value": "2023-01-31"}
                }
            }
        ]

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_create_payloads_raw_full(self, mock_get_full_dates, sample_raw_job_params):
        mock_get_full_dates.return_value = ("2023-01-01", "2023-01-31")
        service = PayloadBuilderService(
            job_type="raw",
            date_type="DAY_PRECISION",
            process_type="FULL",
            job_parameters=sample_raw_job_params,
            db_type="sqlserver",
            secret_db="arn:aws:secretsmanager:..."
        )

        result = service.create_payloads()
        
        assert result["status"] == "payload generation complete"
        assert len(result["payloads_raw"]) == 1
        assert len(result["payloads_staging"]) == 0
        
        payload = result["payloads_raw"][0]
        assert payload["name"] == "sp_test"
        
        params = json.loads(payload["params"])
        assert len(params) == 3

    @patch('src.services.payload_builder_v2.get_incremental_dates')
    def test_create_payloads_raw_incremental(self, mock_get_incremental_dates, sample_raw_job_params):
        mock_get_incremental_dates.return_value = ("2023-02-01", "2023-02-28")
        
        service = PayloadBuilderService(
            job_type="raw",
            date_type="MONTH_PRECISION",
            process_type="INC",
            job_parameters=sample_raw_job_params,
            db_type="mysql",
            secret_db="arn:aws:secretsmanager:..."
        )

        result = service.create_payloads()
        
        assert result["status"] == "payload generation complete"
        assert len(result["payloads_raw"]) == 1
        
        payload = result["payloads_raw"][0]
        
        params = json.loads(payload["params"])
        fecha_inicial = next(p for p in params if p["name"] == "fecha_inicial")
        fecha_final = next(p for p in params if p["name"] == "fecha_final")
        assert fecha_inicial["value"] == "2023-02-01"
        assert fecha_final["value"] == "2023-02-28"

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_create_payloads_staging_full(self, mock_get_full_dates, sample_staging_job_params):
        mock_get_full_dates.return_value = ("2023-01-01", "2023-01-31")
        service = PayloadBuilderService(
            job_type="staging",
            date_type="DAY_PRECISION",
            process_type="FULL",
            job_parameters=sample_staging_job_params
        )

        result = service.create_payloads()
        
        assert result["status"] == "payload generation complete"
        assert len(result["payloads_raw"]) == 0
        assert len(result["payloads_staging"]) == 1
        
        payload = result["payloads_staging"][0]
        assert payload["catalog_database"] == "db"
        assert payload["catalog_table"] == "table"
        assert payload["merge_keys"] == ["key1", "key2"]
        assert payload["partition_key_staging"] == "date"
        assert payload["partition_key_raw"] == "folder"
        assert payload["excel_sheet"] == "Reporte de Alertas"
        assert payload["partition_key_raw_path"] == "mes"

    @patch('src.services.payload_builder_v2.get_incremental_dates')
    def test_create_payloads_staging_incremental(self, mock_get_incremental_dates, sample_staging_job_params):
        mock_get_incremental_dates.return_value = ("2023-03-01", "2023-03-31")
        
        service = PayloadBuilderService(
            job_type="staging",
            date_type="MONTH_PRECISION",
            process_type="INC",
            job_parameters=sample_staging_job_params
        )

        result = service.create_payloads()
        
        assert result["status"] == "payload generation complete"
        assert len(result["payloads_staging"]) == 1
        
        payload = result["payloads_staging"][0]

    @patch('src.services.payload_builder_v2.get_incremental_dates')
    def test_create_payloads_raw_to_staging(self, mock_get_incremental_dates):
        mock_get_incremental_dates.return_value = ("2023-04-01", "2023-04-30")
        
        # For raw-to-staging, configs need both raw and staging fields
        combined_params = [
            {
                "name": "sp_test",
                "path_name_input": "s3://input-path",
                "file_name": "file.json",
                "partition_key_raw": "folder",
                "path_name_output": "s3://output-path",
                "catalog_database": "db",
                "catalog_table": "table",
                "merge_keys": ["key1", "key2"],
                "partition_key_staging": "date",
                "params": {
                    "fecha_inicial": {"type": "date", "value": "2023-01-01"},
                    "fecha_final": {"type": "date", "value": "2023-01-31"}
                }
            }
        ]
        
        service = PayloadBuilderService(
            job_type="raw-to-staging",
            date_type="DAY_PRECISION",
            process_type="INC",
            job_parameters=combined_params,
            db_type="postgresql",
            secret_db="arn:aws:secretsmanager:..."
        )

        result = service.create_payloads()
        
        assert result["status"] == "payload generation complete"
        assert len(result["payloads_raw"]) == 1
        assert len(result["payloads_staging"]) == 1

    def test_create_payloads_invalid_process_type(self, sample_raw_job_params):
        service = PayloadBuilderService(
            job_type="raw",
            date_type="DAY_PRECISION",
            process_type="INVALID",
            job_parameters=sample_raw_job_params,
            db_type="sqlserver",
            secret_db="arn:aws:secretsmanager:..."
        )
        
        with pytest.raises(ValueError, match="Invalid 'process_type'"):
            service.create_payloads()

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_create_payloads_raw_missing_required_params(self, mock_get_full_dates, sample_raw_job_params):
        mock_get_full_dates.return_value = ("2023-01-01", "2023-01-31")
        service = PayloadBuilderService(
            job_type="raw",
            date_type="DAY_PRECISION",
            process_type="FULL",
            job_parameters=sample_raw_job_params
        )
        
        with pytest.raises(ValueError, match="Missing required parameters"):
            service.create_payloads()

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_create_payloads_full_with_fecha_corte(self, mock_get_full_dates):
        """Test that fecha_corte is accepted as a valid alternative for FULL process"""
        mock_get_full_dates.return_value = ("2023-01-01", "2023-01-01")
        params_with_corte = [
            {
                "name": "sp_test",
                "path_name_input": "s3://input-path",
                "file_name": "file.json",
                "partition_key_raw": "folder",
                "params": {
                    "fecha_corte": {"type": "date", "value": "2023-01-01"}
                }
            }
        ]
        
        service = PayloadBuilderService(
            job_type="raw",
            date_type="DAY_PRECISION",
            process_type="FULL",
            job_parameters=params_with_corte,
            db_type="sqlserver",
            secret_db="arn:aws:secretsmanager:..."
        )
        
        result = service.create_payloads()
        assert result["status"] == "payload generation complete"
        assert len(result["payloads_raw"]) == 1
        mock_get_full_dates.assert_called_once_with("2023-01-01", "2023-01-01", "DAY_PRECISION")

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_create_payloads_full_without_date_params(self, mock_get_full_dates):
        """Test that missing all date parameters raises an error for FULL process"""
        mock_get_full_dates.return_value = ("2023-01-01", "2023-01-01")
        params_without_dates = [
            {
                "name": "sp_test",
                "path_name_input": "s3://input-path",
                "file_name": "file.json",
                "partition_key_raw": "folder",
                "params": {
                    "param1": {"type": "string", "value": "test_value"}
                }
            }
        ]
        
        service = PayloadBuilderService(
            job_type="raw",
            date_type="DAY_PRECISION",
            process_type="FULL",
            job_parameters=params_without_dates,
            db_type="sqlserver",
            secret_db="arn:aws:secretsmanager:..."
        )
        
        with pytest.raises(ValueError, match="Missing date parameters for FULL process in Raw job"):
            service.create_payloads()

    @patch('src.services.payload_builder_v2.get_incremental_dates')
    def test_build_raw_payload_with_fecha_corte(self, mock_get_incremental_dates):
        mock_get_incremental_dates.return_value = ("2023-05-01", "2023-05-31")
        
        params_with_corte = [
            {
                "name": "sp_corte",
                "path_name_input": "s3://input-path",
                "file_name": "file.json",
                "partition_key_raw": "folder",
                "params": {
                    "fecha_inicial": {"type": "date", "value": "2023-01-01"},
                    "fecha_corte": {"type": "date", "value": "2023-01-31"}
                }
            }
        ]
        
        service = PayloadBuilderService(
            job_type="raw",
            date_type="DAY_PRECISION",
            process_type="INC",
            job_parameters=params_with_corte,
            db_type="sqlserver",
            secret_db="arn:aws:secretsmanager:..."
        )

        result = service.create_payloads()
        payload = result["payloads_raw"][0]
        
        params = json.loads(payload["params"])
        fecha_corte = next(p for p in params if p["name"] == "fecha_corte")
        assert fecha_corte["value"] == "2023-05-31"

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_month_precision_path_format(self, mock_get_full_dates):
        mock_get_full_dates.return_value = ("2023-01-01", "2023-01-31")
        params = [
            {
                "name": "sp_test",
                "path_name_input": "s3://input-path",
                "file_name": "file.json",
                "partition_key_raw": "folder",
                "params": {
                    "fecha_inicial": {"type": "date", "value": "2023-01"},
                    "fecha_final": {"type": "date", "value": "2023-01"}
                }
            }
        ]
        
        service = PayloadBuilderService(
            job_type="raw",
            date_type="MONTH_PRECISION",
            process_type="FULL",
            job_parameters=params,
            db_type="sqlserver",
            secret_db="arn:aws:secretsmanager:..."
        )

        result = service.create_payloads()
        payload = result["payloads_raw"][0]
        
        assert payload["partition_key_raw"] == "folder"

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_multiple_job_parameters(self, mock_get_full_dates):
        mock_get_full_dates.return_value = ("2023-01-01", "2023-01-31")
        multi_params = [
            {
                "name": "sp_1",
                "path_name_input": "s3://path1",
                "file_name": "file1.json",
                "partition_key_raw": "folder1",
                "params": {
                    "fecha_inicial": {"type": "date", "value": "2023-01-01"},
                    "fecha_final": {"type": "date", "value": "2023-01-31"}
                }
            },
            {
                "name": "sp_2",
                "path_name_input": "s3://path2",
                "file_name": "file2.json",
                "partition_key_raw": "folder2",
                "params": {
                    "fecha_inicial": {"type": "date", "value": "2023-01-01"},
                    "fecha_final": {"type": "date", "value": "2023-01-31"}
                }
            }
        ]
        
        service = PayloadBuilderService(
            job_type="raw",
            date_type="DAY_PRECISION",
            process_type="FULL",
            job_parameters=multi_params,
            db_type="sqlserver",
            secret_db="arn:aws:secretsmanager:..."
        )

        result = service.create_payloads()
        
        assert len(result["payloads_raw"]) == 2
        assert result["payloads_raw"][0]["name"] != result["payloads_raw"][1]["name"]

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_create_payloads_staging_full_with_fecha_corte(self, mock_get_full_dates):
        """Test staging FULL process with fecha_corte parameter"""
        mock_get_full_dates.return_value = ("2023-06-01", "2023-06-01")
        
        staging_params_with_corte = [
            {
                "path_name_input": "s3://input-path",
                "path_name_output": "s3://output-path",
                "catalog_database": "db",
                "catalog_table": "table",
                "merge_keys": ["key1"],
                "partition_key_raw": "folder",
                "partition_key_staging": "date",
                "params": {
                    "fecha_corte": {"type": "date", "value": "2023-06-01"}
                }
            }
        ]
        
        service = PayloadBuilderService(
            job_type="staging",
            date_type="DAY_PRECISION",
            process_type="FULL",
            job_parameters=staging_params_with_corte
        )

        result = service.create_payloads()
        
        assert result["status"] == "payload generation complete"
        assert len(result["payloads_staging"]) == 1
        assert result["payloads_staging"][0]["start_date"] == "2023-06-01"
        assert result["payloads_staging"][0]["end_date"] == "2023-06-01"
        mock_get_full_dates.assert_called_once_with("2023-06-01", "2023-06-01", "DAY_PRECISION")

    @patch('src.services.payload_builder_v2.get_full_dates')
    def test_create_payloads_staging_full_without_date_params(self, mock_get_full_dates):
        """Test staging FULL process without date parameters raises error"""
        mock_get_full_dates.return_value = ("2023-01-01", "2023-01-31")
        
        staging_params_no_dates = [
            {
                "path_name_input": "s3://input-path",
                "path_name_output": "s3://output-path",
                "catalog_database": "db",
                "catalog_table": "table",
                "merge_keys": ["key1"],
                "partition_key_raw": "folder",
                "partition_key_staging": "date",
                "params": {
                    "some_param": {"type": "string", "value": "test"}
                }
            }
        ]
        
        service = PayloadBuilderService(
            job_type="staging",
            date_type="DAY_PRECISION",
            process_type="FULL",
            job_parameters=staging_params_no_dates
        )

        with pytest.raises(ValueError, match="Missing date parameters for FULL process in Staging jobs"):
            service.create_payloads()
