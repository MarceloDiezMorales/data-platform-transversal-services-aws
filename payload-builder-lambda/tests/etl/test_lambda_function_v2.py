from src.etl.lambda_function_v2 import lambda_handler
from unittest.mock import patch
import pytest

@pytest.fixture
def base_event():
    return {
        "payloadParameters": {
            "project_name": "Test Project V2",
            "date_type": "monthly",
            "process_type": "FULL",
            "job_type": "raw-to-staging",
            "job_parameters": [
                {
                    "param_init_date": "start",
                    "param_end_date": "end",
                    "sp_name": "sp",
                    "procedure": "proc",
                    "path_name_input": "input",
                    "folder_name": "folder",
                    "file_name": "file"
                }
            ],
            "secret_db": "secret",
            "db_type": "sqlserver",
            "notification_emails": "test@example.com"
        }
    }


def test_lambda_handler_full(base_event):
    with patch("src.etl.lambda_function_v2.PayloadBuilderService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.create_payloads.return_value = {
            "status": "payload generation complete",
            "payloads": ["payload1", "payload2"]
        }

        response = lambda_handler(base_event, {})

        params = base_event["payloadParameters"]
        MockService.assert_called_once_with(
            params["job_type"],
            params["date_type"],
            params["process_type"],
            params["job_parameters"],
            params["db_type"],
            params["secret_db"]
        )
        mock_instance.create_payloads.assert_called_once()
        assert response["status"] == "payload generation complete"
        assert "payloads" in response


def test_lambda_handler_incremental(base_event):
    base_event["payloadParameters"]["process_type"] = "INC"
    with patch("src.etl.lambda_function_v2.PayloadBuilderService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.create_payloads.return_value = {
            "status": "payload generation complete",
            "payloads": ["inc_payload1"]
        }

        response = lambda_handler(base_event, {})

        MockService.assert_called_once()
        mock_instance.create_payloads.assert_called_once()
        assert response["status"] == "payload generation complete"


def test_lambda_handler_missing_db_type(base_event):
    del base_event["payloadParameters"]["db_type"]
    
    with patch("src.etl.lambda_function_v2.PayloadBuilderService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.create_payloads.return_value = {
            "status": "payload generation complete",
            "payloads": ["payload1"]
        }

        response = lambda_handler(base_event, {})

        params = base_event["payloadParameters"]
        MockService.assert_called_once_with(
            params["job_type"],
            params["date_type"],
            params["process_type"],
            params["job_parameters"],
            "sqlserver",
            params["secret_db"],
        )
        mock_instance.create_payloads.assert_called_once()
        assert response["status"] == "payload generation complete"


def test_lambda_handler_mysql_db_type(base_event):
    base_event["payloadParameters"]["db_type"] = "mysql"
    
    with patch("src.etl.lambda_function_v2.PayloadBuilderService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.create_payloads.return_value = {
            "status": "payload generation complete",
            "payloads": ["payload1"]
        }

        response = lambda_handler(base_event, {})

        params = base_event["payloadParameters"]
        MockService.assert_called_once_with(
            params["job_type"],
            params["date_type"],
            params["process_type"],
            params["job_parameters"],
            "mysql",
            params["secret_db"]
        )
        mock_instance.create_payloads.assert_called_once()
        assert response["status"] == "payload generation complete"


def test_lambda_handler_postgresql_db_type(base_event):
    base_event["payloadParameters"]["db_type"] = "postgresql"
    
    with patch("src.etl.lambda_function_v2.PayloadBuilderService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.create_payloads.return_value = {
            "status": "payload generation complete",
            "payloads": ["payload1"]
        }

        response = lambda_handler(base_event, {})

        MockService.assert_called_once()
        mock_instance.create_payloads.assert_called_once()
        assert response["status"] == "payload generation complete"


def test_lambda_handler_missing_project_name(base_event):
    del base_event["payloadParameters"]["project_name"]
    
    with pytest.raises(ValueError, match="'project_name' and 'notification_emails' are required"):
        lambda_handler(base_event, {})


def test_lambda_handler_missing_notification_emails(base_event):
    del base_event["payloadParameters"]["notification_emails"]
    
    with pytest.raises(ValueError, match="'project_name' and 'notification_emails' are required"):
        lambda_handler(base_event, {})


def test_lambda_handler_with_multiple_job_parameters(base_event):
    base_event["payloadParameters"]["job_parameters"] = [
        {"param_init_date": "start1", "sp_name": "sp1"},
        {"param_init_date": "start2", "sp_name": "sp2"},
        {"param_init_date": "start3", "sp_name": "sp3"}
    ]
    
    with patch("src.etl.lambda_function_v2.PayloadBuilderService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.create_payloads.return_value = {
            "status": "payload generation complete",
            "payloads": ["payload1", "payload2", "payload3"]
        }

        response = lambda_handler(base_event, {})

        MockService.assert_called_once()
        call_args = MockService.call_args[0]
        assert len(call_args[3]) == 3
        mock_instance.create_payloads.assert_called_once()
        assert response["status"] == "payload generation complete"
        assert len(response["payloads"]) == 3
