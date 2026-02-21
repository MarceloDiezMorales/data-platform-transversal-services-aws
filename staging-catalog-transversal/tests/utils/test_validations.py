from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from unittest.mock import MagicMock, patch
from src.utils.validations import (
    validate_dataframe,
    validate_columns_exist,
    validate_column_data_types,
    validate_required_fields,
)
import pytest


@pytest.fixture
def mock_logger():
    with patch("src.utils.validations.logger") as mock_logger:
        yield mock_logger


@pytest.fixture
def mock_df():
    df = MagicMock()
    df.columns = ["id", "nombre", "edad"]
    df.schema = {
        "id": MagicMock(dataType=IntegerType()),
        "nombre": MagicMock(dataType=StringType()),
        "edad": MagicMock(dataType=IntegerType()),
    }
    df.filter.return_value.count.return_value = 0
    return df


@pytest.fixture
def schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("nombre", StringType(), True),
        StructField("edad", IntegerType(), True),
    ])


def test_validate_columns_exist_ok(mock_df, schema, mock_logger):
    validate_columns_exist(mock_df, schema)
    mock_logger.error.assert_not_called()


def test_missing_columns_raises_error(mock_df, schema, mock_logger):
    mock_df.columns = ["id", "nombre"]  # Falta 'edad'
    with pytest.raises(ValueError, match=".*"):
        validate_columns_exist(mock_df, schema)
    mock_logger.error.assert_called_once()
    assert "Missing fields" in mock_logger.error.call_args[0][0]


@patch("src.utils.validations.col", MagicMock())
def test_validate_column_data_types_ok(mock_df, schema, mock_logger):
    def filter_side_effect(expr):
        limit_mock = MagicMock()
        limit_mock.count.return_value = 1
        filter_mock = MagicMock()
        filter_mock.limit.return_value = limit_mock
        return filter_mock
    mock_df.filter.side_effect = filter_side_effect
    validate_column_data_types(mock_df, schema)
    mock_logger.error.assert_not_called()


@patch("src.utils.validations.col", MagicMock())
def test_type_mismatch_raises_error(mock_df, schema, mock_logger):
    def filter_side_effect(expr):
        limit_mock = MagicMock()
        limit_mock.count.return_value = 1
        filter_mock = MagicMock()
        filter_mock.limit.return_value = limit_mock
        return filter_mock
    mock_df.filter.side_effect = filter_side_effect
    mock_df.schema["edad"].dataType = StringType()  # Tipo incorrecto
    with pytest.raises(TypeError, match=".*"):
        validate_column_data_types(mock_df, schema)
    mock_logger.error.assert_called_once()
    assert "Data type mismatch" in mock_logger.error.call_args[0][0]


@patch("src.utils.validations.col", MagicMock())
def test_column_with_only_nulls_skips_validation(mock_df, schema, mock_logger):
    def filter_side_effect(expr):
        limit_mock = MagicMock()
        limit_mock.count.return_value = 0
        filter_mock = MagicMock()
        filter_mock.limit.return_value = limit_mock
        return filter_mock
    mock_df.filter.side_effect = filter_side_effect
    validate_column_data_types(mock_df, schema)
    mock_logger.warning.assert_called()
    assert any("[SKIPPED]" in str(call) and "contains only NULLs" in str(call) 
               for call in mock_logger.warning.call_args_list)


@patch("src.utils.validations.col", MagicMock())
def test_required_fields_with_nulls_raises_error(mock_df, mock_logger):
    mock_df.filter.return_value.count.return_value = 2
    required_fields = ["id"]
    with pytest.raises(ValueError, match=".*"):
        validate_required_fields(mock_df, required_fields)
    mock_logger.error.assert_called_once()
    assert "null or empty" in mock_logger.error.call_args[0][0]


@patch("src.utils.validations.col", MagicMock())
def test_required_fields_with_empty_string_raises_error(mock_df, mock_logger):
    mock_df.filter.return_value.count.return_value = 1
    required_fields = ["nombre"]
    with pytest.raises(ValueError, match=".*"):
        validate_required_fields(mock_df, required_fields)
    mock_logger.error.assert_called_once()
    assert "null or empty" in mock_logger.error.call_args[0][0]


@patch("src.utils.validations.col", MagicMock())
def test_no_required_fields_logs_info(mock_df, mock_logger):
    validate_required_fields(mock_df, [])
    mock_logger.info.assert_called_once()
    assert "No required fields" in mock_logger.info.call_args[0][0]


@patch("src.utils.validations.validate_columns_exist")
@patch("src.utils.validations.validate_column_data_types")
@patch("src.utils.validations.validate_required_fields")
def test_validate_dataframe_ok(mock_req, mock_types, mock_cols, mock_df, schema):
    validate_dataframe(mock_df, schema, ["id"])
    mock_cols.assert_called_once()
    mock_types.assert_called_once()
    mock_req.assert_called_once()