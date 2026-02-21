from src.utils.schemas import build_struct_from_schema, get_non_nullable_fields, get_schema_definition
from pyspark.sql.types import StructType, StringType
from unittest.mock import patch, MagicMock
import pytest
import json

@patch("src.utils.schemas.logger")
def test_build_struct_from_schema_success(mock_logger):
    """ Crea correctamente un StructType a partir de un esquema válido."""
    schema_fields = [
        {"name": "id", "type": "int", "nullable": False},
        {"name": "name", "type": "string", "nullable": True},
    ]

    result = build_struct_from_schema(schema_fields)

    assert isinstance(result, StructType)
    assert len(result.fields) == 2
    assert result.fields[0].name == "id"
    assert isinstance(result.fields[1].dataType, StringType)
    mock_logger.info.assert_called_once()


@patch("src.utils.schemas.logger")
def test_build_struct_from_schema_empty_list(mock_logger):
    """ Lanza ValueError si la lista está vacía."""
    with pytest.raises(ValueError):
        build_struct_from_schema([])

    mock_logger.info.assert_not_called()


@patch("src.utils.schemas.logger")
def test_build_struct_from_schema_missing_keys(mock_logger):
    """ Lanza KeyError si falta 'name' o 'type'."""
    schema_fields = [{"name": "id"}, {"type": "string"}]

    with pytest.raises(KeyError):
        build_struct_from_schema(schema_fields)

    mock_logger.error.assert_called()


@patch("src.utils.schemas.logger")
def test_build_struct_from_schema_unexpected_error(mock_logger):
    """Simula error inesperado (forzando excepción)."""
    with patch("src.utils.schemas.StructField", side_effect=Exception("boom")):
        with pytest.raises(RuntimeError):
            build_struct_from_schema([{"name": "x", "type": "int"}])

    mock_logger.error.assert_called()


@patch("src.utils.schemas.logger")
def test_get_non_nullable_fields_success(mock_logger):
    """ Retorna los campos no nulos correctamente."""
    schema_fields = [
        {"name": "id", "nullable": False},
        {"name": "name", "nullable": True},
        {"name": "email", "nullable": False},
    ]

    result = get_non_nullable_fields(schema_fields)

    assert result == ["id", "email"]
    mock_logger.info.assert_called_once()


@patch("src.utils.schemas.logger")
def test_get_non_nullable_fields_empty_list(mock_logger):
    """ Lanza ValueError si la lista está vacía."""
    with pytest.raises(ValueError):
        get_non_nullable_fields([])

    mock_logger.info.assert_not_called()


@patch("src.utils.schemas.logger")
def test_get_non_nullable_fields_runtime_error(mock_logger):
    """ Simula excepción inesperada dentro del try."""
    schema_fields = [{"name": "id"}, {"nullable": False}]
    with patch("src.utils.schemas.logger.info", side_effect=Exception("test error")):
        with pytest.raises(RuntimeError):
            get_non_nullable_fields(schema_fields)

    mock_logger.error.assert_called()


@patch("src.utils.schemas.get_object")
@patch("src.utils.schemas.logger")
def test_get_schema_definition_success(mock_logger, mock_get_object):
    schema_dict = {
        "fields": [
            {"name": "id", "type": "int", "nullable": False},
            {"name": "name", "type": "string", "nullable": True}
        ]
    }
    mock_get_object.return_value = json.dumps(schema_dict).encode("utf-8")
    mock_spark = MagicMock()

    result = get_schema_definition("bucket", "path/to/data", mock_spark, excel_sheet="")

    assert len(result) == 2
    assert result[0]["name"] == "id"
    assert result[1]["type"] == "string"
    mock_get_object.assert_called_once_with("bucket", "path/to/data/schema.json")


@patch("src.utils.schemas.get_object")
@patch("src.utils.schemas.logger")
def test_get_schema_definition_empty_schema(mock_logger, mock_get_object):
    mock_get_object.return_value = json.dumps({}).encode("utf-8")
    mock_spark = MagicMock()

    with pytest.raises(FileNotFoundError, match="Schema content is empty"):
        get_schema_definition("bucket", "path", mock_spark, excel_sheet="")


@patch("src.utils.schemas.get_object")
@patch("src.utils.schemas.logger")
def test_get_schema_definition_no_fields(mock_logger, mock_get_object):
    schema_dict = {"fields": []}
    mock_get_object.return_value = json.dumps(schema_dict).encode("utf-8")
    mock_spark = MagicMock()

    with pytest.raises(ValueError, match="does not contain any defined fields"):
        get_schema_definition("bucket", "path", mock_spark, excel_sheet="")


@patch("src.utils.schemas.get_object")
@patch("src.utils.schemas.logger")
def test_get_schema_definition_invalid_json(mock_logger, mock_get_object):
    mock_get_object.return_value = b"invalid json {{"
    mock_spark = MagicMock()

    with pytest.raises(ValueError, match="not valid JSON"):
        get_schema_definition("bucket", "path", mock_spark, excel_sheet="")


@patch("src.utils.schemas.get_object")
@patch("src.utils.schemas.logger")
def test_get_schema_definition_missing_fields_key(mock_logger, mock_get_object):
    schema_dict = {"data": [{"name": "id"}]}
    mock_get_object.return_value = json.dumps(schema_dict).encode("utf-8")
    mock_spark = MagicMock()

    with pytest.raises(ValueError, match="does not contain any defined fields"):
        get_schema_definition("bucket", "path", mock_spark, excel_sheet="")


@patch("src.utils.schemas.get_object")
@patch("src.utils.schemas.logger")
def test_get_schema_definition_unexpected_error(mock_logger, mock_get_object):
    mock_get_object.side_effect = Exception("unexpected error")
    mock_spark = MagicMock()

    with pytest.raises(RuntimeError, match="Unexpected error while parsing schema"):
        get_schema_definition("bucket", "path", mock_spark, excel_sheet="")


@patch("src.utils.schemas.get_object")
@patch("src.utils.schemas.logger")
def test_get_schema_definition_path_formatting(mock_logger, mock_get_object):
    schema_dict = {"fields": [{"name": "col1", "type": "string"}]}
    mock_get_object.return_value = json.dumps(schema_dict).encode("utf-8")
    mock_spark = MagicMock()

    get_schema_definition("my-bucket", "raw/data", mock_spark, excel_sheet="")

    mock_get_object.assert_called_once_with("my-bucket", "raw/data/schema.json")


@patch("src.utils.schemas.get_object")
@patch("src.utils.schemas.logger")
def test_get_schema_definition_with_excel_sheet(mock_logger, mock_get_object):
    schema_dict = {"fields": [{"name": "col1", "type": "string"}]}
    mock_get_object.return_value = json.dumps(schema_dict).encode("utf-8")
    mock_spark = MagicMock()

    get_schema_definition("my-bucket", "raw/data", mock_spark, excel_sheet="Hoja 1")

    mock_get_object.assert_called_once_with("my-bucket", "raw/data/hoja_1/schema.json")
