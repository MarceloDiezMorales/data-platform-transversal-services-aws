"""
Test suite for S3 data reading functionality (auto-detect version).

Tests:
- CSV and JSON detection and reading.
- Empty DataFrame handling.
- Unsupported file type errors.
- Exceptions during Spark read.
"""
from src.resources.s3_v2 import (
    read_data_from_s3,
    _detect_file_type_in_s3,
    _extract_entity_from_path,
    read_excel_from_s3,
    get_object,
)
from unittest.mock import MagicMock, patch
import pytest
import pandas as pd
from pyspark.sql.types import (
    StructType,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    TimestampType,
)

# Helper to mock Spark read builder
def _make_builder(csv_df=None, json_df=None, csv_side_effect=None, json_side_effect=None):
    builder = MagicMock()
    builder.option.return_value = builder  # chaining support

    if csv_side_effect is not None:
        builder.csv.side_effect = csv_side_effect
    else:
        builder.csv.return_value = csv_df

    if json_side_effect is not None:
        builder.json.side_effect = json_side_effect
    else:
        builder.json.return_value = json_df

    return builder


def test_read_csv_success():
    """Should read CSV successfully when _detect_file_type_in_s3 returns 'csv'."""
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = False
    mock_df.columns = ["col1", "col2"]
    mock_df.count.return_value = 10

    builder = _make_builder(csv_df=mock_df)
    mock_spark = MagicMock()
    mock_spark.read = builder

    with patch("src.resources.s3_v2._detect_file_type_in_s3", return_value=("csv", None)), \
         patch("src.resources.s3_v2.logger"):
        df = read_data_from_s3(mock_spark, "s3://mock/path")
        assert df.columns == ["col1", "col2"]
        builder.csv.assert_called_once()
        builder.json.assert_not_called()


def test_read_json_success():
    """Should read JSON successfully when _detect_file_type_in_s3 returns 'json'."""
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = False
    mock_df.columns = ["field1"]
    mock_df.count.return_value = 5

    builder = _make_builder(json_df=mock_df)
    mock_spark = MagicMock()
    mock_spark.read = builder

    with patch("src.resources.s3_v2._detect_file_type_in_s3", return_value=("json", None)), \
         patch("src.resources.s3_v2.logger"):
        df = read_data_from_s3(mock_spark, "s3://mock/json")
        assert df.columns == ["field1"]
        builder.json.assert_called_once()
        builder.csv.assert_not_called()


def test_unsupported_file_type_raises():
    """If _detect_file_type_in_s3 returns unknown type, should raise RuntimeError."""
    mock_spark = MagicMock()
    with patch("src.resources.s3_v2._detect_file_type_in_s3", return_value=("xml", None)), \
         patch("src.resources.s3_v2.logger"):
        with pytest.raises(RuntimeError, match="Unsupported file type"):
            read_data_from_s3(mock_spark, "s3://mock/unknown")

def test_spark_read_exception_raises_runtimeerror():
    """If Spark read fails (raises Exception), should wrap in RuntimeError."""
    builder = _make_builder(csv_side_effect=Exception("Spark error"))
    mock_spark = MagicMock()
    mock_spark.read = builder

    with patch("src.resources.s3_v2._detect_file_type_in_s3", return_value=("csv", None)), \
         patch("src.resources.s3_v2.logger"):
        with pytest.raises(RuntimeError, match="Error reading from"):
            read_data_from_s3(mock_spark, "s3://mock/fail")

def test_read_excel_success_add_entity():
    """Should read Excel and add Entidad when add_entity is True."""
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = False
    mock_df.columns = ["col1"]
    mock_df.count.return_value = 1
    mock_df.withColumn.return_value = mock_df

    mock_spark = MagicMock()

    with patch("src.resources.s3_v2._detect_file_type_in_s3", return_value=("excel", "s3://bucket/417/entidad_2025-01.xlsx")), \
         patch("src.resources.s3_v2.read_excel_from_s3", return_value=mock_df) as mock_read_excel, \
         patch("src.resources.s3_v2.lit", return_value=MagicMock()), \
         patch("src.resources.s3_v2._extract_entity_from_path", return_value="entidad"), \
         patch("src.resources.s3_v2.logger"):
        df = read_data_from_s3(
            mock_spark,
            "s3://bucket/417/",
            excel_sheet="Hoja 1",
            add_entity=True,
        )
        assert df.columns == ["col1"]
        mock_read_excel.assert_called_once_with(
            mock_spark,
            "s3://bucket/417/entidad_2025-01.xlsx",
            "Hoja 1",
            True,
        )
        mock_df.withColumn.assert_called_once()


def test_read_excel_success_no_entity():
    """Should read Excel without adding Entidad when add_entity is False."""
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = False
    mock_df.columns = ["col1"]
    mock_df.count.return_value = 1
    mock_df.withColumn.return_value = mock_df

    mock_spark = MagicMock()

    with patch("src.resources.s3_v2._detect_file_type_in_s3", return_value=("excel", "s3://bucket/other/file.xlsx")), \
         patch("src.resources.s3_v2.read_excel_from_s3", return_value=mock_df) as mock_read_excel, \
         patch("src.resources.s3_v2.logger"):
        df = read_data_from_s3(
            mock_spark,
            "s3://bucket/other/",
            excel_sheet=None,
            add_entity=False,
        )
        assert df.columns == ["col1"]
        mock_read_excel.assert_called_once()
        mock_df.withColumn.assert_not_called()


@patch("src.resources.s3_v2.boto3.client")
def test_detect_file_type_csv(mock_boto_client):
    """Should detect CSV file type correctly from S3 path."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "path/to/file.csv"}]
    }
    mock_boto_client.return_value = mock_s3

    result = _detect_file_type_in_s3("s3://bucket/path/to/")
    assert result == ("csv", "s3://bucket/path/to/file.csv")


@patch("src.resources.s3_v2.boto3.client")
def test_detect_file_type_json(mock_boto_client):
    """Should detect JSON file type correctly from S3 path."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "path/to/file.json"}]
    }
    mock_boto_client.return_value = mock_s3

    result = _detect_file_type_in_s3("s3://bucket/path/to/")
    assert result == ("json", "s3://bucket/path/to/file.json")


@patch("src.resources.s3_v2.boto3.client")
def test_detect_file_type_excel(mock_boto_client):
    """Should detect Excel file type correctly from S3 path."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "path/to/file.xlsx"}]
    }
    mock_boto_client.return_value = mock_s3

    result = _detect_file_type_in_s3("s3://bucket/path/to/")
    assert result == ("excel", "s3://bucket/path/to/file.xlsx")


@patch("src.resources.s3_v2.boto3.client")
def test_detect_file_type_excel_with_file_path(mock_boto_client):
    """Should detect Excel file type correctly when a full file path is provided."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "path/to/file.xlsx"}]
    }
    mock_boto_client.return_value = mock_s3

    result = _detect_file_type_in_s3("s3://bucket/path/to/file.xlsx")
    assert result == ("excel", "s3://bucket/path/to/file.xlsx")
    mock_s3.list_objects_v2.assert_called_once_with(Bucket="bucket", Prefix="path/to/file.xlsx")

@patch("src.resources.s3_v2.boto3.client")
def test_detect_file_type_no_supported_files(mock_boto_client):
    """Should raise RuntimeError if no supported file types are found."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "path/to/file.txt"}]
    }
    mock_boto_client.return_value = mock_s3

    with pytest.raises(RuntimeError, match="No supported file type"):
        _detect_file_type_in_s3("s3://bucket/path/to/")


@patch("src.resources.s3_v2.boto3.client")
def test_detect_file_type_no_files(mock_boto_client):
    """Should raise FileNotFoundError if no files are found."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {}
    mock_boto_client.return_value = mock_s3

    with pytest.raises(FileNotFoundError, match="No files found"):
        _detect_file_type_in_s3("s3://bucket/path/to/")


def test_detect_file_type_invalid_path():
    """Should raise ValueError if S3 path is invalid."""
    with pytest.raises(ValueError, match="Invalid S3 path"):
        _detect_file_type_in_s3("invalid-path")


@patch("src.resources.s3_v2.boto3.client")
def test_get_object_success(mock_boto_client):
    """Should successfully retrieve object from S3."""
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = b"test content"
    mock_s3.get_object.return_value = {"Body": mock_body}
    mock_boto_client.return_value = mock_s3
    
    result = get_object("test-bucket", "path/to/file.txt")
    
    assert result == b"test content"
    mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="path/to/file.txt")


@patch("src.resources.s3_v2.boto3.client")
def test_get_object_with_nested_path(mock_boto_client):
    """Should retrieve object with nested path."""
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = b"nested content"
    mock_s3.get_object.return_value = {"Body": mock_body}
    mock_boto_client.return_value = mock_s3
    
    result = get_object("my-bucket", "folder/subfolder/data.json")
    
    assert result == b"nested content"
    mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="folder/subfolder/data.json")


@patch("src.resources.s3_v2.boto3.client")
def test_get_object_empty_content(mock_boto_client):
    """Should handle empty object content."""
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = b""
    mock_s3.get_object.return_value = {"Body": mock_body}
    mock_boto_client.return_value = mock_s3
    
    result = get_object("bucket", "empty.txt")
    
    assert result == b""


@patch("src.resources.s3_v2.boto3.client")
def test_get_object_raises_runtime_error(mock_boto_client):
    """Should raise RuntimeError when S3 operation fails."""
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("S3 access denied")
    mock_boto_client.return_value = mock_s3
    
    with patch("src.resources.s3_v2.logger"):
        with pytest.raises(RuntimeError, match="Error getting object test.txt from bucket my-bucket"):
            get_object("my-bucket", "test.txt")


@patch("src.resources.s3_v2.boto3.client")
def test_get_object_with_binary_content(mock_boto_client):
    """Should handle binary content correctly."""
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    binary_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
    mock_body.read.return_value = binary_data
    mock_s3.get_object.return_value = {"Body": mock_body}
    mock_boto_client.return_value = mock_s3
    
    result = get_object("images-bucket", "photo.png")
    
    assert result == binary_data


def test_extract_entity_from_path_filename():
    assert _extract_entity_from_path("s3://bucket/417/entidad_2025-01.xlsx") == "entidad"


def test_extract_entity_from_path_folder():
    assert _extract_entity_from_path("s3://bucket/417/comisionista/file.xlsx") == "comisionista"


def test_extract_entity_from_path_none():
    assert _extract_entity_from_path("") is None


def test_read_excel_from_s3_infers_schema():
    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = MagicMock()

    pdf = pd.DataFrame(
        {
            "int_col": [1, 2],
            "float_col": [1.5, 2.5],
            "bool_col": [True, False],
            "dt_col": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            "str_col": ["a", "b"],
            "all_null": [None, None],
        }
    )

    with patch("src.resources.s3_v2.get_object", return_value=b"dummy"), \
         patch("src.resources.s3_v2.pd.read_excel", return_value=pdf) as mock_read_excel, \
         patch("src.resources.s3_v2.logger"):
        read_excel_from_s3(mock_spark, "s3://bucket/file.xlsx", None, header=True)

    mock_read_excel.assert_called_once()
    _, excel_kwargs = mock_read_excel.call_args
    assert excel_kwargs["sheet_name"] == 0
    assert excel_kwargs["header"] == 0
    _, kwargs = mock_spark.createDataFrame.call_args
    schema = kwargs["schema"]
    assert isinstance(schema, StructType)
    field_map = {f.name: f.dataType for f in schema.fields}
    assert isinstance(field_map["int_col"], LongType)
    assert isinstance(field_map["float_col"], DoubleType)
    assert isinstance(field_map["bool_col"], BooleanType)
    assert isinstance(field_map["dt_col"], TimestampType)
    assert isinstance(field_map["str_col"], StringType)
    assert isinstance(field_map["all_null"], StringType)


def test_read_excel_from_s3_invalid_path():
    mock_spark = MagicMock()
    with patch("src.resources.s3_v2.logger"):
        with pytest.raises(RuntimeError, match="Error reading Excel from"):
            read_excel_from_s3(mock_spark, "invalid-path", None, header=True)

