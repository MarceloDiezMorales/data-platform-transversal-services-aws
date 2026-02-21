"""
Test suite for S3 data reading functionality (auto-detect version).

Tests:
- CSV and JSON detection and reading.
- Empty DataFrame handling.
- Unsupported file type errors.
- Exceptions during Spark read.
- S3 object retrieval.
"""
from src.resources.s3 import read_data_from_s3, get_object
from unittest.mock import MagicMock, patch
import pytest

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

    with patch("src.resources.s3._detect_file_type_in_s3", return_value="csv"), \
         patch("src.resources.s3.logger"):
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

    with patch("src.resources.s3._detect_file_type_in_s3", return_value="json"), \
         patch("src.resources.s3.logger"):
        df = read_data_from_s3(mock_spark, "s3://mock/json")
        assert df.columns == ["field1"]
        builder.json.assert_called_once()
        builder.csv.assert_not_called()


def test_unsupported_file_type_raises():
    """If _detect_file_type_in_s3 returns unknown type, should raise RuntimeError."""
    mock_spark = MagicMock()
    with patch("src.resources.s3._detect_file_type_in_s3", return_value="xml"), \
         patch("src.resources.s3.logger"):
        with pytest.raises(RuntimeError, match="Unsupported file type"):
            read_data_from_s3(mock_spark, "s3://mock/unknown")


def test_empty_dataframe_raises():
    """If the DataFrame read is empty, should raise ValueError."""
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = True  # empty DF

    builder = _make_builder(csv_df=mock_df)
    mock_spark = MagicMock()
    mock_spark.read = builder

    with patch("src.resources.s3._detect_file_type_in_s3", return_value="csv"), \
         patch("src.resources.s3.logger"):
        with pytest.raises(ValueError, match="read returned empty DataFrame"):
            read_data_from_s3(mock_spark, "s3://mock/empty")


def test_spark_read_exception_raises_runtimeerror():
    """If Spark read fails (raises Exception), should wrap in RuntimeError."""
    builder = _make_builder(csv_side_effect=Exception("Spark error"))
    mock_spark = MagicMock()
    mock_spark.read = builder

    with patch("src.resources.s3._detect_file_type_in_s3", return_value="csv"), \
         patch("src.resources.s3.logger"):
        with pytest.raises(RuntimeError, match="Error reading from"):
            read_data_from_s3(mock_spark, "s3://mock/fail")


@patch("src.resources.s3.boto3.client")
def test_detect_file_type_csv(mock_boto_client):
    """Should detect CSV file type correctly from S3 path."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "path/to/file.csv"}]
    }
    mock_boto_client.return_value = mock_s3

    from src.resources.s3 import _detect_file_type_in_s3
    result = _detect_file_type_in_s3("s3://bucket/path/to/")
    assert result == "csv"


@patch("src.resources.s3.boto3.client")
def test_detect_file_type_json(mock_boto_client):
    """Should detect JSON file type correctly from S3 path."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "path/to/file.json"}]
    }
    mock_boto_client.return_value = mock_s3

    from src.resources.s3 import _detect_file_type_in_s3
    result = _detect_file_type_in_s3("s3://bucket/path/to/")
    assert result == "json"


@patch("src.resources.s3.boto3.client")
def test_detect_file_type_no_supported_files(mock_boto_client):
    """Should raise RuntimeError if no supported file types are found."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "path/to/file.txt"}]
    }
    mock_boto_client.return_value = mock_s3

    from src.resources.s3 import _detect_file_type_in_s3
    with pytest.raises(RuntimeError, match="No supported file type"):
        _detect_file_type_in_s3("s3://bucket/path/to/")


@patch("src.resources.s3.boto3.client")
def test_detect_file_type_no_files(mock_boto_client):
    """Should raise FileNotFoundError if no files are found."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {}
    mock_boto_client.return_value = mock_s3

    from src.resources.s3 import _detect_file_type_in_s3
    with pytest.raises(FileNotFoundError, match="No files found"):
        _detect_file_type_in_s3("s3://bucket/path/to/")


def test_detect_file_type_invalid_path():
    """Should raise ValueError if S3 path is invalid."""
    from src.resources.s3 import _detect_file_type_in_s3
    with pytest.raises(ValueError, match="Invalid S3 path"):
        _detect_file_type_in_s3("invalid-path")


@patch("src.resources.s3.boto3.client")
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


@patch("src.resources.s3.boto3.client")
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


@patch("src.resources.s3.boto3.client")
def test_get_object_empty_content(mock_boto_client):
    """Should handle empty object content."""
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = b""
    mock_s3.get_object.return_value = {"Body": mock_body}
    mock_boto_client.return_value = mock_s3
    
    result = get_object("bucket", "empty.txt")
    
    assert result == b""


@patch("src.resources.s3.boto3.client")
def test_get_object_raises_runtime_error(mock_boto_client):
    """Should raise RuntimeError when S3 operation fails."""
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("S3 access denied")
    mock_boto_client.return_value = mock_s3
    
    with patch("src.resources.s3.logger"):
        with pytest.raises(RuntimeError, match="Error getting object test.txt from bucket my-bucket"):
            get_object("my-bucket", "test.txt")


@patch("src.resources.s3.boto3.client")
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

