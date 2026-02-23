"""
Module for reading data from S3 into Spark DataFrames or retrieving S3 objects.
"""
from src.config.logger import logger
from pyspark.sql import SparkSession
import boto3

def read_data_from_s3(spark: SparkSession, s3_path: str, header: bool = True, delimiter: str = ";"):
    """
    Reads a file from the given S3 path and returns a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session object.
        s3_path (str): S3 path to the file or folder (e.g. 's3://bucket/path/').
        header (bool): Whether the CSV file has a header row. Default is True.
        delimiter (str): Delimiter for CSV files. Default is ';'.

    Returns:
        DataFrame: Spark DataFrame containing the data from the S3 file.

    Raises:
        ValueError: If the file is empty or invalid.
        RuntimeError: If there is an error reading the file from S3.
    """
    file_type = _detect_file_type_in_s3(s3_path)
    logger.info(f"# [INFO]: Detected file type '{file_type}' for path: {s3_path}")

    try:
        if file_type == "csv":
            df = (
                spark.read
                .option("header", str(header).lower())
                .option("inferSchema", "true")
                .option("delimiter", delimiter)
                .csv(s3_path)
            )
        elif file_type == "json":
            df = spark.read.option("multiline", "true").json(s3_path)
        else:
            raise RuntimeError(f"Unsupported file type: {file_type}")

        if df.rdd.isEmpty():
            raise ValueError(f"{file_type.upper()} read returned empty DataFrame")

        logger.info(f"# [SUCCESS]: Read {file_type.upper()} with {len(df.columns)} columns and {df.count()} records.")
        return df

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"# [ERROR]: Failed to read from S3: {e}")
        raise RuntimeError(f"Error reading from {s3_path}: {e}")


def _detect_file_type_in_s3(s3_path: str) -> str:
    """
    Detects the file type (csv, json, parquet) from a given S3 path folder.

    Args:
        s3_path (str): S3 path to the folder (e.g. 's3://bucket/path/fecha_eventos=2025-09/').

    Returns:
        str: Detected file type ('csv', 'json').

    Raises:
        FileNotFoundError: If no files are found.
        RuntimeError: If no supported file type is found.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError("Invalid S3 path. Must start with 's3://'")

    s3_path = s3_path.rstrip("/") + "/"
    bucket = s3_path.split("/")[2]
    prefix = "/".join(s3_path.split("/")[3:])

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        raise FileNotFoundError(f"No files found in {s3_path}")

    for obj in response["Contents"]:
        key = obj["Key"].lower()
        if key.endswith(".csv"):
            return "csv"
        elif key.endswith(".json"):
            return "json"

    raise RuntimeError(f"No supported file type (.csv, .json) found in {s3_path}")


def get_object(bucket: str, key: str) -> bytes:
    """
    Retrieves an object from S3.

    Args:
        bucket (str): The S3 bucket name.
        key (str): The S3 object key.

    Returns:
        bytes: The content of the S3 object.

    Raises:
        RuntimeError: If there is an error retrieving the object.
    """
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket = bucket, Key = key)
        return response['Body'].read()
    except Exception as e:
        logger.error(f"# [ERROR]: Failed to get object from S3: {e}")
        raise RuntimeError(f"Error getting object {key} from bucket {bucket}: {e}")