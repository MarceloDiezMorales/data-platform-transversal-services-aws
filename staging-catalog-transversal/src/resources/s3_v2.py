"""
Module for reading data from S3 into Spark DataFrames or retrieving S3 objects.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    TimestampType,
)
from pyspark.sql.functions import lit
from src.config.logger import logger
from typing import Optional, Tuple
from io import BytesIO
import pandas as pd
import boto3

def read_data_from_s3(
    spark: SparkSession,
    s3_path: str,
    header: bool = True,
    delimiter: str = ";",
    excel_sheet: Optional[str] = None,
    add_entity: bool = False
) -> DataFrame:
    """
    Reads a file from the given S3 path and returns a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session object.
        s3_path (str): S3 path to the file or folder (e.g. 's3://bucket/path/').
        header (bool): Whether the CSV file has a header row. Default is True.
        delimiter (str): Delimiter for CSV files. Default is ';'.
        excel_sheet (Optional[str]): Excel sheet name or index when reading .xlsx files.
        add_entity (bool): Whether to add an "Entidad" column inferred from the path.

    Returns:
        DataFrame: Spark DataFrame containing the data from the S3 file.

    Raises:
        ValueError: If the file is empty or invalid.
        RuntimeError: If there is an error reading the file from S3.
    """
    file_type, sample_path = _detect_file_type_in_s3(s3_path)
    logger.info(f"[INFO] Detected file type '{file_type}' for path: {s3_path} (sample: {sample_path})")
    source_path = sample_path or s3_path

    try:
        if file_type == "csv":
            df = (
                spark.read
                .option("header", str(header).lower())
                .option("inferSchema", "true")
                .option("delimiter", delimiter)
                .option("nullValue", r"\N")
                .csv(s3_path)
            )
        elif file_type == "json":
            df = (
                spark.read
                .option("multiline", "true")
                .json(s3_path)
            )
        elif file_type == "excel":
            df = read_excel_from_s3(spark, source_path, excel_sheet, header)
            if add_entity:
                # Validar si ya existe la columna (case-insensitive)
                cols_lower = {c.lower() for c in df.columns}
                if "entidad" not in cols_lower:
                    entity = _extract_entity_from_path(source_path)
                    if entity:
                        df = df.withColumn("Entidad", lit(entity))
                else:
                    logger.info("[INFO] Column 'Entidad' already exists in the dataset. Skipping auto-fill.")
        else:
            raise RuntimeError(f"Unsupported file type: {file_type}")
        logger.info(f"[SUCCESS] Read {file_type.upper()} with {len(df.columns)} columns and {df.count()} records.")
        return df
    except Exception as e:
        logger.error(f"[ERROR] Failed to read from S3: {e}")
        raise RuntimeError(f"Error reading from {s3_path}: {e}")


def _detect_file_type_in_s3(s3_path: str) -> Tuple[str, Optional[str]]:
    """
    Detects the file type (csv, json, excel) from a given S3 path folder.

    Args:
        s3_path (str): S3 path to the folder (e.g. 's3://bucket/path/fecha_eventos=2025-09/').

    Returns:
        Tuple[str, Optional[str]]: Detected file type and a sample file path.

    Raises:
        FileNotFoundError: If no files are found.
        RuntimeError: If no supported file type is found.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError("Invalid S3 path. Must start with 's3://'")

    s3_path = s3_path.rstrip("/")
    bucket = s3_path.split("/")[2]
    prefix = "/".join(s3_path.split("/")[3:])
    lower_path = s3_path.lower()
    if not (lower_path.endswith(".csv") or lower_path.endswith(".json") or lower_path.endswith(".xlsx")):
        prefix = prefix + "/"

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        raise FileNotFoundError(f"No files found in {s3_path}")

    for obj in response["Contents"]:
        key = obj["Key"]
        key_lower = key.lower()
        sample_path = f"s3://{bucket}/{key}"
        if key_lower.endswith(".csv"):
            return "csv", sample_path
        elif key_lower.endswith(".json"):
            return "json", sample_path
        elif key_lower.endswith(".xlsx"):
            return "excel", sample_path

    raise RuntimeError(f"No supported file type (.csv, .json, .xlsx) found in {s3_path}")


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
        logger.error(f"[ERROR] Failed to get object from S3: {e}")
        raise RuntimeError(f"Error getting object {key} from bucket {bucket}: {e}")
    
def _extract_entity_from_path(s3_path: str) -> Optional[str]:
    """
    Infers entity name from the S3 path or filename.
    - Prefers folder pattern .../<entity>/file.ext (penultimate segment)
    - Fallback: filename like <entity>_YYYY-MM.xlsx
    """
    parts = s3_path.strip("/").split("/")
    if len(parts) >= 2:
        maybe_entity = parts[-1]
        # If the filename has pattern entity_YYYY-MM..., take the prefix before the first underscore
        if "_" in maybe_entity:
            return maybe_entity.split("_", 1)[0]
        # If the penultimate segment carries the entity (e.g., .../comisionista/mes=...), use that
        maybe_entity = parts[-2]
        if "=" in maybe_entity:
            maybe_entity = maybe_entity.split("=", 1)[-1]
        if maybe_entity:
            return maybe_entity
    return None

def read_excel_from_s3(
    spark: SparkSession,
    s3_path: str,
    sheet_name: Optional[str],
    header: bool = True,
) -> DataFrame:
    """
    Reads a specific sheet from an Excel file stored in S3.
    """
    try:
        if not s3_path.startswith("s3://"):
            raise ValueError("Invalid S3 path. Must start with 's3://'")

        # Extract bucket and key
        parts = s3_path[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 path: {s3_path}")
        bucket, key = parts[0], parts[1]

        # Download file and read with pandas to avoid external Spark Excel jars
        body = get_object(bucket, key)

        sheet = 0 if not sheet_name else sheet_name
        pdf = pd.read_excel(BytesIO(body), sheet_name=sheet, header=0 if header else None)
        original_dtypes = pdf.dtypes.to_dict()
        # Normalize NaN/NaT to None so Spark treats empty columns as nulls.
        pdf = pdf.where(pd.notna(pdf), None)
        for col in pdf.columns:
            if pdf[col].isna().all():
                pdf[col] = None
            # Spark expects native datetime, not pandas Timestamp
            if str(pdf[col].dtype) == "datetime64[ns]":
                pdf[col] = pdf[col].dt.to_pydatetime()
            elif pdf[col].dtype == "object":
                pdf[col] = pdf[col].apply(
                    lambda v: v.to_pydatetime() if isinstance(v, pd.Timestamp) else v
                )

        schema_fields = []
        for col in pdf.columns:
            series = pdf[col]
            if series.isna().all():
                spark_type = StringType()
            else:
                dtype = original_dtypes.get(col)
                kind = getattr(dtype, "kind", None)
                if kind in ("i", "u"):
                    spark_type = LongType()
                elif kind == "f":
                    spark_type = DoubleType()
                elif kind == "b":
                    spark_type = BooleanType()
                elif kind == "M":
                    spark_type = TimestampType()
                else:
                    spark_type = StringType()
            schema_fields.append(StructField(col, spark_type, True))

        schema = StructType(schema_fields)
        data_records = pdf.to_dict(orient="records")
        for record in data_records:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.to_pydatetime()
        return spark.createDataFrame(data_records, schema=schema)
    except Exception as e:
        logger.error(f"[ERROR] Failed to read Excel sheet '{sheet_name}' from S3: {e}")
        raise RuntimeError(f"Error reading Excel from {s3_path}: {e}")
