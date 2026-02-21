
"""
Module for validating PySpark DataFrames against a predefined schema and required fields.
"""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
    FloatType,
    ShortType,
    ByteType,
    DateType,
    TimestampType,
    DecimalType,
)
from pyspark.sql.functions import col
from src.config.logger import logger

def validate_dataframe(
    df: DataFrame,
    schema: StructType,
    required_fields: list,
    file_name_excel: str | None = None,
) -> None:
    """
    Validates the DataFrame by checking column existence, data types, and required fields.

    Args:
        df (DataFrame): The DataFrame to validate.
        schema (StructType): The schema to validate against.
        required_fields (list): List of required fields that should not contain null or empty values.
    """
    validate_columns_exist(df, schema)
    validate_column_data_types(df, schema, file_name_excel)
    validate_required_fields(df, required_fields)


def validate_required_fields(df: DataFrame, required_fields: list) -> None:
    """
    Validates that required fields in the DataFrame do not contain null or empty values.

    Args:
        df (DataFrame): The DataFrame to validate.
        required_fields (list): List of required fields that should not contain null or empty values.

    Raises:
        ValueError: If any required field contains null or empty values.
    """
    if not required_fields:
        logger.info("[INFO] No required fields to validate — skipping check.")
        return
    
    for field in required_fields:
        missing = df.filter(col(field).isNull()).count()
        if missing > 0:
            logger.error(f"[ERROR] Required field {field} has {missing} null or empty values.")
            raise ValueError(f"Required field {field} has {missing} null or empty values.")


def validate_columns_exist(df: DataFrame, schema: StructType) -> None:
    """
    Validates that all columns in the schema exist in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to validate.
        schema (StructType): The schema to validate against.

    Raises:
        ValueError: If any fields in the schema are missing from the DataFrame.
    """
    missing_fields = [field.name for field in schema.fields if field.name not in df.columns]
    if missing_fields:
        logger.error(f"[ERROR] Missing fields in DataFrame: {missing_fields}")
        raise ValueError(f"Missing fields in DataFrame: {missing_fields}")

def _is_numeric_type(dt) -> bool:
    return isinstance(dt, (LongType, DoubleType, IntegerType, FloatType, ShortType, ByteType, DecimalType))


def _is_compatible_type(expected, actual) -> bool:
    if isinstance(expected, StringType):
        return True
    if _is_numeric_type(expected):
        return _is_numeric_type(actual) or isinstance(actual, StringType)
    if isinstance(expected, (DateType, TimestampType)):
        return isinstance(actual, StringType)
    return False


def validate_column_data_types(
    df: DataFrame,
    schema: StructType,
    file_name_excel: str | None = None,
) -> None:
    """
    Validates that the data types of the DataFrame columns match the schema.

    Args:
        df (DataFrame): The DataFrame to validate.
        schema (StructType): The schema to validate against.

    Raises:
        TypeError: If any column's data type does not match the expected type in the schema.
    """
    for field in schema.fields:
        if field.name in df.columns:
            not_null_count = df.filter(col(field.name).isNotNull()).limit(1).count()
            if not_null_count == 0:
                logger.warning(f"[SKIPPED] Column '{field.name}' contains only NULLs. Skipping type validation.")
                continue
            actual_type = df.schema[field.name].dataType
            expected_type = field.dataType
            if actual_type != expected_type:
                if file_name_excel and _is_compatible_type(expected_type, actual_type):
                    logger.warning("[WARN] Excel relaxed type check for field %s. Expected %s, found %s.",field.name,expected_type,actual_type,)
                    continue
                logger.error(f"[ERROR] Data type mismatch for field {field.name}. Expected {field.dataType}, found {df.schema[field.name].dataType}.")
                raise TypeError(f"Data type mismatch for field {field.name}. Expected {field.dataType}, found {df.schema[field.name].dataType}.")