"""
Schema utilities for building dynamic StructType definitions and extracting non-nullable fields from schema metadata.
"""
from pyspark.sql.types import (
    StringType, BooleanType, LongType, DoubleType, IntegerType,
    FloatType, ShortType, ByteType, DateType, TimestampType,
    DecimalType, BinaryType, ArrayType, MapType, StructType, StructField
)
from src.resources.s3 import get_object
from src.config.logger import logger
import json
from typing import Optional

def build_struct_from_schema(schema_fields: list) -> StructType:
    """
    Build a dynamic StructType schema from a list of field definitions.

    Each element in `schema_fields` must be a dictionary containing:
        - "name" (str): The column name.
        - "type" (str): The data type (e.g., "string", "int", "double").
        - "nullable" (bool, optional): Indicates if the column allows null values.

    Args:
        schema_fields (list): List of dictionaries describing each column.

    Returns:
        StructType: A Spark StructType object representing the schema.

    Raises:
        ValueError: If `schema_fields` is empty.
        KeyError: If a required key ("name" or "type") is missing.
        RuntimeError: If an unexpected error occurs during schema construction.
    """
    if not schema_fields:
        logger.error("[SCHEMA BUILD] The 'schema_fields' list is empty.")
        raise ValueError("The 'schema_fields' list is empty. Cannot build schema.")

    try:
        type_mapping = {
            "string": StringType(),
            "boolean": BooleanType(),
            "bool": BooleanType(),
            "long": LongType(),
            "bigint": LongType(),
            "int": IntegerType(),
            "integer": IntegerType(),
            "short": ShortType(),
            "byte": ByteType(),
            "float": FloatType(),
            "double": DoubleType(),
            "decimal": DecimalType(38, 10),
            "binary": BinaryType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "datetime": TimestampType(),
            "array": ArrayType(StringType()),
            "map": MapType(StringType(), StringType()),
            "struct": StructType(),
            "number": DoubleType(),
            "numeric": DoubleType(),
            "text": StringType(),
            "char": StringType(),
            "varchar": StringType(),
        }

        struct_fields = []
        for field in schema_fields:
            name = field.get("name")
            type_name = field.get("type", "").lower().strip()

            if not name or not type_name:
                raise KeyError(f"Field missing 'name' or 'type': {field}")

            data_type = type_mapping.get(type_name, StringType())
            nullable = field.get("nullable", True)

            struct_fields.append(StructField(name, data_type, nullable))

        logger.info("[SCHEMA BUILD] Successfully built StructType schema.")
        return StructType(struct_fields)

    except KeyError as e:
        logger.error(f"[SCHEMA BUILD] Missing required key in schema definition: {e}")
        raise KeyError(f"Missing required key in schema definition: {e}")
    except Exception as e:
        logger.error(f"[SCHEMA BUILD] Failed to build StructType: {e}")
        raise RuntimeError(f"Error while building StructType: {e}")


def get_non_nullable_fields(schema_fields: list) -> list:
    """
    Return a list of column names that have 'nullable=False' in the schema definition.

    Args:
        schema_fields (list): List of dictionaries describing schema fields.

    Returns:
        list: A list of non-nullable column names.

    Raises:
        ValueError: If `schema_fields` is empty.
        RuntimeError: If an unexpected error occurs during processing.
    """
    if not schema_fields:
        raise ValueError("The 'schema_fields' list is empty. Cannot continue.")

    try:
        non_nullable_fields = [
            field["name"]
            for field in schema_fields
            if not field.get("nullable", True)
        ]

        logger.info(f"[NON-NULLABLE] Found {len(non_nullable_fields)} non-nullable fields.")
        return non_nullable_fields

    except Exception as e:
        logger.error(f"[NON-NULLABLE] Error while processing schema_fields: {e}")
        raise RuntimeError(f"Error while processing schema_fields: {e}")


def get_schema_definition(bucket_name_schema: str, key_input: str, spark, excel_sheet: Optional[str] = None) -> list[dict]:
    """
    Parses a schema definition from a JSON string.
    
    Args:
        bucket_name_schema (str): S3 bucket name where the schema is stored.
        key_input (str): S3 key/path to the schema JSON file.
        spark: Spark session object.

    Returns:
        list[dict]: List of dictionaries describing schema fields.

    Raises:
        FileNotFoundError: If the schema content is empty or not provided.
        ValueError: If the schema does not contain any defined fields or is invalid.
        RuntimeError: If an unexpected error occurs during parsing.
    """
    try:
        logger.info("[PARSE_SCHEMA] Parsing schema definition from JSON content.")
        if excel_sheet:
            normalized_sheet = "_".join(excel_sheet.strip().split()).lower()
            key = f"{key_input}/{normalized_sheet}/schema.json"
        else:
            key = f"{key_input}/schema.json"
        schema_raw = get_object(bucket_name_schema, key)
        schema_str = schema_raw.decode("utf-8")
        schema_json = json.loads(schema_str)   
        
        if not schema_json:
            raise FileNotFoundError("Schema content is empty or not provided.")

        fields = schema_json.get("fields", [])
        if not fields:
            raise ValueError("The provided schema does not contain any defined fields.")

        logger.info("[PARSE_SCHEMA] Successfully parsed schema with %d fields.", len(fields))
        return fields
    except FileNotFoundError as e:
        logger.error("[PARSE_SCHEMA] %s", e)
        raise
    except json.JSONDecodeError as e:
        logger.error("[PARSE_SCHEMA] Invalid JSON format: %s", e)
        raise ValueError("The provided content is not valid JSON.") from e
    except ValueError as e:
        logger.error("[PARSE_SCHEMA] %s", e)
        raise
    except Exception as e:
        logger.error("[PARSE_SCHEMA] Unexpected error: %s", e)
        raise RuntimeError(f"Unexpected error while parsing schema: {str(e)}") from e