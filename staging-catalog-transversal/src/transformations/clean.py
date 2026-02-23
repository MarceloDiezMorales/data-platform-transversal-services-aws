"""
Module that provides DataFrame transformation utilities for Spark ETL pipelines.
"""
from pyspark.sql import DataFrame, functions as F
from src.config.logger import logger
from typing import Union, Optional
import json
import re

def _snake_case(name: str) -> str:
    """
    Converts a column name to snake_case format.
    
    Args:
        name (str): Original column name.

    Returns:
        str: Column name in snake_case format.

    Raises:
        RuntimeError: If an error occurs during conversion.
    """
    try:
        name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
        return name.lower()
    except Exception as e:
        logger.error("[SNAKE_CASE] Error converting '%s' to snake_case: %s", name, e)
        raise RuntimeError(f"Error converting '{name}' to snake_case: {str(e)}") from e


def rename_columns_to_snake_case(df: DataFrame, schema_fields: list) -> DataFrame:
    """
    Renames all columns in a DataFrame to snake_case format.
    
    Args:
        df (DataFrame): Input PySpark DataFrame.
        schema_fields (list): List of dictionaries describing schema fields.

    Returns:
        DataFrame: DataFrame with columns renamed to snake_case.

    Raises:
        RuntimeError: If an error occurs during renaming.
    """
    try:
        logger.info("[RENAME] Starting column renaming to snake_case.")
        select_expr = [
            f"`{field['name']}` AS `{field['converted_name'] if field.get('converted_name') else _snake_case(field['name'])}`"
            for field in schema_fields
            if field['name'] in df.columns
        ]
        df = df.selectExpr(*select_expr)
        logger.info("[RENAME] Column renaming completed successfully.")
        return df
    except Exception as e:
        logger.error("[RENAME] Error renaming columns to snake_case: %s", e)
        raise RuntimeError(f"Error renaming columns to snake_case: {str(e)}") from e


def parse_list_param(param: Union[str, list, None]) -> Optional[list]:
    """
    Parses a string or list into a cleaned list of strings.
    
    Args:
        param (Union[str, list, None]): Input parameter to parse.
    
    Returns:
        Optional[list]: Cleaned list of strings or None if input is empty/None-like.

    Raises:
        RuntimeError: If an error occurs during parsing.
    """
    try:
        if param in [None, "", "''", '""', "None", "null", "NULL"]:
            logger.info("[PARSE_LIST] Parameter is empty or None-like, returning None.")
            return None

        if isinstance(param, str):
            items = [x.strip() for x in param.split(",") if x.strip()]
            logger.info("[PARSE_LIST] Parsed string into list: %s", items)
            return items if items else None

        if isinstance(param, list):
            items = [x.strip() for x in param if isinstance(x, str) and x.strip()]
            logger.info("[PARSE_LIST] Cleaned existing list: %s", items)
            return items if items else None

        logger.info("[PARSE_LIST] Invalid parameter type (%s), returning None.", type(param))
        return None
    except Exception as e:
        logger.error("[PARSE_LIST] Error parsing parameter '%s': %s", param, e)
        raise RuntimeError(f"Error parsing parameter '{param}': {str(e)}") from e


def _normalize_type(data_type: str) -> str:
    """
    Normalizes a data type string to a standard Spark type.
    
    Args:
        data_type (str): Original data type string.

    Returns:
        str: Normalized Spark data type string.

    Raises:
        ValueError: If the data type is invalid or empty.
        RuntimeError: If an error occurs during normalization.
    """
    try:
        if not data_type or not isinstance(data_type, str):
            raise ValueError("Invalid or empty data type provided.")

        t = data_type.lower().strip()

        if t.startswith("decimal"):
            return t
        elif t in ["int", "integer"]:
            return "int"
        elif t in ["bigint", "long"]:
            return "bigint"
        elif t in ["double", "float", "number", "numeric"]:
            return "double"
        elif t in ["boolean", "bool"]:
            return "boolean"
        elif t in ["timestamp", "datetime"]:
            return "timestamp"
        elif t == "date":
            return "date"
        else:
            return "string"
    except Exception as e:
        logger.error("[NORMALIZE_TYPE] Error normalizing data type '%s': %s", data_type, e)
        raise RuntimeError(f"Error normalizing data type '{data_type}': {str(e)}") from e


def cast_columns_from_schema(df: DataFrame, schema_fields: list[dict], file_name_excel: Optional[str] = None) -> DataFrame:
    """
    Casts DataFrame columns according to the provided schema definition.
    
    Args:
        df (DataFrame): Input PySpark DataFrame.
        schema_fields (list[dict]): List of dictionaries describing schema fields.

    Returns:
        DataFrame: DataFrame with columns casted to the specified types.

    Raises:
        ValueError: If the schema is empty or invalid.
        RuntimeError: If an error occurs during casting.
    """
    try:
        if not schema_fields:
            raise ValueError("Schema is empty. Cannot perform casting.")

        logger.info("[CAST COLUMNS] Starting column type casting from schema.")
        for field in schema_fields:
            col_name = field.get("name")
            converted_type = field.get("converted_type")

            if not col_name:
                raise ValueError(f"Schema field missing 'name': {field}")
            if not converted_type:
                logger.debug("[CAST COLUMNS] Skipping column '%s', no 'converted_type'.", col_name)
                continue

            normalized_type = _normalize_type(converted_type)
            if normalized_type in ("date", "timestamp"):
                df = df.withColumn(
                    col_name,
                    safe_cast_date_or_ts(col_name, normalized_type)
                )
            else:
                df = df.withColumn(col_name, F.col(col_name).cast(normalized_type))
        logger.info("[CAST COLUMNS] All applicable columns successfully casted.")
        return df
    except Exception as e:
        logger.error("[CAST COLUMNS] Error casting columns from schema: %s", e)
        raise RuntimeError(f"Error casting columns from schema: {str(e)}") from e


def parse_schema_definition(file_content: str) -> list[dict]:
    """
    Parses a schema definition from a JSON string.
    
    Args:
        file_content (str): JSON string containing the schema definition.

    Returns:
        list[dict]: List of dictionaries describing schema fields.

    Raises:
        FileNotFoundError: If the schema content is empty or not provided.
        ValueError: If the schema does not contain any defined fields or is invalid.
        RuntimeError: If an unexpected error occurs during parsing.
    """
    try:
        logger.info("[PARSE_SCHEMA] Parsing schema definition from JSON content.")
        if not file_content:
            raise FileNotFoundError("Schema content is empty or not provided.")

        schemas_dict = json.loads(file_content)
        fields = schemas_dict.get("fields", [])
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


def add_partition_column(df: DataFrame, column_name: str, value: str) -> DataFrame:
    """
    Adds a new partition column with a fixed value to the DataFrame.

    Args:
        df (DataFrame): Input Spark DataFrame.
        column_name (str): Name of the column to add.
        column_name (str): Name of the column to add.
        value (str): Value to assign to all rows in that column.

    Returns:
        DataFrame: DataFrame with the new column added.

    Raises:
        RuntimeError: If an error occurs while adding the partition column.
    """
    logger.info(f"[INFO] Adding partition column '{column_name}' with value '{value}'")
    return df.withColumn(column_name, F.lit(value))


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Cleans DataFrame column names by removing special characters,
    accents, and spaces. Converts everything to lowercase.
    Special rule: if the column contains 'año', it is replaced with 'anio'.

    Args:
        df (DataFrame): Input PySpark DataFrame.

    Returns:
        DataFrame: DataFrame with cleaned column names.

    Raises:
        Exception: If an error occurs during column name normalization.
    """
    try:
        logger.info("[INFO] Starting normalization of DataFrame column names.")
        logger.info(f"Original columns: {df.columns}")

        new_columns = []
        for col in df.columns:
            original_col = col

            clean_col = (
                re.sub(r"[áÁàÀäÄ]", "a",
                re.sub(r"[éÉèÈëË]", "e",
                re.sub(r"[íÍìÌïÏ]", "i",
                re.sub(r"[óÓòÒöÖ]", "o",
                re.sub(r"[úÚùÙüÜ]", "u",
                re.sub(r"[ñÑ]", "n", col)))))))    

            clean_col = re.sub(r"\bano\b", "anio", clean_col, flags=re.IGNORECASE)

            clean_col = re.sub(r"[^0-9a-zA-Z_]", "_", clean_col)

            clean_col = re.sub(r"_+", "_", clean_col).strip("_")

            if original_col != clean_col:
                logger.info(f"Renamed column '{original_col}' → '{clean_col}'")

            new_columns.append(clean_col)


        for old_col, new_col in zip(df.columns, new_columns):
            df = df.withColumnRenamed(old_col, new_col)

        logger.info(f"[INFO] Column normalization complete. Final columns: {new_columns}")

        return df

    except Exception as e:
        logger.error(f"[ERROR] Error cleaning column names: {e}", exc_info=True)
        raise Exception(f"Error cleaning column names: {e}")

def _parse_es_ampm(col_expr):
    # 1) Reemplaza espacios raros (NBSP y NNBSP) por espacio normal
    c = F.regexp_replace(col_expr, u"[\u00A0\u202F]", " ")

    # 2) Compacta espacios
    c = F.trim(F.regexp_replace(c, r"\s+", " "))

    # 3) Normaliza a. m. / p. m. a AM/PM (sin depender de \s)
    c = F.regexp_replace(c, r"(?i)p\.\s*m\.", "PM")
    c = F.regexp_replace(c, r"(?i)a\.\s*m\.", "AM")
    c = F.regexp_replace(c, r"(?i)p\s*m", "PM")
    c = F.regexp_replace(c, r"(?i)a\s*m", "AM")

    return F.to_timestamp(c, "d/M/yyyy h:mm:ss a")



def safe_cast_date_or_ts(col_name: str, target: str):
    raw = F.col(col_name)

    if target == "date":
        casted = raw.cast("date")
        parsed = F.to_date(_parse_es_ampm(raw))
        # solo arregla cuando el cast fallo
        return F.when(raw.isNotNull() & casted.isNull(), parsed).otherwise(casted)

    if target == "timestamp":
        casted = raw.cast("timestamp")
        parsed = _parse_es_ampm(raw)
        return F.when(raw.isNotNull() & casted.isNull(), parsed).otherwise(casted)

    raise ValueError("target must be 'date' or 'timestamp'")
