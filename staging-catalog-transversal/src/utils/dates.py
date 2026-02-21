"""
Module with date utilities for manipulation and range generation with day or month precision.
"""
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, date
from src.config.logger import logger
from typing import List
import pytz

MONTH_PRECISION_FORMAT = "%Y-%m"
DAY_PRECISION_FORMAT = "%Y-%m-%d"

def get_today_frozen_date() -> date:
    """
    Get today's date in the "America/Bogota" timezone.

    Returns:
        date: Today's date in "America/Bogota" timezone.
    """
    tz = pytz.timezone("America/Bogota")
    return datetime.now(tz).date()


def parse_month(date_str: str) -> datetime:
    """
    Parse a string to a datetime object with month precision.
    
    Args:
        date_str (str): Date string in "YYYY-MM" or "YYYY-MM-DD" format.

    Returns:
        datetime: Parsed datetime object set to the first day of the month.
    """
    if len(date_str) == 10: 
        date_str = date_str[:7]
    if len(date_str) != 7:
        raise ValueError("[ERROR] MONTH_PRECISION requires format YYYY-MM")
    return datetime.strptime(date_str, MONTH_PRECISION_FORMAT).replace(day = 1)


def parse_day(date_str: str) -> datetime:
    """
    Parse a string to a datetime object with day precision.

    Args:
        date_str (str): Date string in "YYYY-MM-DD" format.

    Returns:
        datetime: Parsed datetime object.
    """
    if len(date_str) != 10:
        raise ValueError("[ERROR] DAY_PRECISION requires format YYYY-MM-DD")
    return datetime.strptime(date_str, DAY_PRECISION_FORMAT)


def get_previous_date(date_type: str) -> str:
    """
    Get the previous day or month based on date_type.
    
    Args:
        date_type (str): "MONTH_PRECISION" or "DAY_PRECISION".
    
    Returns:
        str: Previous date as a string in the appropriate format.
    """
    today = get_today_frozen_date()
    if date_type == "MONTH_PRECISION":
        first_day_current_month = today.replace(day = 1)
        previous_month = first_day_current_month - relativedelta(months = 1)
        return previous_month.strftime(MONTH_PRECISION_FORMAT)
    elif date_type == "DAY_PRECISION":
        yesterday = today - timedelta(days = 1)
        return yesterday.strftime(DAY_PRECISION_FORMAT)
    else:
        raise ValueError(f"[ERROR] Invalid date_type '{date_type}'")


def get_date_ranges(start: str, end: str, date_type: str) -> List[str]:
    """
    Generate a list of dates or months between `start` and `end` depending on date precision.

    Args:
        start (str): Start date string.
        end (str): End date string.
        date_type (str): "MONTH_PRECISION" or "DAY_PRECISION".

    Returns:
        List[str]: List of date strings in the specified format.
    """
    try:
        if date_type == "MONTH_PRECISION":
            current = parse_month(start)
            end_date = parse_month(end)
            delta = relativedelta(months = 1)
            fmt = MONTH_PRECISION_FORMAT
        elif date_type == "DAY_PRECISION":
            current = parse_day(start)
            end_date = parse_day(end)
            delta = timedelta(days = 1)
            fmt = DAY_PRECISION_FORMAT
        else:
            raise ValueError(f"[ERROR] Invalid date_type '{date_type}'")

        if current > end_date:
            raise ValueError("[ERROR] Start date cannot be greater than end date.")

        result = []
        while current <= end_date:
            result.append(current.strftime(fmt))
            current += delta

        logger.info(f"[INFO] Generated {len(result)} date ranges with precision {date_type}")
        return result
    except ValueError as ve:
        logger.error(f"[ERROR] {str(ve)}")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Failed to generate date ranges: {e}")
        raise
