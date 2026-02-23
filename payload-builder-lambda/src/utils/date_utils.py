"""
Module that contains different transversal utility functions used in the project.
"""
from datetime import date, datetime, timedelta
from src.config.logger import logger
from typing import Tuple
import pytz

@staticmethod
def get_today_frozen_date() -> date:
    """
    Get today's date in the "America/Bogota" timezone.

    Returns:
        date: Today's date in "America/Bogota" timezone.
    """
    tz = pytz.timezone("America/Bogota")
    return datetime.now(tz).date()


@staticmethod
def get_incremental_dates(date_type: str) -> Tuple[str, str]:
    """
    Generate a tuple of dates based on the provided date_type.

    Args:
        date_type (str): The type of date precision ('DAY_PRECISION' or 'MONTH_PRECISION').

    Returns:
        Tuple[str, str]: A tuple of strings representing dates formatted as 'yyyy-MM-dd'.  
    """
    try:
        today = get_today_frozen_date()
        if date_type == 'DAY_PRECISION':
            day_before = today - timedelta(days = 1)
            return day_before.strftime("%Y-%m-%d"), day_before.strftime("%Y-%m-%d")
        elif date_type == 'MONTH_PRECISION':
            first_day_current_month = today.replace(day = 1)
            last_day_previous_month = first_day_current_month - timedelta(days = 1)
            first_day_previous_month = last_day_previous_month.replace(day = 1)
            return first_day_previous_month.strftime("%Y-%m-%d"), last_day_previous_month.strftime("%Y-%m-%d")
        else:
            raise ValueError("Invalid date_type. Expected 'DAY_PRECISION' or 'MONTH_PRECISION'.")
        
    except Exception as e:
        logger.error(f"[ERROR] Error in get_incremental_dates: {e}")
        raise Exception(f"Error in get_incremental_dates: {e}")
    

@staticmethod
def get_full_dates(start: str, end: str, date_type: str) -> Tuple[str, str]:
    """
    Check and parse dates for full process based on the provided date_type.

    Args:
        start (str): The start date as a string.
        end (str): The end date as a string.
        date_type (str): The type of date precision ('DAY_PRECISION' or 'MONTH_PRECISION').

    Returns:
        Tuple[str, str]: A tuple representing the full date range.
    """
    try:
        if date_type == 'MONTH_PRECISION':
            start_date = datetime.strptime(start, "%Y-%m").replace(day = 1)
            end_date = datetime.strptime(end, "%Y-%m")
            if end_date.month == 12:
                end_date = end_date.replace(year = end_date.year + 1, month = 1)
            else:
                end_date = end_date.replace(month = end_date.month + 1)
            end_date = end_date - timedelta(days = 1)
        elif date_type == 'DAY_PRECISION':
            start_date = datetime.strptime(start, "%Y-%m-%d")
            end_date = datetime.strptime(end, "%Y-%m-%d")
        else:
            logger.error("[ERROR] Invalid date_type. Expected 'MONTH_PRECISION' or 'DAY_PRECISION'.")
            raise ValueError("Invalid date_type. Expected 'MONTH_PRECISION' or 'DAY_PRECISION'.")

        if start_date > end_date:
            logger.error("[ERROR] Start date must be before or equal to end date")
            raise ValueError("Start date must be before or equal to end date")

        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    
    except Exception as e:
        logger.error(f"[ERROR] An error occurred in get_full_dates: {e}")
        raise Exception(f"An error occurred in get_full_dates: {e}")