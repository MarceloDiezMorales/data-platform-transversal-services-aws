from types import SimpleNamespace
from datetime import datetime
import logging
import pytest
import sys

mock_logger = logging.getLogger("mock_logger")
mock_logger.info = lambda msg: None
mock_logger.error = lambda msg: None
sys.modules["src.config.logger"] = SimpleNamespace(logger=mock_logger)

from src.utils.dates import get_previous_date, get_date_ranges, parse_month, parse_day

def test_parse_month_valid_format():
    result = parse_month("2024-01")
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 1

def test_parse_month_from_day_format():
    result = parse_month("2024-03-15")
    assert result.year == 2024
    assert result.month == 3
    assert result.day == 1

def test_parse_month_invalid_length():
    with pytest.raises(ValueError, match=r"MONTH_PRECISION requires format YYYY-MM"):
        parse_month("2024-1")

def test_parse_month_invalid_format():
    with pytest.raises(ValueError):
        parse_month("invalid")

def test_parse_day_valid_format():
    result = parse_day("2024-01-15")
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15

def test_parse_day_invalid_length():
    with pytest.raises(ValueError, match=r"DAY_PRECISION requires format YYYY-MM-DD"):
        parse_day("2024-01")

def test_parse_day_invalid_format():
    with pytest.raises(ValueError):
        parse_day("2024-13-01")

def test_get_previous_date_month_precision():
    result = get_previous_date("MONTH_PRECISION")
    assert len(result) == 7  # Formato YYYY-MM
    datetime.strptime(result, "%Y-%m")  # Valida formato correcto

def test_get_previous_date_day_precision():
    result = get_previous_date("DAY_PRECISION")
    assert len(result) == 10  # Formato YYYY-MM-DD
    datetime.strptime(result, "%Y-%m-%d")

def test_get_previous_date_invalid_type():
    with pytest.raises(ValueError, match=r"Invalid date_type 'DAILY'"):
        get_previous_date("DAILY")

def test_get_date_ranges_month_precision():
    result = get_date_ranges("2024-01", "2024-03", "MONTH_PRECISION")
    assert result == ["2024-01", "2024-02", "2024-03"]

def test_get_date_ranges_day_precision():
    result = get_date_ranges("2024-01-01", "2024-01-03", "DAY_PRECISION")
    assert result == ["2024-01-01", "2024-01-02", "2024-01-03"]

def test_get_date_ranges_invalid_type():
    with pytest.raises(ValueError, match=r"Invalid date_type 'YEAR_PRECISION'"):
        get_date_ranges("2024-01", "2024-02", "YEAR_PRECISION")

def test_get_date_ranges_invalid_format_for_day():
    with pytest.raises(ValueError, match=r"DAY_PRECISION requires format YYYY-MM-DD"):
        get_date_ranges("2024-01", "2024-02", "DAY_PRECISION")

def test_get_date_ranges_start_after_end():
    with pytest.raises(ValueError, match=r"Start date cannot be greater than end date"):
        get_date_ranges("2024-02", "2024-01", "MONTH_PRECISION")

def test_get_date_ranges_single_month():
    result = get_date_ranges("2024-01", "2024-01", "MONTH_PRECISION")
    assert result == ["2024-01"]

def test_get_date_ranges_single_day():
    result = get_date_ranges("2024-01-15", "2024-01-15", "DAY_PRECISION")
    assert result == ["2024-01-15"]

def test_get_date_ranges_month_cross_year():
    result = get_date_ranges("2023-12", "2024-02", "MONTH_PRECISION")
    assert result == ["2023-12", "2024-01", "2024-02"]

def test_get_date_ranges_day_cross_month():
    result = get_date_ranges("2024-01-30", "2024-02-02", "DAY_PRECISION")
    assert result == ["2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02"]

def test_get_date_ranges_month_precision_with_day_format():
    result = get_date_ranges("2024-01-15", "2024-03-20", "MONTH_PRECISION")
    assert result == ["2024-01", "2024-02", "2024-03"]

def test_get_date_ranges_exception_handling():
    with pytest.raises(ValueError):
        get_date_ranges("invalid", "2024-02", "MONTH_PRECISION")
