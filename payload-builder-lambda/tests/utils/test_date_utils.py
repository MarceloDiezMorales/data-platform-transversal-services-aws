from src.utils.date_utils import get_incremental_dates, get_today_frozen_date, get_full_dates
from unittest.mock import patch
from datetime import date
import pytest

@patch('src.utils.date_utils.get_today_frozen_date')
def test_get_incremental_dates_day_precision(mock_today):
    mock_today.return_value = date(2023, 6, 15)
    
    start_date, end_date = get_incremental_dates('DAY_PRECISION')
    
    assert start_date == "2023-06-14"
    assert end_date == "2023-06-14"


@patch('src.utils.date_utils.get_today_frozen_date')
def test_get_incremental_dates_month_precision(mock_today):
    mock_today.return_value = date(2023, 6, 15)
    
    start_date, end_date = get_incremental_dates('MONTH_PRECISION')
    
    assert start_date == "2023-05-01"
    assert end_date == "2023-05-31"


@patch('src.utils.date_utils.get_today_frozen_date')
def test_get_incremental_dates_month_precision_january(mock_today):
    mock_today.return_value = date(2023, 1, 15)
    
    start_date, end_date = get_incremental_dates('MONTH_PRECISION')
    
    assert start_date == "2022-12-01"
    assert end_date == "2022-12-31"


@patch('src.utils.date_utils.get_today_frozen_date')
def test_get_incremental_dates_month_precision_march(mock_today):
    mock_today.return_value = date(2023, 3, 15)
    
    start_date, end_date = get_incremental_dates('MONTH_PRECISION')
    
    assert start_date == "2023-02-01"
    assert end_date == "2023-02-28"


@patch('src.utils.date_utils.get_today_frozen_date')
def test_get_incremental_dates_month_precision_leap_year(mock_today):
    mock_today.return_value = date(2024, 3, 15)
    
    start_date, end_date = get_incremental_dates('MONTH_PRECISION')
    
    assert start_date == "2024-02-01"
    assert end_date == "2024-02-29"


@patch('src.utils.date_utils.get_today_frozen_date')
def test_get_incremental_dates_invalid_type(mock_today):
    mock_today.return_value = date(2023, 6, 15)
    
    with pytest.raises(Exception, match="Error in get_incremental_dates: Invalid date_type"):
        get_incremental_dates('INVALID_TYPE')


@patch('src.utils.date_utils.datetime')
def test_get_today_frozen_date(mock_datetime):
    mock_now = mock_datetime.now.return_value
    mock_now.date.return_value = date(2023, 11, 18)
    
    result = get_today_frozen_date()
    
    assert result == date(2023, 11, 18)


class TestGetFullDates:
    """Tests for get_full_dates function."""

    def test_get_full_dates_day_precision_single_day(self):
        """Test day precision with same start and end date."""
        start_date, end_date = get_full_dates("2023-06-15", "2023-06-15", "DAY_PRECISION")
        
        assert start_date == "2023-06-15"
        assert end_date == "2023-06-15"

    def test_get_full_dates_day_precision_multiple_days(self):
        """Test day precision with different start and end dates."""
        start_date, end_date = get_full_dates("2023-06-15", "2023-06-20", "DAY_PRECISION")
        
        assert start_date == "2023-06-15"
        assert end_date == "2023-06-20"

    def test_get_full_dates_day_precision_across_months(self):
        """Test day precision across month boundaries."""
        start_date, end_date = get_full_dates("2023-06-25", "2023-07-05", "DAY_PRECISION")
        
        assert start_date == "2023-06-25"
        assert end_date == "2023-07-05"

    def test_get_full_dates_day_precision_across_years(self):
        """Test day precision across year boundaries."""
        start_date, end_date = get_full_dates("2023-12-28", "2024-01-05", "DAY_PRECISION")
        
        assert start_date == "2023-12-28"
        assert end_date == "2024-01-05"

    def test_get_full_dates_month_precision_single_month(self):
        """Test month precision with same start and end month."""
        start_date, end_date = get_full_dates("2023-06", "2023-06", "MONTH_PRECISION")
        
        assert start_date == "2023-06-01"
        assert end_date == "2023-06-30"

    def test_get_full_dates_month_precision_multiple_months(self):
        """Test month precision with different start and end months."""
        start_date, end_date = get_full_dates("2023-03", "2023-06", "MONTH_PRECISION")
        
        assert start_date == "2023-03-01"
        assert end_date == "2023-06-30"

    def test_get_full_dates_month_precision_february_non_leap(self):
        """Test month precision with February in non-leap year."""
        start_date, end_date = get_full_dates("2023-02", "2023-02", "MONTH_PRECISION")
        
        assert start_date == "2023-02-01"
        assert end_date == "2023-02-28"

    def test_get_full_dates_month_precision_february_leap(self):
        """Test month precision with February in leap year."""
        start_date, end_date = get_full_dates("2024-02", "2024-02", "MONTH_PRECISION")
        
        assert start_date == "2024-02-01"
        assert end_date == "2024-02-29"

    def test_get_full_dates_month_precision_december(self):
        """Test month precision with December."""
        start_date, end_date = get_full_dates("2023-12", "2023-12", "MONTH_PRECISION")
        
        assert start_date == "2023-12-01"
        assert end_date == "2023-12-31"

    def test_get_full_dates_month_precision_across_years(self):
        """Test month precision across year boundaries."""
        start_date, end_date = get_full_dates("2023-11", "2024-02", "MONTH_PRECISION")
        
        assert start_date == "2023-11-01"
        assert end_date == "2024-02-29"

    def test_get_full_dates_month_precision_full_year(self):
        """Test month precision for full year."""
        start_date, end_date = get_full_dates("2023-01", "2023-12", "MONTH_PRECISION")
        
        assert start_date == "2023-01-01"
        assert end_date == "2023-12-31"

    def test_get_full_dates_invalid_date_type(self):
        """Test that invalid date_type raises Exception."""
        with pytest.raises(Exception, match="An error occurred in get_full_dates: Invalid date_type"):
            get_full_dates("2023-06-15", "2023-06-20", "INVALID_TYPE")

    def test_get_full_dates_start_after_end_day_precision(self):
        """Test that start date after end date raises Exception."""
        with pytest.raises(Exception, match="An error occurred in get_full_dates: Start date must be before or equal to end date"):
            get_full_dates("2023-06-20", "2023-06-15", "DAY_PRECISION")

    def test_get_full_dates_start_after_end_month_precision(self):
        """Test that start month after end month raises Exception."""
        with pytest.raises(Exception, match="An error occurred in get_full_dates: Start date must be before or equal to end date"):
            get_full_dates("2023-06", "2023-03", "MONTH_PRECISION")

    def test_get_full_dates_invalid_day_format(self):
        """Test that invalid date format raises exception."""
        with pytest.raises(Exception):
            get_full_dates("2023/06/15", "2023/06/20", "DAY_PRECISION")

    def test_get_full_dates_invalid_month_format(self):
        """Test that invalid month format raises exception."""
        with pytest.raises(Exception):
            get_full_dates("2023/06", "2023/07", "MONTH_PRECISION")

    def test_get_full_dates_month_precision_january_to_december(self):
        """Test month precision from January to December."""
        start_date, end_date = get_full_dates("2023-01", "2023-12", "MONTH_PRECISION")
        
        assert start_date == "2023-01-01"
        assert end_date == "2023-12-31"

    def test_get_full_dates_day_precision_leap_year_february(self):
        """Test day precision with leap year February dates."""
        start_date, end_date = get_full_dates("2024-02-28", "2024-02-29", "DAY_PRECISION")
        
        assert start_date == "2024-02-28"
        assert end_date == "2024-02-29"
