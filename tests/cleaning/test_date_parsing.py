"""Tests for multiple date format parsing in the cleaning pipeline."""

import pandas as pd
import pytest
from data_pipeline.cleaning.clean_sales_data import _parse_multiple_date_formats


class TestDateParsing:
    """Test multiple date format parsing functionality."""

    def test_parse_multiple_date_formats_success(self):
        """Test that all supported date formats parse correctly to the same date."""
        expected_date = pd.Timestamp("2023-12-25")
        
        test_cases = [
            "2023-12-25",     # %Y-%m-%d
            "12/25/2023",     # %m/%d/%Y
            "25-12-2023",     # %d-%m-%Y
            "2023/12/25",     # %Y/%m/%d
        ]
        
        for date_str in test_cases:
            result = _parse_multiple_date_formats(date_str)
            assert result == expected_date, f"Failed to parse {date_str}"

    def test_parse_multiple_date_formats_invalid(self):
        """Test that invalid date strings return None."""
        invalid_cases = [
            "invalid",
            "13/25/2023",     # Invalid month
            "25/13/2023",     # Invalid month  
            "2023-13-25",     # Invalid month
            "",
            None,
            "2023/25/12",     # Would be ambiguous
        ]
        
        for date_str in invalid_cases:
            result = _parse_multiple_date_formats(date_str)
            assert result is None, f"Expected None for {date_str}, got {result}"

    def test_different_valid_dates(self):
        """Test parsing different valid dates in different formats."""
        test_cases = [
            ("2024-01-15", pd.Timestamp("2024-01-15")),
            ("01/15/2024", pd.Timestamp("2024-01-15")),
            ("15-01-2024", pd.Timestamp("2024-01-15")),
            ("2024/01/15", pd.Timestamp("2024-01-15")),
            ("2022-06-30", pd.Timestamp("2022-06-30")),
            ("06/30/2022", pd.Timestamp("2022-06-30")),
            ("30-06-2022", pd.Timestamp("2022-06-30")),
        ]
        
        for date_str, expected in test_cases:
            result = _parse_multiple_date_formats(date_str)
            assert result == expected, f"Failed to parse {date_str} correctly"

    def test_pandas_series_integration(self):
        """Test that the function works correctly when applied to a pandas Series."""
        test_df = pd.DataFrame({
            "sale_date": [
                "2023-12-25",     # %Y-%m-%d
                "12/25/2023",     # %m/%d/%Y
                "25-12-2023",     # %d-%m-%Y
                "2023/12/25",     # %Y/%m/%d
                "invalid",        # Should become NaT
                "",               # Should become NaT
                "2024-01-01",     # Another valid date
            ]
        })
        
        parsed_dates = test_df["sale_date"].apply(_parse_multiple_date_formats)
        
        # Check that first 4 dates are the same
        expected_christmas = pd.Timestamp("2023-12-25")
        assert parsed_dates.iloc[0] == expected_christmas
        assert parsed_dates.iloc[1] == expected_christmas  
        assert parsed_dates.iloc[2] == expected_christmas
        assert parsed_dates.iloc[3] == expected_christmas
        
        # Check invalid entries are NaT
        assert pd.isna(parsed_dates.iloc[4])
        assert pd.isna(parsed_dates.iloc[5])
        
        # Check another valid date
        assert parsed_dates.iloc[6] == pd.Timestamp("2024-01-01")
        
        # Check count of valid dates
        valid_count = parsed_dates.notna().sum()
        assert valid_count == 5

    def test_edge_cases(self):
        """Test edge cases for date parsing."""
        edge_cases = [
            ("2000-01-01", pd.Timestamp("2000-01-01")),  # Y2K date
            ("2020-02-29", pd.Timestamp("2020-02-29")),  # Leap year
            ("12-31-1999", None),  # MM-dd-yyyy format (not supported)
            ("31/12/1999", None),  # dd/MM/yyyy format (not supported)  
            ("1999-12-31", pd.Timestamp("1999-12-31")),  # Valid ISO format
        ]
        
        for date_str, expected in edge_cases:
            result = _parse_multiple_date_formats(date_str)
            if expected is None:
                assert result is None, f"Expected None for {date_str}, got {result}"
            else:
                assert result == expected, f"Failed to parse {date_str} correctly"