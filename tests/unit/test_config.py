"""
Unit tests for extract/config.py

These tests verify the NHS financial year and month code
logic is correct. This logic is critical — if it produces
wrong month codes, the extractor downloads the wrong files.

Run with: pytest tests/unit/ -v
"""

import pytest
from extract.config import get_nhs_month_code, get_financial_year_for_month


class TestGetNHSMonthCode:
    """Tests for the get_nhs_month_code function."""

    def test_january_2026(self):
        assert get_nhs_month_code(2026, 1) == "Jan26"

    def test_december_2025(self):
        assert get_nhs_month_code(2025, 12) == "Dec25"

    def test_april_2025(self):
        """April is the start of the NHS financial year."""
        assert get_nhs_month_code(2025, 4) == "Apr25"

    def test_march_2026(self):
        """March is the end of the NHS financial year."""
        assert get_nhs_month_code(2026, 3) == "Mar26"

    def test_all_months_produce_three_letter_prefix(self):
        """All month codes should start with a three-letter month name."""
        months = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]
        for i, expected_prefix in enumerate(months, 1):
            result = get_nhs_month_code(2026, i)
            assert result.startswith(expected_prefix), (
                f"Month {i} should start with {expected_prefix}, got {result}"
            )

    def test_two_digit_year_suffix(self):
        """Year suffix should always be two digits."""
        result = get_nhs_month_code(2026, 1)
        assert result[-2:] == "26"


class TestGetFinancialYearForMonth:
    """
    Tests for the get_financial_year_for_month function.

    NHS financial year runs April to March.
    April 2025 - March 2026 = '2025-26'
    """

    def test_january_is_in_previous_financial_year(self):
        """January 2026 is in financial year 2025-26."""
        assert get_financial_year_for_month(2026, 1) == "2025-26"

    def test_april_starts_new_financial_year(self):
        """April 2025 starts financial year 2025-26."""
        assert get_financial_year_for_month(2025, 4) == "2025-26"

    def test_march_ends_financial_year(self):
        """March 2026 ends financial year 2025-26."""
        assert get_financial_year_for_month(2026, 3) == "2025-26"

    def test_april_2024_starts_2024_25(self):
        """April 2024 starts financial year 2024-25."""
        assert get_financial_year_for_month(2024, 4) == "2024-25"

    def test_march_2025_ends_2024_25(self):
        """March 2025 ends financial year 2024-25."""
        assert get_financial_year_for_month(2025, 3) == "2024-25"

    def test_financial_year_format(self):
        """Financial year should be in format YYYY-YY."""
        result = get_financial_year_for_month(2025, 6)
        assert len(result) == 7
        assert result[4] == "-"
        assert result[:4].isdigit()
        assert result[5:].isdigit()