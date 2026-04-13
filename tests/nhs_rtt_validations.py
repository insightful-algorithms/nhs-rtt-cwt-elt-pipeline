"""
NHS RTT Custom Data Validation Rules
======================================
Business-specific validation checks for NHS RTT data that go
beyond what Great Expectations provides out of the box.

These checks encode domain knowledge about NHS RTT data:
- How NHS England structures its data
- What values are clinically meaningful
- What anomalies indicate data quality issues vs real events

Run these validations after the Great Expectations suite,
before dbt models execute.
"""

import logging
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a single validation check."""

    check_name: str
    passed: bool
    severity: str  # 'error', 'warning', 'info'
    message: str
    affected_rows: int = 0
    details: dict = field(default_factory=dict)


class NHSRTTValidator:
    """
    Validates NHS RTT data against business rules derived from:
    - The NHS Constitution (18-week standard)
    - NHS England data quality guidance
    - Domain knowledge from NHS performance analysts
    """

    # The 92% standard — patients within 18 weeks as % of total incomplete
    RTT_18_WEEK_STANDARD = 92.0

    # Expected number of Integrated Care Boards in England
    # Changed from 42 to 42 after July 2022 ICB reorganisation
    EXPECTED_ICB_COUNT = 42

    # Treatment function codes that should always be present
    CORE_SPECIALTIES = {
        "C_110",  # Trauma and Orthopaedic — always highest volume
        "C_130",  # Ophthalmology — always high volume
        "C_100",  # General Surgery
        "C_120",  # Ear Nose and Throat
        "C_999",  # Total row — always present
    }

    def __init__(self, zip_path: Path):
        self.zip_path = zip_path
        self.df = self._load_data()

    def _load_data(self) -> pd.DataFrame:
        """Load CSV from ZIP into a DataFrame."""
        with zipfile.ZipFile(self.zip_path, "r") as zf:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            with zf.open(csv_files[0]) as f:
                return pd.read_csv(f, low_memory=False)

    def run_all_checks(self) -> List[ValidationResult]:
        """
        Run all validation checks and return results.
        Checks run in order — structural checks first,
        then business logic checks.
        """
        results = []

        results.append(self.check_row_count())
        results.append(self.check_column_count())
        results.append(self.check_single_period())
        results.append(self.check_rtt_part_types())
        results.append(self.check_icb_count())
        results.append(self.check_core_specialties_present())
        results.append(self.check_no_negative_patient_counts())
        results.append(self.check_total_all_never_null())
        results.append(self.check_week_band_sum_consistency())
        results.append(self.check_provider_code_format())

        return results

    def check_row_count(self) -> ValidationResult:
        """
        Monthly RTT files should contain 150k-200k rows.
        Alert on significant deviation — may indicate partial load.
        """
        row_count = len(self.df)
        passed = 50_000 <= row_count <= 300_000

        return ValidationResult(
            check_name="row_count_in_expected_range",
            passed=passed,
            severity="error" if not passed else "info",
            message=(
                f"Row count: {row_count:,}. "
                f"{'Within' if passed else 'OUTSIDE'} expected range "
                f"(50,000 - 300,000)."
            ),
            details={"row_count": row_count},
        )

    def check_column_count(self) -> ValidationResult:
        """121 columns expected. Any change breaks downstream models."""
        col_count = len(self.df.columns)
        passed = col_count == 121

        return ValidationResult(
            check_name="column_count_equals_121",
            passed=passed,
            severity="error" if not passed else "info",
            message=(
                f"Column count: {col_count}. "
                f"{'Correct' if passed else 'UNEXPECTED — review dbt staging model'}."
            ),
            details={"column_count": col_count},
        )

    def check_single_period(self) -> ValidationResult:
        """
        Each file should contain exactly one reporting period.
        Multiple periods would indicate files have been concatenated.
        """
        periods = self.df["Period"].unique().tolist()
        passed = len(periods) == 1

        return ValidationResult(
            check_name="single_period_per_file",
            passed=passed,
            severity="error" if not passed else "info",
            message=(
                f"Periods found: {periods}. "
                f"{'Single period confirmed' if passed else 'MULTIPLE PERIODS'}."
            ),
            details={"periods": periods},
        )

    def check_rtt_part_types(self) -> ValidationResult:
        """
        RTT Part Types must be one of the five known values.
        New values indicate NHS England has changed the format.
        """
        expected = {"Part_1A", "Part_1B", "Part_2", "Part_2A", "Part_3"}
        actual = set(self.df["RTT Part Type"].unique())
        unexpected = actual - expected
        passed = len(unexpected) == 0

        return ValidationResult(
            check_name="rtt_part_types_are_known_values",
            passed=passed,
            severity="error" if not passed else "info",
            message=(
                f"Part types found: {actual}. "
                f"{'All known values' if passed else f'UNEXPECTED VALUES: {unexpected}'}."
            ),
            details={
                "expected": list(expected),
                "actual": list(actual),
                "unexpected": list(unexpected),
            },
        )

    def check_icb_count(self) -> ValidationResult:
        """
        England has 42 Integrated Care Boards since July 2022.
        A significant change would indicate an NHS reorganisation.
        """
        icb_count = self.df["Provider Parent Org Code"].nunique()
        passed = icb_count == self.EXPECTED_ICB_COUNT

        return ValidationResult(
            check_name="icb_count_equals_42",
            passed=passed,
            severity="warning" if not passed else "info",
            message=(
                f"ICB count: {icb_count}. "
                f"{'42 confirmed' if passed else 'UNEXPECTED COUNT — check NHS reorganisation'}."
            ),
            details={"icb_count": icb_count},
        )

    def check_core_specialties_present(self) -> ValidationResult:
        """
        Core specialties should always appear in RTT data.
        Missing specialties may indicate a partial file.
        """
        actual_codes = set(self.df["Treatment Function Code"].unique())
        missing = self.CORE_SPECIALTIES - actual_codes
        passed = len(missing) == 0

        return ValidationResult(
            check_name="core_specialties_present",
            passed=passed,
            severity="warning" if not passed else "info",
            message=(
                f"Core specialties check. " f"{'All present' if passed else f'MISSING: {missing}'}."
            ),
            details={"missing_specialties": list(missing)},
        )

    def check_no_negative_patient_counts(self) -> ValidationResult:
        """
        Patient counts must never be negative.
        Negative values indicate data corruption or processing errors.
        """
        numeric_cols = self.df.select_dtypes(include=["number"]).columns
        negative_mask = (self.df[numeric_cols] < 0).any(axis=1)
        negative_count = negative_mask.sum()
        passed = negative_count == 0

        return ValidationResult(
            check_name="no_negative_patient_counts",
            passed=passed,
            severity="error" if not passed else "info",
            message=(
                f"Negative value check. "
                f"{'No negatives found' if passed else 'NEGATIVE VALUES found'}."
            ),
            affected_rows=int(negative_count),
        )

    def check_total_all_never_null(self) -> ValidationResult:
        """
        Total All is our primary patient count metric and must
        never be null. Null here indicates structural data loss.
        """
        null_count = self.df["Total All"].isna().sum()
        passed = null_count == 0

        return ValidationResult(
            check_name="total_all_never_null",
            passed=passed,
            severity="error" if not passed else "info",
            message=(
                f"Total All null check. "
                f"{'No nulls' if passed else f'NULL VALUES: {null_count:,} rows affected'}."
            ),
            affected_rows=int(null_count),
        )

    def check_week_band_sum_consistency(self) -> ValidationResult:
        """
        For Incomplete Pathways (Part_2), the sum of all week bands
        should approximately equal Total All.

        We allow a 5% tolerance because:
        - Suppressed values (*) may have been excluded
        - Rounding in the source data

        This is a powerful check — it catches cases where NHS England
        has shifted data between columns without changing the total.
        """
        part2 = self.df[self.df["RTT Part Type"] == "Part_2"].copy()

        week_cols = [col for col in self.df.columns if "Weeks SUM" in col]

        if not week_cols:
            return ValidationResult(
                check_name="week_band_sum_consistency",
                passed=False,
                severity="error",
                message="No week band columns found. Column format may have changed.",
            )

        part2["week_band_sum"] = part2[week_cols].fillna(0).sum(axis=1)
        part2["total_all_numeric"] = pd.to_numeric(part2["Total All"], errors="coerce").fillna(0)

        # Check where sum differs from Total All by more than 5%
        part2["pct_diff"] = (
            abs(part2["week_band_sum"] - part2["total_all_numeric"])
            / part2["total_all_numeric"].replace(0, 1)
            * 100
        )

        inconsistent = (part2["pct_diff"] > 5).sum()
        total_part2 = len(part2)
        passed = inconsistent == 0

        return ValidationResult(
            check_name="week_band_sum_consistency",
            passed=passed,
            severity="warning" if not passed else "info",
            message=(
                f"Week band sum vs Total All check (Part_2 rows). "
                f"{'Consistent' if passed else 'Discrepancies found — review week band totals'}."
            ),
            affected_rows=int(inconsistent),
            details={
                "total_part2_rows": total_part2,
                "inconsistent_rows": int(inconsistent),
            },
        )

    def check_provider_code_format(self) -> ValidationResult:
        """
        NHS ODS provider codes follow a specific format.
        Valid formats: 3 uppercase letters, or letter+2digits,
        or letter+digit+alphanumeric combinations.
        Invalid codes indicate data entry errors in source systems.
        """
        codes = self.df["Provider Org Code"].dropna()
        # Basic check: codes should be 3-6 characters, alphanumeric
        invalid_mask = ~codes.str.match(r"^[A-Z0-9]{3,6}$")
        invalid_count = invalid_mask.sum()
        passed = invalid_count == 0

        return ValidationResult(
            check_name="provider_code_format_valid",
            passed=passed,
            severity="warning" if not passed else "info",
            message=(
                f"Provider ODS code format check. "
                f"{'All codes valid' if passed else 'Invalid ODS code format detected'}."
            ),
            affected_rows=int(invalid_count),
        )


def run_validations(zip_path: Path) -> bool:
    """
    Run all NHS RTT validations and log results.
    Returns True if all error-severity checks pass.
    Returns False if any error-severity check fails.
    Warning-severity failures are logged but do not block the pipeline.
    """
    logger.info(f"Running NHS RTT validations on: {zip_path}")

    validator = NHSRTTValidator(zip_path)
    results = validator.run_all_checks()

    errors = []
    warnings = []

    logger.info("=" * 60)
    logger.info("NHS RTT VALIDATION RESULTS")
    logger.info("=" * 60)

    for result in results:
        status = "PASS" if result.passed else "FAIL"
        icon = "✓" if result.passed else "✗"

        log_fn = (
            logger.info
            if result.passed
            else (logger.error if result.severity == "error" else logger.warning)
        )

        log_fn(f"[{status}] {icon} {result.check_name}: {result.message}")

        if not result.passed:
            if result.severity == "error":
                errors.append(result)
            else:
                warnings.append(result)

    logger.info("=" * 60)
    logger.info(
        f"Summary: {len(results)} checks — "
        f"{len(results) - len(errors) - len(warnings)} passed, "
        f"{len(warnings)} warnings, "
        f"{len(errors)} errors"
    )

    if errors:
        logger.error(
            f"VALIDATION FAILED: {len(errors)} error(s). " f"Pipeline halted before BigQuery load."
        )
        return False

    if warnings:
        logger.warning(
            f"VALIDATION PASSED WITH WARNINGS: {len(warnings)} warning(s). "
            f"Review before confirming data quality."
        )

    logger.info("All error-severity validations passed.")
    return True
