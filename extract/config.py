"""
NHS RTT & CWT Pipeline — Data Source Configuration
===================================================
Central configuration for all NHS England data source URLs,
financial year structures, and file naming conventions.

All NHS data is published under the Open Government Licence v3.0:
https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
"""

from dataclasses import dataclass

# ─── NHS Financial Year Configuration ─────────────────────────────────────────

# The NHS financial year runs April to March.
# Example: "2025-26" covers April 2025 to March 2026.
# We store years as strings matching NHS England's URL pattern exactly.

NHS_FINANCIAL_YEARS = {
    "2024-25": {
        "url": "https://www.england.nhs.uk/statistics/statistical-work-areas/"
        "rtt-waiting-times/rtt-data-2024-25/",
        "start_month": "Apr24",
        "end_month": "Mar25",
    },
    "2025-26": {
        "url": "https://www.england.nhs.uk/statistics/statistical-work-areas/"
        "rtt-waiting-times/rtt-data-2025-26/",
        "start_month": "Apr25",
        "end_month": "Mar26",
    },
}

# The current financial year — update each April
CURRENT_FINANCIAL_YEAR = "2025-26"


# ─── Month Code Mapping ────────────────────────────────────────────────────────

# NHS England uses short month codes in filenames: Jan26, Feb26, Mar26 etc.
# We map Python month numbers to these codes for a given financial year.

MONTH_NUMBER_TO_NHS_CODE = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def get_nhs_month_code(year: int, month: int) -> str:
    """
    Convert a calendar year and month number to NHS month code.

    Examples:
        get_nhs_month_code(2026, 1)  → 'Jan26'
        get_nhs_month_code(2025, 11) → 'Nov25'
        get_nhs_month_code(2025, 4)  → 'Apr25'
    """
    month_str = MONTH_NUMBER_TO_NHS_CODE[month]
    year_str = str(year)[-2:]  # Take last 2 digits: 2026 → '26'
    return f"{month_str}{year_str}"


def get_financial_year_for_month(year: int, month: int) -> str:
    """
    Return the NHS financial year string for a given calendar month.

    The NHS year runs April(4) to March(3).
    April 2025 to March 2026 = financial year "2025-26"

    Examples:
        get_financial_year_for_month(2026, 1)  → '2025-26'
        get_financial_year_for_month(2025, 4)  → '2025-26'
        get_financial_year_for_month(2025, 3)  → '2024-25'
    """
    if month >= 4:
        # April onwards: financial year starts this calendar year
        fy_start = year
    else:
        # January to March: financial year started last calendar year
        fy_start = year - 1

    fy_end = fy_start + 1
    return f"{fy_start}-{str(fy_end)[-2:]}"


# ─── RTT File Types ────────────────────────────────────────────────────────────

# Each monthly release contains these file types.
# We primarily target the Full CSV ZIP — it contains everything.

RTT_FILE_TYPES = {
    "full_csv_zip": {
        "pattern": "Full CSV data file {month_code}",
        "extension": ".zip",
        "priority": 1,  # This is our primary target
        "description": "Complete monthly RTT dataset — all pathways combined",
    },
    "incomplete_provider": {
        "pattern": "Incomplete Provider {month_code}",
        "extension": ".xlsx",
        "priority": 2,
        "description": "Patients still waiting, by provider trust",
    },
    "incomplete_commissioner": {
        "pattern": "Incomplete Commissioner {month_code}",
        "extension": ".xlsx",
        "priority": 3,
        "description": "Patients still waiting, by commissioning organisation",
    },
}

# For this pipeline we download the full CSV ZIP only.
# Individual XLSX files are available as fallback if the ZIP is unavailable.
PRIMARY_FILE_TYPE = "full_csv_zip"


# ─── Pipeline Configuration ────────────────────────────────────────────────────


@dataclass
class PipelineConfig:
    """
    Runtime configuration for the NHS RTT extraction pipeline.
    Values are loaded from environment variables in production
    but have sensible defaults for local development.
    """

    # GCP settings
    gcp_project_id: str = "your-gcp-project-id"
    gcp_raw_dataset: str = "nhs_rtt_raw"
    gcp_bucket_name: str = "your-gcp-bucket-name"
    gcp_region: str = "europe-west2"  # London — UK data residency

    # Local working directory for downloaded files
    raw_data_dir: str = "./data/raw"

    # How many months to load on the first (historical) run
    historical_load_months: int = 24

    # Whether to re-download files marked as "revised" by NHS England
    reload_revised: bool = True

    # Request settings — be respectful to NHS England's servers
    request_timeout_seconds: int = 120
    request_delay_seconds: float = 2.0  # Wait between requests
    max_retries: int = 3

    # File validation
    min_file_size_bytes: int = 10_000  # Reject suspiciously small files
    max_file_size_bytes: int = 50_000_000  # 50MB upper limit


# ─── GCS Path Structure ────────────────────────────────────────────────────────


def get_gcs_raw_path(financial_year: str, month_code: str, filename: str) -> str:
    """
    Return the GCS object path for a raw NHS RTT file.

    Structure: raw/rtt/{financial_year}/{month_code}/{filename}

    Example:
        get_gcs_raw_path("2025-26", "Jan26", "Full_CSV_Jan26.zip")
        → "raw/rtt/2025-26/Jan26/Full_CSV_Jan26.zip"

    Why this structure?
    - Partitioned by financial year and month for efficient querying
    - Mirrors NHS England's own organisation of releases
    - Makes it easy to identify and reprocess any specific month
    """
    return f"raw/rtt/{financial_year}/{month_code}/{filename}"
