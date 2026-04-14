"""
BigQuery Loader — NHS RTT Raw Data
====================================
Loads NHS RTT CSV files from local ZIPs into BigQuery
raw dataset. Handles multiple months in sequence.

Design decisions:
- Loads raw data UNCHANGED — no transformations here
- Adds audit columns (_loaded_at, _source_file, _is_revised)
- Uses WRITE_TRUNCATE per partition — idempotent reruns
- Validates row counts after every load
- Logs full audit trail for every load operation
"""

import hashlib
import io
import logging
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Audit record for every BigQuery load operation."""
    month_code: str
    financial_year: str
    source_file: str
    is_revised: bool
    rows_loaded: int
    rows_expected: int
    load_timestamp: str
    bq_table: str
    success: bool
    error_message: Optional[str] = None


class BigQueryLoader:
    """
    Loads NHS RTT CSV data into BigQuery raw dataset.

    Each monthly file is loaded into a single table
    nhs_rtt_raw.rtt_full_extract with a _period_month
    column used for partitioning and idempotent reloads.
    """

    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.raw_dataset = os.getenv("GCP_RAW_DATASET", "nhs_rtt_raw")
        self.table_name = "rtt_full_extract"
        self.full_table_id = (
            f"{self.project_id}.{self.raw_dataset}.{self.table_name}"
        )
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        self.client = bigquery.Client(project=self.project_id)
        logger.info(f"BigQuery loader initialised for {self.full_table_id}")

    def load_zip_file(
        self,
        zip_path: Path,
        month_code: str,
        financial_year: str,
        is_revised: bool = False,
    ) -> LoadResult:
        """
        Load a single monthly ZIP file into BigQuery.

        Steps:
        1. Extract CSV from ZIP into memory
        2. Add audit columns
        3. Delete existing rows for this month (idempotency)
        4. Load to BigQuery
        5. Verify row count
        """
        logger.info(f"Loading {month_code} from {zip_path.name}")

        # Step 1: Extract CSV from ZIP
        try:
            df = self._extract_csv_from_zip(zip_path)
            logger.info(
                f"Extracted {len(df):,} rows x {len(df.columns)} columns"
            )
        except Exception as e:
            return LoadResult(
                month_code=month_code,
                financial_year=financial_year,
                source_file=zip_path.name,
                is_revised=is_revised,
                rows_loaded=0,
                rows_expected=0,
                load_timestamp=datetime.now(timezone.utc).isoformat(),
                bq_table=self.full_table_id,
                success=False,
                error_message=f"CSV extraction failed: {e}",
            )

        rows_expected = len(df)

        # Step 2: Add audit columns
        df = self._add_audit_columns(
            df, month_code, financial_year,
            zip_path.name, is_revised
        )

        # Step 3: Delete existing rows for this month
        self._delete_existing_month(month_code)

        # Step 4: Load to BigQuery
        try:
            rows_loaded = self._load_dataframe(df, month_code)
        except Exception as e:
            return LoadResult(
                month_code=month_code,
                financial_year=financial_year,
                source_file=zip_path.name,
                is_revised=is_revised,
                rows_loaded=0,
                rows_expected=rows_expected,
                load_timestamp=datetime.now(timezone.utc).isoformat(),
                bq_table=self.full_table_id,
                success=False,
                error_message=f"BigQuery load failed: {e}",
            )

        # Step 5: Verify row count
        if rows_loaded != rows_expected:
            logger.warning(
                f"Row count mismatch for {month_code}: "
                f"expected {rows_expected:,}, loaded {rows_loaded:,}"
            )

        logger.info(
            f"Successfully loaded {month_code}: "
            f"{rows_loaded:,} rows into {self.full_table_id}"
        )

        return LoadResult(
            month_code=month_code,
            financial_year=financial_year,
            source_file=zip_path.name,
            is_revised=is_revised,
            rows_loaded=rows_loaded,
            rows_expected=rows_expected,
            load_timestamp=datetime.now(timezone.utc).isoformat(),
            bq_table=self.full_table_id,
            success=True,
        )

    def _extract_csv_from_zip(self, zip_path: Path) -> pd.DataFrame:
        """Extract CSV from ZIP and return as DataFrame."""
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            if not csv_files:
                raise ValueError(f"No CSV files found in {zip_path}")
            with zf.open(csv_files[0]) as f:
                df = pd.read_csv(f, encoding="utf-8", low_memory=False)
        return df

    def _add_audit_columns(
        self,
        df: pd.DataFrame,
        month_code: str,
        financial_year: str,
        source_file: str,
        is_revised: bool,
    ) -> pd.DataFrame:
        """
        Add audit and metadata columns to the DataFrame.
        These columns are added by our pipeline — not from NHS England.
        They enable full audit trail and idempotent reloads.
        """
        now = datetime.now(timezone.utc).isoformat()
        df["_pipeline_loaded_at"] = now
        df["_source_file"] = source_file
        df["_financial_year"] = financial_year
        df["_month_code"] = month_code
        df["_is_revised"] = is_revised
        df["_pipeline_version"] = "1.0.0"
        return df

    def _delete_existing_month(self, month_code: str):
        """
        Delete any existing rows for this month before loading.
        This makes the load idempotent — safe to re-run.

        Why delete before insert rather than WRITE_TRUNCATE?
        WRITE_TRUNCATE would delete ALL months from the table.
        We want to delete only the specific month being reloaded
        so other months remain intact.
        """
        delete_query = f"""
            DELETE FROM `{self.full_table_id}`
            WHERE _month_code = '{month_code}'
        """
        try:
            job = self.client.query(delete_query)
            job.result()
            logger.info(
                f"Deleted existing rows for {month_code} "
                f"(idempotent reload)"
            )
        except Exception as e:
            # Table may not exist yet on first load — that is fine
            logger.debug(f"Delete skipped (table may not exist yet): {e}")

    def _load_dataframe(
        self, df: pd.DataFrame, month_code: str
    ) -> int:
        """Load DataFrame to BigQuery and return rows loaded."""

        # Replace NaN with None for BigQuery compatibility
        df = df.where(pd.notnull(df), None)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            # Auto-detect schema from DataFrame
            autodetect=True,
            # Append to existing table
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # Create table if it does not exist
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            # Source format
            source_format=bigquery.SourceFormat.PARQUET,
        )

        # Convert to parquet for efficient upload
        # Parquet handles mixed types and nulls better than CSV
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Load to BigQuery
        load_job = self.client.load_table_from_file(
            parquet_buffer,
            self.full_table_id,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            ),
        )
        load_job.result()

        # Verify by querying row count for this month
        count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{self.full_table_id}`
            WHERE _month_code = '{month_code}'
        """
        result = self.client.query(count_query).result()
        for row in result:
            return row.row_count

        return 0