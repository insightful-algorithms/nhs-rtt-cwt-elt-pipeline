"""
NHS RTT Historical Backfill Script
====================================
Downloads multiple months of NHS RTT data in sequence.
Run this once to populate the data lake with historical data
before the monthly Airflow DAG takes over for ongoing loads.

This script is intentionally simple and sequential — it downloads
one month at a time with a polite delay between requests.
Do NOT parallelise this — NHS England's servers are public
infrastructure shared by the entire health system.

Usage:
    python extract/backfill.py --months 6
    python extract/backfill.py --start 2025-10 --end 2026-03
    python extract/backfill.py --months 12 --dry-run

The script skips months that have already been downloaded
unless --force is passed.
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from extract.config import (
    PipelineConfig,
    get_financial_year_for_month,
    get_nhs_month_code,
)
from extract.nhs_rtt_extractor import NHSRTTExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backfill")


def get_months_to_load(num_months: int) -> list:
    """
    Return the last N months as (year, month) tuples.
    Works backwards from the current month.

    Example: if today is April 2026 and num_months=6,
    returns [(2025,10),(2025,11),(2025,12),(2026,1),(2026,2),(2026,3)]
    Note: current month excluded as NHS England publishes
    with a ~5 week lag.
    """
    from dateutil.relativedelta import relativedelta

    today = datetime.today()
    # Start from last month (most recent published)
    current = today.replace(day=1) - relativedelta(months=1)

    months = []
    for _ in range(num_months):
        months.append((current.year, current.month))
        current -= relativedelta(months=1)

    # Return in chronological order (oldest first)
    return list(reversed(months))


def get_months_in_range(start_str: str, end_str: str) -> list:
    """
    Return all months between start and end inclusive.
    Format: YYYY-MM (e.g. 2025-10)
    """
    from dateutil.relativedelta import relativedelta

    start = datetime.strptime(start_str, "%Y-%m")
    end = datetime.strptime(end_str, "%Y-%m")

    months = []
    current = start
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months


def run_backfill(
    months: list,
    config: PipelineConfig,
    dry_run: bool = False,
    force: bool = False,
    delay_seconds: float = 3.0,
):
    """
    Download RTT data for each month in the list.

    Args:
        months: List of (year, month) tuples
        config: Pipeline configuration
        dry_run: If True, show what would be downloaded without downloading
        force: If True, re-download even if file already exists
        delay_seconds: Polite delay between requests
    """
    extractor = NHSRTTExtractor(config)
    results = []

    logger.info("=" * 60)
    logger.info("NHS RTT Historical Backfill")
    logger.info(f"Months to process: {len(months)}")
    logger.info(f"Dry run: {dry_run}")
    logger.info(f"Force re-download: {force}")
    logger.info(f"Delay between requests: {delay_seconds}s")
    logger.info("=" * 60)

    for i, (year, month) in enumerate(months, 1):
        month_code = get_nhs_month_code(year, month)
        financial_year = get_financial_year_for_month(year, month)

        logger.info(f"[{i}/{len(months)}] Processing {month_code} " f"({financial_year})")

        # Check if already downloaded

        local_dir = Path(config.raw_data_dir) / "rtt" / financial_year / month_code

        if local_dir.exists() and any(local_dir.glob("*.zip")) and not force:
            existing = list(local_dir.glob("*.zip"))[0]
            logger.info(
                f"  Already downloaded: {existing.name} — skipping. " f"Use --force to re-download."
            )
            results.append(
                {
                    "month_code": month_code,
                    "status": "skipped",
                    "reason": "already exists",
                }
            )
            continue

        if dry_run:
            logger.info(f"  DRY RUN — would download {month_code}")
            results.append(
                {
                    "month_code": month_code,
                    "status": "dry_run",
                }
            )
            continue

        # Download the month
        result = extractor.extract_month(year=year, month=month)

        if result.success:
            logger.info(
                f"  SUCCESS — {result.filename} "
                f"({result.file_size_bytes:,} bytes) "
                f"revised={result.is_revised}"
            )
            results.append(
                {
                    "month_code": month_code,
                    "status": "success",
                    "filename": result.filename,
                    "file_size_bytes": result.file_size_bytes,
                    "is_revised": result.is_revised,
                    "md5": result.md5_checksum,
                    "csv_files": result.csv_files_inside_zip,
                }
            )
        else:
            logger.error(f"  FAILED — {result.error_message}")
            results.append(
                {
                    "month_code": month_code,
                    "status": "failed",
                    "error": result.error_message,
                }
            )

        # Polite delay between requests
        if i < len(months):
            logger.info(f"  Waiting {delay_seconds}s before next request...")
            time.sleep(delay_seconds)

    # Summary
    logger.info("=" * 60)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 60)

    success = [r for r in results if r["status"] == "success"]
    skipped = [r for r in results if r["status"] == "skipped"]
    failed = [r for r in results if r["status"] == "failed"]
    dry = [r for r in results if r["status"] == "dry_run"]

    logger.info(f"Total months:    {len(results)}")
    logger.info(f"Downloaded:      {len(success)}")
    logger.info(f"Skipped:         {len(skipped)}")
    logger.info(f"Failed:          {len(failed)}")
    if dry:
        logger.info(f"Dry run:         {len(dry)}")

    if success:
        total_bytes = sum(r.get("file_size_bytes", 0) for r in success)
        logger.info(
            f"Total downloaded: {total_bytes:,} bytes " f"({total_bytes / 1_000_000:.1f} MB)"
        )
        revised = [r for r in success if r.get("is_revised")]
        if revised:
            logger.info(
                f"Revised files:   {len(revised)} " f"({[r['month_code'] for r in revised]})"
            )

    if failed:
        logger.error("FAILED MONTHS:")
        for r in failed:
            logger.error(f"  {r['month_code']}: {r.get('error')}")

    logger.info("=" * 60)

    return len(failed) == 0


def main():
    parser = argparse.ArgumentParser(description="NHS RTT historical data backfill")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--months", type=int, help="Number of recent months to download (e.g. --months 6)"
    )
    group.add_argument("--start", type=str, help="Start month in YYYY-MM format (use with --end)")

    parser.add_argument("--end", type=str, help="End month in YYYY-MM format (use with --start)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be downloaded without downloading"
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-download even if file already exists"
    )
    parser.add_argument(
        "--delay", type=float, default=3.0, help="Delay in seconds between requests (default: 3.0)"
    )
    parser.add_argument(
        "--data-dir", type=str, default="./data/raw", help="Local directory for downloaded files"
    )

    args = parser.parse_args()

    # Validate arguments
    if args.start and not args.end:
        parser.error("--start requires --end")
    if args.end and not args.start:
        parser.error("--end requires --start")

    # Build month list
    if args.months:
        months = get_months_to_load(args.months)
    else:
        months = get_months_in_range(args.start, args.end)

    if not months:
        logger.error("No months to process")
        sys.exit(1)

    logger.info(f"Months to process: {[f'{y}-{m:02d}' for y, m in months]}")

    # Run backfill
    config = PipelineConfig(
        raw_data_dir=args.data_dir,
        request_delay_seconds=args.delay,
        max_retries=3,
    )

    success = run_backfill(
        months=months,
        config=config,
        dry_run=args.dry_run,
        force=args.force,
        delay_seconds=args.delay,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
