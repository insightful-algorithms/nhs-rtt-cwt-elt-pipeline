"""
NHS RTT Historical Load Runner
================================
Finds all downloaded ZIP files and loads them into BigQuery.
Run this after the backfill script has downloaded the ZIP files.

Usage:
    python -m load.load_to_bigquery
    python -m load.load_to_bigquery --month Jan26
    python -m load.load_to_bigquery --force
"""

import argparse
import logging
import sys
from pathlib import Path

from load.bigquery_loader import BigQueryLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("load_runner")


def find_zip_files(data_dir: str = "./data/raw") -> list:
    """
    Find all NHS RTT ZIP files in the data directory.
    Returns list of dicts with path, month_code, financial_year.
    """
    raw_path = Path(data_dir) / "rtt"
    zip_files = []

    if not raw_path.exists():
        logger.error(f"Data directory not found: {raw_path}")
        return []

    for financial_year_dir in sorted(raw_path.iterdir()):
        if not financial_year_dir.is_dir():
            continue
        financial_year = financial_year_dir.name

        for month_dir in sorted(financial_year_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            month_code = month_dir.name

            zips = list(month_dir.glob("*.zip"))
            if not zips:
                continue

            zip_path = zips[0]
            is_revised = "revised" in zip_path.name.lower()

            zip_files.append({
                "zip_path": zip_path,
                "month_code": month_code,
                "financial_year": financial_year,
                "is_revised": is_revised,
            })

    return zip_files


def run_load(data_dir: str = "./data/raw", month_filter: str = None):
    """Load all available ZIP files into BigQuery."""

    zip_files = find_zip_files(data_dir)

    if not zip_files:
        logger.error("No ZIP files found to load.")
        sys.exit(1)

    # Apply month filter if specified
    if month_filter:
        zip_files = [
            z for z in zip_files
            if z["month_code"].lower() == month_filter.lower()
        ]
        if not zip_files:
            logger.error(f"No ZIP file found for month: {month_filter}")
            sys.exit(1)

    logger.info("=" * 60)
    logger.info("NHS RTT BigQuery Load Runner")
    logger.info(f"Files to load: {len(zip_files)}")
    logger.info("=" * 60)

    for z in zip_files:
        logger.info(
            f"  {z['month_code']} ({z['financial_year']}) "
            f"{'[REVISED]' if z['is_revised'] else ''} "
            f"— {z['zip_path'].name}"
        )

    logger.info("=" * 60)

    loader = BigQueryLoader()
    results = []

    for i, z in enumerate(zip_files, 1):
        logger.info(
            f"\n[{i}/{len(zip_files)}] Loading {z['month_code']}..."
        )

        result = loader.load_zip_file(
            zip_path=z["zip_path"],
            month_code=z["month_code"],
            financial_year=z["financial_year"],
            is_revised=z["is_revised"],
        )
        results.append(result)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("LOAD SUMMARY")
    logger.info("=" * 60)

    success = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    total_rows = sum(r.rows_loaded for r in success)

    logger.info(f"Total files:    {len(results)}")
    logger.info(f"Successful:     {len(success)}")
    logger.info(f"Failed:         {len(failed)}")
    logger.info(f"Total rows:     {total_rows:,}")

    for r in success:
        revised_flag = " [REVISED]" if r.is_revised else ""
        logger.info(
            f"  {r.month_code}{revised_flag}: "
            f"{r.rows_loaded:,} rows loaded"
        )

    if failed:
        logger.error("FAILED LOADS:")
        for r in failed:
            logger.error(f"  {r.month_code}: {r.error_message}")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("ALL LOADS COMPLETE")
    logger.info(f"Table: {results[0].bq_table if results else 'N/A'}")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Load NHS RTT ZIP files into BigQuery"
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Load specific month only (e.g. Jan26)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data/raw",
        help="Directory containing downloaded ZIP files",
    )
    args = parser.parse_args()

    run_load(data_dir=args.data_dir, month_filter=args.month)


if __name__ == "__main__":
    main()