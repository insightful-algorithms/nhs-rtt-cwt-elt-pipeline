"""
NHS RTT Extractor
=================
Downloads NHS Referral To Treatment (RTT) waiting time data
from NHS England's statistical release pages.

Key design decisions:
- Downloads the Full CSV ZIP only (contains all pathway types)
- Detects 'revised' files and flags them for reprocessing
- Saves raw files unmodified — transformation happens in dbt
- Implements retry logic and polite request delays
- Produces structured metadata for every download
"""

import hashlib
import logging
import os
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from extract.config import (
    NHS_FINANCIAL_YEARS,
    PipelineConfig,
    get_financial_year_for_month,
    get_gcs_raw_path,
    get_nhs_month_code,
)

# ─── Logging ──────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class DownloadResult:
    """
    Structured result for every file download attempt.
    This becomes the audit record — every download is logged.
    """
    month_code: str
    financial_year: str
    source_url: str
    filename: str
    local_path: str
    file_size_bytes: int
    md5_checksum: str
    is_revised: bool
    download_timestamp: str
    success: bool
    error_message: Optional[str] = None
    gcs_path: Optional[str] = None
    csv_files_inside_zip: list = field(default_factory=list)


# ─── Extractor Class ──────────────────────────────────────────────────────────

class NHSRTTExtractor:
    """
    Downloads NHS RTT monthly data from NHS England statistical releases.

    Usage:
        config = PipelineConfig()
        extractor = NHSRTTExtractor(config)
        result = extractor.extract_month(year=2026, month=1)
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        """
        Build a requests Session with retry logic and polite headers.

        Why a Session?
        A requests.Session reuses the underlying TCP connection across
        multiple requests to the same host. This is faster and more
        respectful to NHS England's servers than opening a new connection
        for every request.

        Why these headers?
        NHS England's web server may block requests that look like bots.
        A realistic User-Agent and Accept header makes our requests
        indistinguishable from a normal browser visit.
        """
        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (compatible; NHS-RTT-Pipeline/1.0; "
                "Data Engineering Portfolio Project; "
                "Contact: github.com/insightful-algorithms)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.5",
        })
        return session

    def extract_month(self, year: int, month: int) -> DownloadResult:
        """
        Main entry point — extract RTT data for a specific month.

        Args:
            year:  Calendar year (e.g. 2026)
            month: Calendar month number (e.g. 1 for January)

        Returns:
            DownloadResult with full audit metadata

        Example:
            result = extractor.extract_month(2026, 1)
            # Downloads Full CSV ZIP for January 2026
        """
        month_code = get_nhs_month_code(year, month)
        financial_year = get_financial_year_for_month(year, month)

        logger.info(f"Starting extraction for {month_code} ({financial_year})")

        # Step 1: Get the NHS England release page for this financial year
        page_url = NHS_FINANCIAL_YEARS[financial_year]["url"]
        logger.info(f"Fetching release page: {page_url}")

        page_html = self._fetch_page(page_url)
        if not page_html:
            return self._failed_result(
                month_code, financial_year, page_url,
                "Failed to fetch NHS England release page"
            )

        # Step 2: Find the download link for this month's Full CSV ZIP
        download_info = self._find_download_link(page_html, month_code, page_url)
        if not download_info:
            return self._failed_result(
                month_code, financial_year, page_url,
                f"Could not find Full CSV ZIP link for {month_code} on {page_url}"
            )

        download_url, filename, is_revised = download_info
        logger.info(f"Found download link: {download_url} (revised={is_revised})")

        # Step 3: Download the file
        local_path = self._get_local_path(financial_year, month_code, filename)
        downloaded = self._download_file(download_url, local_path)
        if not downloaded:
            return self._failed_result(
                month_code, financial_year, download_url,
                f"Failed to download file from {download_url}"
            )

        # Step 4: Validate the downloaded file
        validation_error = self._validate_file(local_path)
        if validation_error:
            return self._failed_result(
                month_code, financial_year, download_url, validation_error
            )

        # Step 5: Inspect ZIP contents
        csv_files = self._list_zip_contents(local_path)
        logger.info(f"ZIP contains {len(csv_files)} CSV files: {csv_files}")

        # Step 6: Calculate checksum for audit trail
        md5 = self._calculate_md5(local_path)
        file_size = os.path.getsize(local_path)

        # Step 7: Build GCS path for upload (upload handled by load/ module)
        gcs_path = get_gcs_raw_path(financial_year, month_code, filename)

        result = DownloadResult(
            month_code=month_code,
            financial_year=financial_year,
            source_url=download_url,
            filename=filename,
            local_path=str(local_path),
            file_size_bytes=file_size,
            md5_checksum=md5,
            is_revised=is_revised,
            download_timestamp=datetime.utcnow().isoformat(),
            success=True,
            gcs_path=gcs_path,
            csv_files_inside_zip=csv_files,
        )

        logger.info(
            f"Successfully extracted {month_code}: "
            f"{file_size:,} bytes, {len(csv_files)} CSVs inside ZIP"
        )
        return result

    def _fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch an NHS England page with retry logic.
        Returns the HTML content or None on failure.
        """
        for attempt in range(1, self.config.max_retries + 1):
            try:
                logger.debug(f"Fetching page (attempt {attempt}): {url}")
                response = self.session.get(
                    url,
                    timeout=self.config.request_timeout_seconds
                )
                response.raise_for_status()
                return response.text

            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt} failed for {url}: {e}")
                if attempt < self.config.max_retries:
                    wait = self.config.request_delay_seconds * attempt
                    logger.info(f"Waiting {wait}s before retry...")
                    time.sleep(wait)

        logger.error(f"All {self.config.max_retries} attempts failed for {url}")
        return None

    def _find_download_link(
        self,
        html: str,
        month_code: str,
        page_url: str
    ) -> Optional[tuple]:
        """
        Parse the NHS England release page to find the Full CSV ZIP
        download link for a specific month.

        Returns:
            Tuple of (download_url, filename, is_revised) or None

        Why BeautifulSoup?
        NHS England's pages are standard HTML. BeautifulSoup parses
        HTML reliably and lets us search by link text — much more
        robust than regex on raw HTML strings.
        """
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            logger.error(f"Failed to parse HTML: {e}")
            return None

        # Search for links containing "Full CSV data file" and our month code
        # NHS England link text examples:
        #   "Full CSV data file Jan26 (ZIP, 4M)"
        #   "Full CSV data file Mar25 (ZIP, 4M) revised"

        search_text = f"Full CSV data file {month_code}"

        for link in soup.find_all("a", href=True):
            link_text = link.get_text(strip=True)

            if search_text.lower() in link_text.lower():
                href = link["href"]

                # Resolve relative URLs to absolute
                if href.startswith("http"):
                    download_url = href
                else:
                    download_url = urljoin(page_url, href)

                # Extract filename from URL
                filename = download_url.split("/")[-1]
                if not filename.endswith(".zip"):
                    filename = f"RTT_Full_CSV_{month_code}.zip"

                # Detect if NHS England has marked this as revised
                is_revised = "revised" in link_text.lower()

                return download_url, filename, is_revised

        logger.warning(
            f"No Full CSV ZIP link found for {month_code}. "
            f"Searched for: '{search_text}'"
        )
        return None

    def _download_file(self, url: str, local_path: Path) -> bool:
        """
        Download a file from URL to local path with retry logic.
        Uses streaming download to handle large files efficiently.

        Why streaming?
        The Full CSV ZIPs are up to 4MB. Loading the entire response
        into memory before writing to disk would use 4MB of RAM per
        download. Streaming writes in 8KB chunks — memory usage stays
        constant regardless of file size.
        """
        local_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, self.config.max_retries + 1):
            try:
                logger.info(f"Downloading (attempt {attempt}): {url}")

                # Polite delay between requests
                if attempt > 1:
                    time.sleep(self.config.request_delay_seconds * attempt)

                response = self.session.get(
                    url,
                    stream=True,
                    timeout=self.config.request_timeout_seconds
                )
                response.raise_for_status()

                # Stream write in 8KB chunks
                bytes_written = 0
                with open(local_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            bytes_written += len(chunk)

                logger.info(f"Downloaded {bytes_written:,} bytes to {local_path}")
                return True

            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt} failed: {e}")
                # Remove partial file if download failed mid-way
                if local_path.exists():
                    local_path.unlink()

        logger.error(f"All download attempts failed for {url}")
        return False

    def _validate_file(self, local_path: Path) -> Optional[str]:
        """
        Validate a downloaded file meets minimum quality standards.
        Returns an error message string if validation fails, None if valid.
        """
        if not local_path.exists():
            return f"File does not exist: {local_path}"

        file_size = os.path.getsize(local_path)

        if file_size < self.config.min_file_size_bytes:
            return (
                f"File too small ({file_size} bytes). "
                f"Minimum expected: {self.config.min_file_size_bytes} bytes. "
                f"This may indicate a failed or empty download."
            )

        if file_size > self.config.max_file_size_bytes:
            return (
                f"File unexpectedly large ({file_size:,} bytes). "
                f"Maximum expected: {self.config.max_file_size_bytes:,} bytes."
            )

        # Validate it is actually a ZIP file
        if not zipfile.is_zipfile(local_path):
            return f"Downloaded file is not a valid ZIP archive: {local_path}"

        return None  # All checks passed

    def _list_zip_contents(self, zip_path: Path) -> list:
        """
        List all CSV files inside the ZIP archive.
        This tells us what data is available without extracting everything.
        """
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                return [
                    name for name in zf.namelist()
                    if name.lower().endswith(".csv")
                ]
        except zipfile.BadZipFile as e:
            logger.error(f"Cannot read ZIP contents: {e}")
            return []

    def _calculate_md5(self, file_path: Path) -> str:
        """
        Calculate MD5 checksum of a file for audit trail.

        Why MD5?
        MD5 is not cryptographically secure, but it is perfectly adequate
        for detecting whether a file has changed between downloads.
        If NHS England revises a month's data, the MD5 will change —
        our pipeline can detect this and trigger a reprocess.
        """
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def _get_local_path(
        self, financial_year: str, month_code: str, filename: str
    ) -> Path:
        """
        Build the local file path for a downloaded file.
        Structure: data/raw/rtt/{financial_year}/{month_code}/{filename}
        """
        return Path(self.config.raw_data_dir) / "rtt" / financial_year / month_code / filename

    def _failed_result(
        self,
        month_code: str,
        financial_year: str,
        url: str,
        error: str
    ) -> DownloadResult:
        """Return a structured failure result for logging and alerting."""
        logger.error(f"Extraction failed for {month_code}: {error}")
        return DownloadResult(
            month_code=month_code,
            financial_year=financial_year,
            source_url=url,
            filename="",
            local_path="",
            file_size_bytes=0,
            md5_checksum="",
            is_revised=False,
            download_timestamp=datetime.utcnow().isoformat(),
            success=False,
            error_message=error,
        )