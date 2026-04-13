"""
NHS RTT Data Availability Sensor
=================================
A custom Airflow sensor that polls the NHS England statistical
release page and waits until the target month's RTT data
has been published before allowing the pipeline to proceed.

Why do we need a sensor?
NHS England publishes RTT data on the first working day of each
month — but the exact time varies. Without a sensor, our DAG
would fail if it ran before the data appeared. The sensor keeps
checking every 10 minutes until the data is available, then
allows the downstream tasks to proceed automatically.

This is called an event-driven pipeline trigger — a core
production pattern in data engineering.
"""

import logging
from datetime import datetime

import requests
from airflow.sensors.base import BaseSensorOperator
from bs4 import BeautifulSoup

from extract.config import (
    NHS_FINANCIAL_YEARS,
    get_financial_year_for_month,
    get_nhs_month_code,
)

logger = logging.getLogger(__name__)


class NHSRTTDataSensor(BaseSensorOperator):
    """
    Waits until NHS England has published RTT data for the target month.

    Airflow will call the poke() method repeatedly on the defined
    poke_interval until it returns True, at which point the sensor
    succeeds and downstream tasks are triggered.

    Args:
        target_year:  The calendar year to check for (e.g. 2026)
        target_month: The calendar month to check for (e.g. 1 = January)

    Example DAG usage:
        wait_for_data = NHSRTTDataSensor(
            task_id='check_nhs_data_published',
            target_year={{ execution_date.year }},
            target_month={{ execution_date.month }},
            poke_interval=600,   # Check every 10 minutes
            timeout=86400,       # Give up after 24 hours
            mode='reschedule',   # Release worker slot between pokes
        )
    """

    # These tell Airflow what parameters this sensor accepts
    template_fields = ["target_year", "target_month"]

    def __init__(
        self,
        target_year: int,
        target_month: int,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.target_year = target_year
        self.target_month = target_month

    def poke(self, context) -> bool:
        """
        Check whether the target month's data is available.
        Returns True if found, False to keep waiting.

        Why mode='reschedule'?
        In 'reschedule' mode, Airflow releases the worker slot
        between pokes. In 'puck' mode it holds the slot the entire
        time. For a sensor that might wait hours, reschedule mode
        is far more efficient — other tasks can use the worker
        while we wait.
        """
        month_code = get_nhs_month_code(
            int(self.target_year),
            int(self.target_month)
        )
        financial_year = get_financial_year_for_month(
            int(self.target_year),
            int(self.target_month)
        )

        logger.info(
            f"Checking NHS England for {month_code} RTT data "
            f"(financial year: {financial_year})"
        )

        page_url = NHS_FINANCIAL_YEARS.get(financial_year, {}).get("url")
        if not page_url:
            logger.error(
                f"No URL configured for financial year {financial_year}. "
                f"Update extract/config.py to add this year."
            )
            return False

        try:
            response = requests.get(
                page_url,
                timeout=30,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; NHS-RTT-Pipeline/1.0)"
                    )
                }
            )
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Could not reach NHS England page: {e}. "
                f"Will retry on next poke interval."
            )
            return False

        # Parse the page and look for the target month's link
        soup = BeautifulSoup(response.text, "lxml")
        search_text = f"Full CSV data file {month_code}"

        for link in soup.find_all("a", href=True):
            link_text = link.get_text(strip=True)
            if search_text.lower() in link_text.lower():
                logger.info(
                    f"NHS England has published {month_code} data. "
                    f"Found link: '{link_text}'. Proceeding with pipeline."
                )
                return True

        logger.info(
            f"NHS England has not yet published {month_code} data. "
            f"Will check again in {self.poke_interval} seconds."
        )
        return False