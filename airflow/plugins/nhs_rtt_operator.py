"""
NHS RTT Extraction Operator
============================
A custom Airflow operator that wraps our NHSRTTExtractor class
and integrates it cleanly into an Airflow DAG.

Why wrap the extractor in an operator?
The DAG file should be a readable description of the workflow —
not a place where business logic lives. By wrapping the extractor
in an operator, the DAG task becomes one clean line:

    extract = NHSRTTExtractOperator(task_id='extract_rtt_data', ...)

The operator handles all the Airflow integration (XCom pushing,
logging, failure handling) while the extractor handles the
NHS-specific logic. Separation of concerns again.

What is XCom?
XCom (Cross-Communication) is Airflow's mechanism for passing
data between tasks. When the extract task completes, it pushes
the DownloadResult metadata to XCom. The next task pulls it
from XCom to know where the file is and what its checksum was.
"""

import json
import logging
from dataclasses import asdict

from airflow.models import BaseOperator

from extract.config import PipelineConfig
from extract.nhs_rtt_extractor import NHSRTTExtractor

logger = logging.getLogger(__name__)


class NHSRTTExtractOperator(BaseOperator):
    """
    Extracts NHS RTT data for a specific month and pushes
    the download result metadata to Airflow XCom.

    Args:
        target_year:  Calendar year (e.g. 2026)
        target_month: Calendar month number (e.g. 1 for January)
        raw_data_dir: Local directory for downloaded files

    XCom output (key: 'download_result'):
        A JSON-serialised DownloadResult dataclass containing:
        - source_url, filename, local_path
        - file_size_bytes, md5_checksum
        - is_revised, download_timestamp
        - gcs_path, csv_files_inside_zip
    """

    template_fields = ["target_year", "target_month"]
    ui_color = "#00A9CE"  # NHS blue — makes this operator recognisable in UI

    def __init__(
        self,
        target_year: int,
        target_month: int,
        raw_data_dir: str = "/opt/airflow/data/raw",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_year = target_year
        self.target_month = target_month
        self.raw_data_dir = raw_data_dir

    def execute(self, context):
        """
        Called by Airflow when this task runs.
        Must return a value or push to XCom — we do both.
        """
        logger.info(
            f"Starting NHS RTT extraction for "
            f"{self.target_month}/{self.target_year}"
        )

        config = PipelineConfig(raw_data_dir=self.raw_data_dir)
        extractor = NHSRTTExtractor(config)

        result = extractor.extract_month(
            year=int(self.target_year),
            month=int(self.target_month)
        )

        if not result.success:
            raise Exception(
                f"NHS RTT extraction failed for "
                f"{self.target_month}/{self.target_year}: "
                f"{result.error_message}"
            )

        # Convert dataclass to dict for XCom serialisation
        result_dict = asdict(result)

        # Push to XCom so downstream tasks can access the result
        context["task_instance"].xcom_push(
            key="download_result",
            value=json.dumps(result_dict)
        )

        logger.info(
            f"Extraction complete. "
            f"File: {result.filename} "
            f"({result.file_size_bytes:,} bytes). "
            f"GCS path: {result.gcs_path}"
        )

        return result_dict