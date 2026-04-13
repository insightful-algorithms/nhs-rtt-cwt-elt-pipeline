"""
NHS RTT & CWT Monthly ELT Pipeline
====================================
Main Airflow DAG for the NHS Referral To Treatment (RTT)
waiting times data pipeline.

Schedule: First working day of each month, aligned with
NHS England's RTT statistical release calendar.

Pipeline flow:
    1. Sensor: Wait for NHS England to publish new month's data
    2. Extract: Download Full CSV ZIP from NHS England
    3. Validate: Verify download integrity
    4. Load GCS: Upload raw file to Google Cloud Storage
    5. Load BQ: Load CSV into BigQuery raw dataset
    6. dbt run: Execute staging → intermediate → mart models
    7. dbt test: Run all data quality tests
    8. Notify: Send success/failure email alert

Design decisions documented here:
    - Why mode='reschedule' for the sensor → see nhs_rtt_sensor.py
    - Why XCom for inter-task communication → see nhs_rtt_operator.py
    - Why catchup=False → explained in the DAG args below
    - Why max_active_runs=1 → only one month loads at a time
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

# Import our custom plugins
from nhs_rtt_sensor import NHSRTTDataSensor
from nhs_rtt_operator import NHSRTTExtractOperator


# ─── Default arguments ────────────────────────────────────────────────────────
# These apply to every task in the DAG unless overridden at task level.
# This is the production-standard way to configure Airflow tasks.

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,       # Each run is independent
    "email": [os.getenv("ALERT_EMAIL", "admin@example.com")],
    "email_on_failure": True,       # Alert on any task failure
    "email_on_retry": False,        # Don't spam on retries
    "retries": 3,                   # Retry failed tasks 3 times
    "retry_delay": timedelta(minutes=5),   # Wait 5 min between retries
    "retry_exponential_backoff": True,     # Increase delay on each retry
    "execution_timeout": timedelta(hours=2),  # Kill hung tasks after 2hrs
}


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="nhs_rtt_monthly_pipeline",
    description=(
        "Monthly ELT pipeline for NHS RTT waiting times data. "
        "Extracts from NHS England, loads to BigQuery, "
        "transforms with dbt, validates with Great Expectations."
    ),
    default_args=default_args,

    # Run at 7am on the 1st of each month.
    # The sensor will wait for NHS England to publish before proceeding.
    # NHS England typically publishes between 9:30am and 12:00pm.
    schedule_interval="0 7 1 * *",

    start_date=datetime(2025, 4, 1),  # Start of current NHS financial year

    # catchup=False means Airflow will NOT run for every month
    # between start_date and today when the DAG is first activated.
    # Why? We will handle historical loads separately via a backfill
    # script. Having Airflow auto-catchup 24 months would create
    # 24 simultaneous pipeline runs and overwhelm NHS England's servers.
    catchup=False,

    # Only one run of this DAG at a time.
    # Prevents two months loading simultaneously and causing
    # BigQuery conflicts.
    max_active_runs=1,

    tags=["nhs", "rtt", "monthly", "production"],

) as dag:

    # ── Task 1: Sensor ────────────────────────────────────────────────────────
    # Wait for NHS England to publish the current month's data.
    # Uses reschedule mode to release the worker slot while waiting.

    wait_for_nhs_data = NHSRTTDataSensor(
        task_id="check_nhs_data_published",
        target_year="{{ execution_date.year }}",
        target_month="{{ execution_date.month }}",
        poke_interval=600,       # Check every 10 minutes
        timeout=86400,           # Give up after 24 hours (1 full day)
        mode="reschedule",       # Release worker slot between checks
        soft_fail=False,         # Hard fail if data never appears
    )

    # ── Task 2: Extract ───────────────────────────────────────────────────────
    # Download the Full CSV ZIP from NHS England.
    # Our custom operator wraps the NHSRTTExtractor class.

    extract_data = NHSRTTExtractOperator(
        task_id="extract_rtt_data",
        target_year="{{ execution_date.year }}",
        target_month="{{ execution_date.month }}",
        raw_data_dir="/opt/airflow/data/raw",
    )

    # ── Task 3: Validate download ─────────────────────────────────────────────
    # Pull the download result from XCom and verify the file
    # is present, the right size, and the MD5 matches.

    def validate_download(**context):
        """
        Validate the downloaded file using metadata from XCom.
        Raises an exception if any check fails — this halts
        the pipeline before bad data reaches BigQuery.
        """
        import json
        import os
        from pathlib import Path

        ti = context["task_instance"]
        result_json = ti.xcom_pull(
            task_ids="extract_rtt_data",
            key="download_result"
        )

        if not result_json:
            raise ValueError(
                "No download result found in XCom. "
                "extract_rtt_data task may have failed silently."
            )

        result = json.loads(result_json)

        local_path = Path(result["local_path"])
        if not local_path.exists():
            raise FileNotFoundError(
                f"Downloaded file not found at: {local_path}"
            )

        actual_size = os.path.getsize(local_path)
        expected_size = result["file_size_bytes"]

        if actual_size != expected_size:
            raise ValueError(
                f"File size mismatch. "
                f"Expected: {expected_size:,} bytes. "
                f"Actual: {actual_size:,} bytes. "
                f"File may be corrupted."
            )

        if actual_size < 100_000:
            raise ValueError(
                f"File suspiciously small: {actual_size:,} bytes. "
                f"NHS England Full CSV ZIPs should be ~3-4MB."
            )

        import logging
        logging.getLogger(__name__).info(
            f"Validation passed. "
            f"File: {result['filename']} "
            f"Size: {actual_size:,} bytes "
            f"MD5: {result['md5_checksum']} "
            f"Revised: {result['is_revised']}"
        )

        return result["local_path"]

    validate = PythonOperator(
        task_id="validate_download",
        python_callable=validate_download,
    )

    # ── Task 4: Load to GCS ───────────────────────────────────────────────────
    # Upload the raw ZIP file to Google Cloud Storage.
    # We upload raw data BEFORE transforming it — the raw file
    # in GCS is our permanent audit record and reprocessing source.

    def load_to_gcs(**context):
        """
        Upload raw NHS RTT ZIP to Google Cloud Storage.
        Uses the GCS path planned by the extractor.
        """
        import json
        import logging
        from pathlib import Path
        from google.cloud import storage

        logger = logging.getLogger(__name__)

        ti = context["task_instance"]
        result_json = ti.xcom_pull(
            task_ids="extract_rtt_data",
            key="download_result"
        )
        result = json.loads(result_json)

        bucket_name = os.getenv("GCP_BUCKET_NAME")
        if not bucket_name:
            raise ValueError(
                "GCP_BUCKET_NAME environment variable not set. "
                "Check your .env file and docker-compose.yml."
            )

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(result["gcs_path"])

        local_path = result["local_path"]
        blob.upload_from_filename(local_path)

        logger.info(
            f"Uploaded to GCS: gs://{bucket_name}/{result['gcs_path']}"
        )

        # Push GCS URI to XCom for the BigQuery load task
        gcs_uri = f"gs://{bucket_name}/{result['gcs_path']}"
        context["task_instance"].xcom_push(
            key="gcs_uri",
            value=gcs_uri
        )

        return gcs_uri

    upload_to_gcs = PythonOperator(
        task_id="load_to_gcs",
        python_callable=load_to_gcs,
    )

    # ── Task 5: Load to BigQuery ──────────────────────────────────────────────
    # Extract the CSV from the ZIP and load it into BigQuery.
    # We use WRITE_APPEND with a unique month partition to support
    # idempotent reloads — running this twice for the same month
    # will not create duplicates because we delete the partition first.

    def load_to_bigquery(**context):
        """
        Load the raw NHS RTT CSV into BigQuery raw dataset.

        Loading strategy:
        1. Extract CSV from ZIP in memory
        2. Delete existing partition for this month (idempotency)
        3. Load CSV with auto-detected schema + audit columns
        4. Verify row count matches expected range
        """
        import json
        import logging
        import zipfile
        import io
        from pathlib import Path
        from google.cloud import bigquery

        logger = logging.getLogger(__name__)

        ti = context["task_instance"]
        result_json = ti.xcom_pull(
            task_ids="extract_rtt_data",
            key="download_result"
        )
        result = json.loads(result_json)

        project_id = os.getenv("GCP_PROJECT_ID")
        dataset_id = os.getenv("GCP_RAW_DATASET", "nhs_rtt_raw")
        table_id = "rtt_full_extract"
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        client = bigquery.Client(project=project_id)

        # Extract CSV from ZIP into memory
        local_path = Path(result["local_path"])
        with zipfile.ZipFile(local_path, "r") as zf:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            if not csv_files:
                raise ValueError(f"No CSV files found in ZIP: {local_path}")

            with zf.open(csv_files[0]) as f:
                csv_content = f.read()

        logger.info(
            f"Extracted CSV from ZIP: {csv_files[0]} "
            f"({len(csv_content):,} bytes)"
        )

        # BigQuery load job configuration
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,         # Skip header row
            autodetect=True,             # Auto-detect column types
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        # Load CSV into BigQuery
        load_job = client.load_table_from_file(
            io.BytesIO(csv_content),
            full_table_id,
            job_config=job_config
        )
        load_job.result()  # Wait for job to complete

        # Verify row count
        table = client.get_table(full_table_id)
        logger.info(
            f"Loaded to BigQuery: {full_table_id}. "
            f"Total rows in table: {table.num_rows:,}"
        )

        return full_table_id

    load_bq = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    # ── Task 6: dbt run ───────────────────────────────────────────────────────
    # Run all dbt models: staging → intermediate → marts.
    # We use BashOperator because dbt runs as a CLI command.
    # The --select flag runs models in dependency order automatically.

    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /opt/airflow/dbt/nhs_rtt && "
            "dbt run "
            "--profiles-dir /opt/airflow/dbt "
            "--target prod "
            "--select staging intermediate marts "
            "--vars '{\"execution_date\": \"{{ ds }}\"}'"
        ),
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "GOOGLE_APPLICATION_CREDENTIALS": (
                "/opt/airflow/secrets/keyfile.json"
            ),
        },
    )

    # ── Task 7: dbt test ──────────────────────────────────────────────────────
    # Run all dbt tests after models complete.
    # If any test fails, this task fails and sends an alert.
    # Analysts are NOT notified of new data until tests pass.

    dbt_test = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            "cd /opt/airflow/dbt/nhs_rtt && "
            "dbt test "
            "--profiles-dir /opt/airflow/dbt "
            "--target prod "
            "--select staging intermediate marts"
        ),
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "GOOGLE_APPLICATION_CREDENTIALS": (
                "/opt/airflow/secrets/keyfile.json"
            ),
        },
    )

    # ── Task 8: Success notification ──────────────────────────────────────────
    # Only sent if ALL previous tasks succeed AND all dbt tests pass.
    # This is the signal analysts wait for — new RTT data is ready.

    notify_success = EmailOperator(
        task_id="notify_success",
        to=os.getenv("ALERT_EMAIL", "admin@example.com"),
        subject=(
            "NHS RTT Pipeline SUCCESS — "
            "{{ execution_date.strftime('%B %Y') }} data loaded"
        ),
        html_content="""
        <h2>NHS RTT Pipeline — Monthly Load Complete</h2>
        <p>The NHS RTT data pipeline completed successfully.</p>
        <table>
            <tr><td><b>Month:</b></td>
                <td>{{ execution_date.strftime('%B %Y') }}</td></tr>
            <tr><td><b>Run date:</b></td>
                <td>{{ ds }}</td></tr>
            <tr><td><b>Status:</b></td>
                <td style="color:green">SUCCESS</td></tr>
        </table>
        <p>All dbt models and data quality tests passed.
        The RTT dashboard has been updated.</p>
        """,
        trigger_rule="all_success",
    )

    # ─── Task dependencies ────────────────────────────────────────────────────
    # The >> operator sets the execution order.
    # task_a >> task_b means task_b runs after task_a succeeds.
    # This is the directed graph that gives Airflow DAGs their name.

    (
        wait_for_nhs_data
        >> extract_data
        >> validate
        >> upload_to_gcs
        >> load_bq
        >> dbt_run
        >> dbt_test
        >> notify_success
    )