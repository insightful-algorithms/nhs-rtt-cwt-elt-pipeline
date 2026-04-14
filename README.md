# NHS RTT & CWT Batch ELT Pipeline

> Production-grade batch ELT pipeline for NHS Referral To Treatment (RTT)
> and Cancer Waiting Times (CWT) data - built with Python, Apache Airflow,
> dbt and Google BigQuery.

[![NHS RTT Pipeline CI](https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline/actions/workflows/ci.yml)

---

## The Problem This Solves

Over 7.6 million patients are on NHS waiting lists in England.
NHS trusts are legally required to treat referred patients within
**18 weeks** of referral and cancer patients within **62 days** of
urgent GP referral. These are constitutional rights - not targets.

Despite the critical importance of this data, many trusts and ICBs
still process RTT and CWT data manually - downloading CSVs from NHS
England's website, running Excel macros, and emailing spreadsheets.

This pipeline replaces that entire manual process with a robust,
automated, auditable ELT pipeline - deployable by any NHS trust
or ICB within a day.

---

## Live Resources

| Resource                       | Link                                                                                            |
| ------------------------------ | ----------------------------------------------------------------------------------------------- |
| Live RTT Performance Dashboard | [Looker Studio](https://lookerstudio.google.com/reporting/04501192-988b-4c52-835f-0fd814d68f79) |
| Data Catalogue                 | [GitHub Pages](https://insightful-algorithms.github.io/nhs-rtt-cwt-elt-pipeline/)               |
| Source Code                    | [GitHub](https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline)                     |

---

## January 2026 Performance Snapshot

| Metric                                       | Value                                 |
| -------------------------------------------- | ------------------------------------- |
| Total patients waiting (incomplete pathways) | 7,187,677                             |
| Patients within 18 weeks                     | 4,412,147                             |
| 18-week performance                          | 60.94%                                |
| Constitutional standard                      | 92.0%                                 |
| Gap to standard                              | 31.06 percentage points               |
| Patients waiting over 2 years                | Tracked per trust and specialty       |
| Data last updated                            | January 2026                          |
| Data coverage                                | August 2025 — January 2026 (6 months) |
| Total rows in warehouse                      | 1,092,767                             |

## Architecture

```

NHS England         Airflow              Google Cloud Platform
Statistical   ───► Orchestrated    ───► GCS Raw Landing  ───► BigQuery Raw
Releases            Monthly DAG          (europe-west2)        Dataset
(CSV ZIPs)          + Sensor                                      │
                                                                  │
                                                                  ▼
Looker Studio ◄─── dbt Marts ◄────── dbt Intermediate ◄─── dbt Staging
Dashboard           fact_rtt_           18-week metrics       Cleaned +
(Live)              performance         RAG status            typed view
```

## ![Architecture](docs/architecture/architecture.md)

Full architecture diagram: [`docs/architecture/architecture.md`](docs/architecture/architecture.md)

---

## Tech Stack

| Layer            | Tool                                   | Purpose                                   |
| ---------------- | -------------------------------------- | ----------------------------------------- |
| Orchestration    | Apache Airflow 2.9                     | Monthly scheduling, retries, SLA alerting |
| Extraction       | Python 3.11 + Requests + BeautifulSoup | Download NHS statistical releases         |
| Storage          | Google Cloud Storage (europe-west2)    | Raw file landing zone                     |
| Warehouse        | Google BigQuery (europe-west2)         | Scalable analytical storage               |
| Transformation   | dbt-bigquery 1.8                       | Staging → Intermediate → Marts            |
| Data Quality     | Great Expectations 0.18                | Schema and threshold validation           |
| Containerisation | Docker + Docker Compose                | Reproducible local environment            |
| CI/CD            | GitHub Actions                         | Automated quality gates on every PR       |
| Dashboard        | Looker Studio                          | Live RTT performance dashboard            |

---

## Data Sources

| Dataset              | Publisher   | Frequency | Licence  |
| -------------------- | ----------- | --------- | -------- |
| RTT Waiting Times    | NHS England | Monthly   | OGL v3.0 |
| Cancer Waiting Times | NHS England | Quarterly | OGL v3.0 |

All data published under the
[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

---

## What Is In BigQuery

| Table                          | Dataset                    | Rows      | Description                          |
| ------------------------------ | -------------------------- | --------- | ------------------------------------ |
| `rtt_full_extract`             | `nhs_rtt_raw`              | 1,092,767 | Raw data as published by NHS England |
| `treatment_functions`          | `nhs_rtt_dbt_seeds`        | 24        | Clinical specialty reference         |
| `stg_nhs_rtt__full_extract`    | `nhs_rtt_dbt_staging`      | 1,092,767 | Cleaned and typed view               |
| `int_rtt__incomplete_pathways` | `nhs_rtt_dbt_intermediate` | 311,800   | 18-week metrics calculated           |
| `fact_rtt_performance`         | `nhs_rtt_dbt_marts`        | 311,800   | Analyst-ready fact table             |
| `snap_dim_provider`            | `snapshots`                | 527       | SCD Type 2 trust history             |

---

## Key Engineering Decisions

**ELT over ETL**
Raw data lands in BigQuery unmodified before transformation.
The audit trail is preserved and any month can be reprocessed
if transformation logic changes.

**UK data residency**
All GCP resources use `europe-west2` (London) to meet NHS
information governance requirements.

**Idempotent loads**
Each pipeline run can be re-executed safely. BigQuery
month-level delete-before-insert ensures reruns overwrite
rather than append.

**SCD Type 2 for providers**
NHS trusts change names and merge. SCD Type 2 tracking ensures
historical performance data is attributed to the correct
entity at the correct point in time.

**Event-driven extraction**
A custom Airflow sensor polls NHS England before downloading,
ensuring the pipeline waits for publication rather than failing
on an empty page.

**Revised data handling**
NHS England frequently revises historical months. The extractor
detects revised files via MD5 checksum comparison and flags
them with `_is_revised = True` in the audit trail.

---

## Project Structure

```
## Project Structure

nhs-rtt-cwt-elt-pipeline/
├── extract/
│   ├── config.py              # NHS URLs, financial year logic
│   ├── nhs_rtt_extractor.py   # Extraction class with audit logging
│   └── backfill.py            # Historical data backfill script
├── load/
│   ├── bigquery_loader.py     # BigQuery loader with idempotent reload
│   └── load_to_bigquery.py    # Load runner script
├── airflow/
│   ├── dags/
│   │   └── nhs_rtt_pipeline.py    # 8-task production DAG
│   └── plugins/
│       ├── nhs_rtt_sensor.py      # Custom NHS data availability sensor
│       └── nhs_rtt_operator.py    # Custom extraction operator
├── dbt/nhs_rtt/
│   ├── models/
│   │   ├── staging/               # stg_nhs_rtt__full_extract
│   │   ├── intermediate/          # int_rtt__incomplete_pathways
│   │   └── marts/                 # fact_rtt_performance
│   ├── seeds/
│   │   └── treatment_functions.csv
│   └── snapshots/
│       └── snap_dim_provider.sql  # SCD Type 2
├── great_expectations/
│   └── expectations/
│       └── nhs_rtt_raw_suite.json # 10 data quality checks
├── tests/
│   ├── unit/
│   │   └── test_config.py         # 10 unit tests
│   └── nhs_rtt_validations.py     # 10 NHS business rule validators
├── docs/
│   ├── architecture/
│   │   └── architecture.md        # System architecture diagram
│   └── handover/
│       ├── DATA_DICTIONARY.md     # Every field documented
│       ├── ANALYST_STARTER_PACK.md  # 10 ready-to-run SQL queries
│       └── DATA_SCIENTIST_GUIDE.md  # Modelling guide and caveats
└── .github/
│
└── workflows/
│
├── ci.yml                 # CI pipeline (black, isort, flake8, pytest)
│
└── dbt-docs.yml           # Auto-deploy data catalogue to GitHub Pages
```

---

## Getting Started

### Prerequisites

- Docker Desktop installed and running
- Google Cloud account with BigQuery and GCS enabled
- GCP service account key with BigQuery Admin and Storage Admin roles
- Python 3.11+
- Git

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline.git
cd nhs-rtt-cwt-elt-pipeline

# 2. Create virtual environment
python -m venv venv
source venv/Scripts/activate  # Windows Git Bash
# source venv/bin/activate    # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Edit .env with your GCP credentials and project details

# 5. Start Airflow locally
docker-compose up airflow-init
docker-compose up -d

# 6. Access Airflow UI at http://localhost:8080
```

### Download historical data

```bash
# Dry run — see what would be downloaded
python -m extract.backfill --months 6 --dry-run

# Download last 6 months
python -m extract.backfill --months 6 --delay 3.0

# Download specific date range
python -m extract.backfill --start 2025-08 --end 2026-01 --delay 3.0
```

### Load to BigQuery

```bash
# Load all downloaded files
python -m load.load_to_bigquery

# Load specific month only
python -m load.load_to_bigquery --month Jan26
```

### Run dbt

```bash
cd dbt/nhs_rtt

# Install packages
dbt deps --profiles-dir ../

# Load reference data
dbt seed --profiles-dir ../

# Run all models
dbt run --profiles-dir ../

# Run all tests
dbt test --profiles-dir ../

# Run snapshot (SCD Type 2)
dbt snapshot --profiles-dir ../

# Generate documentation site
dbt docs generate --profiles-dir ../
dbt docs serve --profiles-dir ../
```

---

## Data Quality Standards

| Check                          | Layer   | Action on failure   |
| ------------------------------ | ------- | ------------------- |
| Row count 50k-300k             | Raw     | Block BigQuery load |
| Column count equals 121        | Raw     | Block BigQuery load |
| Period format valid            | Raw     | Block BigQuery load |
| RTT Part Types known values    | Raw     | Block BigQuery load |
| ICB count equals 42            | Raw     | Warning             |
| No negative patient counts     | Raw     | Block BigQuery load |
| No null provider codes         | Staging | dbt test failure    |
| No null Total All              | Staging | dbt test failure    |
| RTT Part Types accepted values | Staging | dbt test failure    |
| No null period dates           | Staging | dbt test failure    |

---

## CI/CD Pipeline

Every pull request triggers three automated jobs:

| Job                  | Checks                           | Purpose                         |
| -------------------- | -------------------------------- | ------------------------------- |
| Python Code Quality  | black, isort, flake8, pytest     | Formatting, linting, unit tests |
| dbt Model Validation | Structure, CSV, doc completeness | dbt project integrity           |
| Security Scan        | Credential pattern matching      | No secrets in code              |

Branch protection rules require all three to pass before merge.
No code reaches main without CI passing.

---

## For Data Consumers

**Data Analysts** — start with `docs/handover/DATA_DICTIONARY.md`
and `docs/handover/ANALYST_STARTER_PACK.md`

**Data Scientists** — start with `docs/handover/DATA_SCIENTIST_GUIDE.md`

**Primary analytical table:** nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance

---

## **Author**

---

**Ose Omokhua**
MSc Data Science · BSc Physics
London, UK

Open to Data Engineer & Data Scientist roles (UK and Remote)

[![GitHub](https://img.shields.io/badge/GitHub-insightful--algorithms-181717?style=flat&logo=github&logoColor=white)](https://github.com/insightful-algorithms)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/omokhua-ose)

---
