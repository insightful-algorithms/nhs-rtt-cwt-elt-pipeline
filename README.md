# NHS RTT & CWT Batch ELT Pipeline

> Production-grade batch ELT pipeline for NHS Referral To Treatment (RTT)
> and Cancer Waiting Times (CWT) data — built with Python, Apache Airflow,
> dbt and Google BigQuery.

[![NHS RTT Pipeline CI](https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline/actions/workflows/ci.yml)

---

## The Problem This Solves

Over 7.6 million patients are on NHS waiting lists in England. NHS trusts
are legally required to treat referred patients within **18 weeks** of
referral and cancer patients within **62 days** of urgent GP referral.

Despite the critical importance of this data, many trusts and ICBs still
process RTT and CWT data manually — downloading CSVs, running Excel macros,
and emailing spreadsheets. This pipeline replaces that entire manual process
with a robust, automated, auditable ELT pipeline.

---

## Architecture

![Architecture](docs/architecture/architecture.md)

| Layer          | Tool                 | Purpose                               |
| -------------- | -------------------- | ------------------------------------- |
| Orchestration  | Apache Airflow 2.9   | Monthly scheduling, retries, alerting |
| Extraction     | Python + Requests    | Download NHS statistical releases     |
| Validation     | Great Expectations   | Schema and threshold checks           |
| Storage        | Google Cloud Storage | Raw file landing zone (London)        |
| Warehouse      | Google BigQuery      | Scalable analytical storage           |
| Transformation | dbt 1.8              | Staging → Intermediate → Marts        |
| Data Quality   | dbt tests            | 18-week threshold enforcement         |
| CI/CD          | GitHub Actions       | Automated quality gates               |

---

## Live Dashboard

**NHS RTT Performance Dashboard — England 2025-26**

Real-time performance monitoring for NHS Referral To Treatment
waiting times across all English trusts and specialties.

[View Live Dashboard](https://lookerstudio.google.com/reporting/04501192-988b-4c52-835f-0fd814d68f79)

| Metric                   | January 2026 |
| ------------------------ | ------------ |
| Total patients waiting   | 7,187,677    |
| Patients within 18 weeks | 4,412,147    |
| 18-week performance      | 60.94%       |
| Constitutional standard  | 92.0%        |
| Data last updated        | January 2026 |

---

## Data Sources

| Dataset              | Publisher   | Frequency | Volume            |
| -------------------- | ----------- | --------- | ----------------- |
| RTT Waiting Times    | NHS England | Monthly   | ~183k rows/month  |
| Cancer Waiting Times | NHS England | Quarterly | ~50k rows/quarter |

All data published under the
[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

---

## Key Engineering Decisions

**ELT over ETL** — Raw data lands in BigQuery unmodified before transformation.
The audit trail is preserved and any month can be reprocessed without
re-downloading source files.

**UK data residency** — All GCP resources use `europe-west2` (London) to meet
NHS information governance requirements.

**Idempotent pipeline** — Each run can be re-executed safely. BigQuery
partitioning by month ensures reruns overwrite rather than append.

**SCD Type 2** — NHS trusts change names and merge over time. Slowly changing
dimension tracking ensures historical performance is attributed to the correct
entity.

**Event-driven extraction** — A custom Airflow sensor polls NHS England before
downloading, ensuring the pipeline waits for publication rather than failing
on an empty page.

---

## Project Structure

```
nhs-rtt-cwt-elt-pipeline/
├── extract/                    # NHS data extraction
│   ├── config.py               # URLs, financial year logic
│   └── nhs_rtt_extractor.py    # Extraction class with audit logging
├── airflow/
│   ├── dags/                   # Pipeline DAG definitions
│   └── plugins/                # Custom sensors and operators
├── dbt/nhs_rtt/
│   ├── models/
│   │   ├── staging/            # Clean and type source data
│   │   ├── intermediate/       # 18-week metrics calculation
│   │   └── marts/              # Analyst-ready dimensional models
│   ├── seeds/                  # Treatment function reference data
│   └── snapshots/              # SCD Type 2 provider dimension
├── great_expectations/         # Data quality expectation suites
├── tests/
│   ├── unit/                   # Unit tests for Python modules
│   └── nhs_rtt_validations.py  # NHS business rule validator
├── docs/architecture/          # Architecture diagrams
└── .github/workflows/          # CI/CD pipeline definitions
```

---

---

## Getting Started

### Prerequisites

- Docker Desktop
- Google Cloud account with BigQuery and GCS enabled
- GCP service account with BigQuery Admin and Storage Admin roles
- Python 3.11+

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline.git
cd nhs-rtt-cwt-elt-pipeline

# 2. Configure environment variables
cp .env.example .env
# Edit .env with your GCP credentials

# 3. Start Airflow
docker-compose up airflow-init
docker-compose up -d

# 4. Open Airflow UI
# http://localhost:8080
# Default credentials set in .env

# 5. Install Python dependencies locally
python -m venv venv
source venv/Scripts/activate  # Windows
pip install -r requirements.txt
```

### Running dbt locally

```bash
cd dbt/nhs_rtt
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate
dbt docs serve
```

---

## Data Quality Standards

| Check                       | Layer        | Action on failure   |
| --------------------------- | ------------ | ------------------- |
| 121 columns present         | Raw          | Block BigQuery load |
| No null provider codes      | Staging      | dbt test failure    |
| RTT Part Types valid        | Staging      | dbt test failure    |
| Total All never null        | Staging      | dbt test failure    |
| pct_within_18_weeks 0-100   | Intermediate | Pipeline alert      |
| Row count in expected range | Raw          | Block BigQuery load |
| No negative patient counts  | Raw          | Block BigQuery load |

---

## CI/CD Pipeline

Every pull request and push to `main` triggers:

1. **Black** — code formatting check
2. **isort** — import ordering check
3. **flake8** — linting
4. **pytest** — unit tests
5. **dbt validation** — project structure checks
6. **Security scan** — hardcoded credential detection

Branch protection rules require all checks to pass before merging.

---

## Licence

MIT — see [LICENSE](LICENSE)
