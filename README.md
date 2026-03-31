# NHS RTT & CWT Batch ELT Pipeline

> An end-to-end, production-grade batch ELT pipeline for NHS Referral To
> Treatment (RTT) and Cancer Waiting Times (CWT) data — built with Python,
> Apache Airflow, dbt and Google BigQuery.

---

## The Problem This Solves

Over 7.6 million patients are on NHS waiting lists in England.
NHS trusts are legally required to treat referred patients within **18 weeks**
and cancer patients within **62 days**. Performance against these standards
is monitored nationally by NHS England and reported to ministers monthly.

Despite the critical importance of this data, many trusts and ICBs still
process RTT and CWT data manually — downloading CSVs, running Excel macros,
and emailing spreadsheets. This pipeline replaces that entire process with
a robust, automated, auditable ELT pipeline.

---

## Architecture

```
NHS England         Airflow           Google Cloud
Statistical   ───►  Orchestrated  ──► BigQuery Raw    ──► dbt Staging
Releases            Monthly DAG       Dataset             Models
(CSV files)                                               │
                                                          ▼
Looker Studio ◄── dbt Marts ◄─── dbt Intermediate ◄── dbt Staging
Dashboard          (Analysts)        (Business logic)    (Typed + cleaned)
```

> Full architecture diagram: `docs/architecture/architecture.png`

---

## Data Sources

| Dataset              | Source             | Update Frequency | Rows (approx) |
| -------------------- | ------------------ | ---------------- | ------------- |
| RTT Waiting Times    | NHS England        | Monthly          | ~500K/month   |
| Cancer Waiting Times | NHS England        | Quarterly        | ~50K/quarter  |
| A&E Attendances      | NHS England SitRep | Weekly           | ~20K/week     |

All data is publicly available under the
[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

---

## Tech Stack

| Layer            | Tool                   | Purpose                           |
| ---------------- | ---------------------- | --------------------------------- |
| Orchestration    | Apache Airflow 2.9     | DAG scheduling, retries, alerting |
| Extraction       | Python 3.11 + Requests | Download NHS CSV releases         |
| Storage          | Google Cloud Storage   | Raw file landing zone             |
| Warehouse        | Google BigQuery        | Scalable analytical storage       |
| Transformation   | dbt-bigquery 1.8       | Staging → Marts transformation    |
| Data Quality     | Great Expectations     | Schema + threshold validation     |
| Containerisation | Docker + Compose       | Reproducible local environment    |
| CI/CD            | GitHub Actions         | Automated testing on every PR     |
| Infrastructure   | Terraform              | Cloud resource provisioning       |

---

## Project Structure

```
nhs-rtt-cwt-elt-pipeline/
├── airflow/
│   ├── dags/          # Airflow DAG definitions
│   ├── plugins/       # Custom Airflow operators and hooks
│   └── logs/          # Pipeline execution logs
├── dbt/nhs_rtt/
│   ├── models/
│   │   ├── staging/       # Raw → typed and renamed
│   │   ├── intermediate/  # Business logic applied
│   │   └── marts/         # Analyst-ready dimensional models
│   ├── tests/         # Custom dbt data tests
│   ├── macros/        # Reusable SQL macros
│   └── seeds/         # Static reference data (trust codes etc.)
├── extract/           # NHS data extraction scripts
├── load/              # BigQuery loading utilities
├── tests/
│   ├── unit/          # Unit tests for Python functions
│   └── integration/   # End-to-end pipeline tests
├── docs/architecture/ # Architecture diagrams
└── .github/workflows/ # CI/CD pipeline definitions
```

---

## Getting Started

### Prerequisites

- Docker Desktop installed and running
- Google Cloud account with BigQuery and GCS enabled
- GCP service account key with BigQuery and GCS permissions
- Python 3.11+

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline.git
cd nhs-rtt-cwt-elt-pipeline

# 2. Configure environment variables
cp .env.example .env
# Edit .env with your GCP credentials and project details

# 3. Initialise and start Airflow
docker-compose up airflow-init
docker-compose up -d

# 4. Access the Airflow UI
# Open http://localhost:8080 in your browser
# Username: admin  Password: (set in .env)

# 5. Install Python dependencies locally (for dbt and testing)
pip install -r requirements.txt
```

---

## Pipeline Overview

The pipeline runs on the **first working day of each month**, aligned
with NHS England's RTT publication schedule.

```
[Sensor: NHS data published?]
        │
        ▼
[Extract: Download CSV files]
        │
        ▼
[Validate: Schema checks]
        │
        ▼
[Load: Upload to GCS + BigQuery raw]
        │
        ▼
[Transform: dbt run]
        │
        ▼
[Test: dbt test + Great Expectations]
        │
        ▼
[Alert: Email on success or failure]
```

---

## Data Quality Standards

This pipeline enforces the following checks at every layer:

| Check                                   | Layer        | Threshold                    |
| --------------------------------------- | ------------ | ---------------------------- |
| No null patient counts                  | Staging      | Zero tolerance               |
| RTT 18-week incomplete pathways ≥ 0     | Staging      | Zero tolerance               |
| Trust ODS code in reference list        | Staging      | Zero tolerance               |
| 18-week performance ≤ 100%              | Intermediate | Warning if breached          |
| Monthly row count within expected range | Marts        | Alert if ±20% of prior month |

---

## Licence

MIT — see [LICENSE](LICENSE)

---

## Author

Built by Ose Omokhua as part of a professional Data Engineering portfolio.
