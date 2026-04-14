# NHS RTT Pipeline — Data Dictionary

**Last updated:** April 2026
**Data Engineer/Plumber:** Ose Omokhua - Insightful Algorithms
**Source:** NHS England Statistical Releases
**Licence:** Open Government Licence v3.0
**Dashboard:** https://lookerstudio.google.com/reporting/04501192-988b-4c52-835f-0fd814d68f79

---

## Who should read this

| Role | Start here |
|---|---|
| Data Analyst | Read the `fact_rtt_performance` section |
| Data Scientist | Read both `fact_rtt_performance` and `stg_nhs_rtt__full_extract` |
| Dashboard user | Use the live Looker Studio dashboard - no SQL needed |

---

## BigQuery project and datasets

**Project:** `nhs-rtt-pipeline-oo-2026`
**Region:** europe-west2 (London) - UK data residency compliant

| Dataset | Who uses it | Purpose |
|---|---|---|
| `nhs_rtt_raw` | Pipeline only | Raw data exactly as published by NHS England - do not query directly |
| `nhs_rtt_dbt_staging` | Data scientists only | Cleaned and typed - all 121 columns available |
| `nhs_rtt_dbt_intermediate` | Pipeline only | Business logic layer - internal pipeline step |
| `nhs_rtt_dbt_marts` | Analysts and data scientists | Analyst-ready dimensional models - start here |
| `nhs_rtt_dbt_seeds` | Both | Clinical specialty reference data |
| `snapshots` | Advanced use | SCD Type 2 trust name history |

---

## For Data Analysts — primary table

### `nhs_rtt_dbt_marts.fact_rtt_performance`

**This is the only table you need for the vast majority of analysis.**

**What it contains:** Incomplete RTT pathways (Part_2) only.
These are patients currently waiting for treatment -
the data used to monitor the 18-week constitutional standard.

**Grain:** One row per provider trust × clinical specialty × reporting month.

**Current coverage:** August 2025 - January 2026 (6 months)

**Row count:** 311,800 rows

---

### Date and time fields

| Field | Type | Description | Example |
|---|---|---|---|
| `period_date` | DATE | First day of the reporting month | `2026-01-01` |
| `financial_year` | STRING | NHS financial year (April–March) | `2025-26` |
| `calendar_year` | INT64 | Calendar year | `2026` |
| `calendar_month` | INT64 | Month number 1-12 | `1` |
| `month_name` | STRING | Full month name | `January` |
| `month_label` | STRING | Short label for charts | `Jan 2026` |

---

### Provider (trust) fields

| Field | Type | Description | Example |
|---|---|---|---|
| `provider_org_code` | STRING | NHS ODS code for the trust | `RRK` |
| `provider_org_name` | STRING | Full trust name | `University Hospitals Birmingham NHS FT` |
| `provider_parent_org_code` | STRING | ICB ODS code | `QHL` |
| `provider_parent_name` | STRING | Full ICB name | `NHS West Midlands ICB` |

**Coverage:** 517 unique provider trusts, 42 Integrated Care Boards

---

### Commissioner fields

| Field | Type | Description | Example |
|---|---|---|---|
| `commissioner_org_code` | STRING | Sub-ICB Location code | `15N` |
| `commissioner_org_name` | STRING | Sub-ICB Location name | `NHS Birmingham and Solihull` |
| `commissioner_parent_org_code` | STRING | ICB code | `QHL` |
| `commissioner_parent_name` | STRING | ICB name | `NHS West Midlands ICB` |

---

### Clinical specialty fields

| Field | Type | Description | Example |
|---|---|---|---|
| `treatment_function_code` | STRING | NHS specialty code | `C_110` |
| `treatment_function_name` | STRING | Specialty name | `Trauma and Orthopaedic Service` |
| `specialty_group` | STRING | Broad category | `Surgical` |
| `is_surgical` | BOOLEAN | True if surgical | `true` |

**The 24 clinical specialties:**

| Code | Specialty | Group |
|---|---|---|
| C_100 | General Surgery Service | Surgical |
| C_101 | Urology Service | Surgical |
| C_110 | Trauma and Orthopaedic Service | Surgical |
| C_120 | Ear Nose and Throat Service | Surgical |
| C_130 | Ophthalmology Service | Surgical |
| C_140 | Oral Surgery Service | Surgical |
| C_150 | Neurosurgery Service | Surgical |
| C_160 | Plastic Surgery Service | Surgical |
| C_170 | Cardiothoracic Surgery Service | Surgical |
| C_190 | General Medicine Service | Medical |
| C_300 | Dermatology Service | Medical |
| C_301 | Gastroenterology Service | Medical |
| C_320 | Cardiology Service | Medical |
| C_340 | Respiratory Medicine Service | Medical |
| C_400 | Neurology Service | Medical |
| C_410 | Rheumatology Service | Medical |
| C_502 | Gynaecology Service | Surgical |
| C_504 | Obstetrics Service | Medical |
| C_711 | Diagnostic Imaging Service | Diagnostic |
| X02 | Other - Medical Services | Other |
| X05 | Other - Surgical Services | Other |
| X09 | Other - Mental Health Services | Other |

---

### Patient count metrics

| Field | Type | Description |
|---|---|---|
| `total_incomplete` | INT64 | Total patients currently waiting |
| `patients_within_18_weeks` | INT64 | Patients waiting 18 weeks or less |
| `patients_beyond_18_weeks` | INT64 | Patients waiting more than 18 weeks |
| `patients_waiting_52_plus_weeks` | INT64 | Patients waiting more than 1 year |
| `patients_waiting_over_104_weeks` | INT64 | Patients waiting more than 2 years |

---

### Performance metrics

| Field | Type | Description | Notes |
|---|---|---|---|
| `pct_within_18_weeks` | FLOAT64 | Percentage within 18 weeks | Constitutional standard = 92% |
| `meets_18_week_standard` | BOOLEAN | True if pct >= 92% | False = in breach |
| `rag_status` | STRING | Traffic light rating | Green / Amber / Red |
| `breach_severity` | STRING | Worst breach category | Within Standard / Over 18 Weeks / Over 1 Year / Over 2 Years |

**RAG status thresholds:**

| Status | Threshold | Meaning |
|---|---|---|
| Green | >= 92.0% | Meeting constitutional standard |
| Amber | 85.0% to 91.9% | Underperforming, not in formal breach |
| Red | < 85.0% | Formal breach - regulatory attention likely |

---

### Audit fields

| Field | Type | Description |
|---|---|---|
| `rtt_performance_id` | STRING | Unique row identifier (surrogate key) |
| `_loaded_at` | TIMESTAMP | When this record was processed by the pipeline |
| `_source` | STRING | Source system identifier |

---

## For Data Scientists - additional table

### `nhs_rtt_dbt_staging.stg_nhs_rtt__full_extract`

**Use this in addition to `fact_rtt_performance` when you need
the full week-band granularity for feature engineering.**

**What it adds:** 104 individual week-band columns showing exactly
how many patients are waiting in each one-week interval.
These are collapsed into summary metrics in the mart.

**Grain:** One row per provider × commissioner × specialty × pathway type × month.
This includes ALL pathway types - not just Incomplete Pathways.

**Row count:** 1,092,767 rows (all 6 months, all pathway types)

---

### Key staging columns for data scientists

| Field | Description | Use in modelling |
|---|---|---|
| `period_date` | Reporting month | Time index |
| `rtt_part_type` | Pathway type (Part_1A/1B/2/2A/3) | Filter to Part_2 for incomplete pathways |
| `provider_org_code` | Trust identifier | Categorical feature |
| `treatment_function_code` | Specialty code | Categorical feature |
| `wks_00_to_01` through `wks_17_to_18` | Patients within standard by week | Distribution features |
| `wks_18_to_19` through `wks_over_104` | Patients beyond standard by week | Distribution features |
| `total_all` | Total patients in this row | Primary count |
| `_is_revised` | True if NHS England revised this month | Data quality flag |
| `_month_code` | NHS month code e.g. Jan26 | Join key |

**Important:** Filter to `rtt_part_type = 'Part_2'` for
Incomplete Pathways analysis. Other part types represent
completed pathways and new periods - not the waiting list.

---

## Reference table - both audiences

### `nhs_rtt_dbt_seeds.treatment_functions`

| Field | Type | Description |
|---|---|---|
| `treatment_function_code` | STRING | NHS specialty code |
| `treatment_function_name` | STRING | Full specialty name |
| `specialty_group` | STRING | Surgical / Medical / Diagnostic / Other |
| `is_surgical` | BOOLEAN | True if surgical specialty |

**24 rows** - one per clinical specialty.
Already joined into `fact_rtt_performance` — only needed
if working directly with raw or staging data.

---

## Advanced use - trust name history

### `snapshots.snap_dim_provider`

**Use this only for trend analysis that spans trust mergers
or name changes (advanced use cases).**

| Field | Description |
|---|---|
| `provider_org_code` | Trust ODS code |
| `provider_org_name` | Trust name at this point in time |
| `provider_parent_org_code` | ICB code at this point in time |
| `provider_parent_name` | ICB name at this point in time |
| `dbt_valid_from` | When this version of the record became active |
| `dbt_valid_to` | When this version expired (NULL = currently active) |
| `dbt_is_current` | True if this is the current record |

**How to use for historical analysis:**
```sql
SELECT f.*, p.provider_org_name AS name_at_time_of_reporting
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance` f
LEFT JOIN `nhs-rtt-pipeline-oo-2026.snapshots.snap_dim_provider` p
    ON f.provider_org_code = p.provider_org_code
    AND f.period_date BETWEEN p.dbt_valid_from
    AND COALESCE(p.dbt_valid_to, CURRENT_DATE())
```

---

## Known data limitations

**1. Six months of data only**
Current dataset covers August 2025 to January 2026.
February and March 2026 not yet published by NHS England as
of April 2026. Data grows monthly as NHS England publishes.

**2. Revised data in August and September 2025**
These months contain figures revised by NHS England on
12th February 2026. They supersede originally published values
and are the most accurate figures available. The `_is_revised`
flag in the staging table identifies these months.

**3. Missing trust submissions**
A trust that does not submit data for a given month will be
absent from that month entirely - not represented as zeros.
Do not treat missing rows as zero activity.

**4. Aggregated data only**
This is trust-specialty-month level data. Individual patient
records are not available. Suitable for organisational-level
analysis only - not patient-level research.

**5. Suppressed values**
NHS England suppresses counts below 5 to protect patient
confidentiality. At trust-specialty aggregation level this
is rare but possible for very small or specialist trusts.

---

## Data refresh schedule

New data loads monthly when NHS England publishes.
NHS England typically publishes approximately 6 weeks
after the reporting month ends.

| Month | Approximate publication date |
|---|---|
| February 2026 | Mid-April to May 2026 |
| March 2026 | May to June 2026 |

To load new data when published:
```bash
python -m extract.backfill --months 1 --delay 3.0
python -m load.load_to_bigquery
cd dbt/nhs_rtt && dbt run --profiles-dir ../
cd dbt/nhs_rtt && dbt test --profiles-dir ../
```