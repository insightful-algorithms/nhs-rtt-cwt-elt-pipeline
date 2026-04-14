# NHS RTT - Analyst SQL Starter Pack

**10 production-ready BigQuery queries**

All queries use:
`nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`

This is the only table you need. All metrics are pre-calculated.
All dimension names are clean and human-readable.

---

## How to run these queries

**BigQuery Console:**
Go to https://console.cloud.google.com/bigquery
Paste any query and click Run.

**Python (pandas):**

```python
from google.cloud import bigquery
client = bigquery.Client(project="nhs-rtt-pipeline-oo-2026")
df = client.query(YOUR_QUERY_HERE).to_dataframe()
```

**Looker Studio:**
Connect to `nhs_rtt_dbt_marts.fact_rtt_performance`
and use the field picker to build charts without SQL.

---

## Query 1 - National performance by month

The headline metric. England's 18-week performance
month by month across the 6-month dataset.

```sql
SELECT
    month_label,
    period_date,
    SUM(total_incomplete)                           AS total_waiting,
    SUM(patients_within_18_weeks)                   AS within_18_weeks,
    SUM(patients_beyond_18_weeks)                   AS beyond_18_weeks,
    ROUND(
        SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ), 1
    )                                               AS pct_within_18_weeks,
    SUM(patients_waiting_52_plus_weeks)             AS over_1_year,
    SUM(patients_waiting_over_104_weeks)            AS over_2_years
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
GROUP BY month_label, period_date
ORDER BY period_date;
```

---

## Query 2 - Trust league table for January 2026

All trusts ranked by 18-week performance.
Worst performers appear first.

```sql
SELECT
    provider_org_name,
    provider_parent_name                            AS icb,
    SUM(total_incomplete)                           AS total_waiting,
    ROUND(
        SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ), 1
    )                                               AS pct_within_18_weeks,
    SUM(patients_waiting_52_plus_weeks)             AS over_1_year,
    SUM(patients_waiting_over_104_weeks)            AS over_2_years,
    CASE
        WHEN SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ) >= 92 THEN 'Green'
        WHEN SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ) >= 85 THEN 'Amber'
        ELSE 'Red'
    END                                             AS rag_status
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
WHERE period_date = '2026-01-01'
GROUP BY provider_org_name, provider_parent_name
ORDER BY pct_within_18_weeks ASC;
```

---

## Query 3 - Performance by clinical specialty

Which specialties have the worst 18-week performance nationally?

```sql
SELECT
    treatment_function_name,
    specialty_group,
    is_surgical,
    ROUND(AVG(pct_within_18_weeks), 1)             AS avg_pct_within_18_weeks,
    SUM(total_incomplete)                           AS total_waiting_all_months,
    SUM(patients_waiting_over_104_weeks)            AS total_over_2_years
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
GROUP BY treatment_function_name, specialty_group, is_surgical
ORDER BY avg_pct_within_18_weeks ASC;
```

---

## Query 4 - ICB performance comparison

How do all 42 Integrated Care Boards compare?

```sql
SELECT
    provider_parent_name                            AS icb_name,
    provider_parent_org_code                        AS icb_code,
    period_date,
    month_label,
    SUM(total_incomplete)                           AS total_waiting,
    ROUND(
        SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ), 1
    )                                               AS pct_within_18_weeks,
    SUM(patients_waiting_over_104_weeks)            AS over_2_years
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
GROUP BY
    provider_parent_name,
    provider_parent_org_code,
    period_date,
    month_label
ORDER BY period_date, pct_within_18_weeks ASC;
```

---

## Query 5 - Patients waiting over 2 years

The most politically sensitive metric.
Which trusts have the most patients waiting
more than 104 weeks?

```sql
SELECT
    period_date,
    month_label,
    provider_org_name,
    provider_parent_name                            AS icb,
    treatment_function_name,
    SUM(patients_waiting_over_104_weeks)            AS patients_over_2_years,
    SUM(total_incomplete)                           AS total_waiting
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
WHERE patients_waiting_over_104_weeks > 0
GROUP BY
    period_date,
    month_label,
    provider_org_name,
    provider_parent_name,
    treatment_function_name
ORDER BY patients_over_2_years DESC;
```

---

## Query 6 - Single trust trend over time

Replace the trust name to analyse any specific trust.

```sql
SELECT
    month_label,
    period_date,
    treatment_function_name,
    total_incomplete,
    patients_within_18_weeks,
    pct_within_18_weeks,
    patients_waiting_52_plus_weeks,
    patients_waiting_over_104_weeks,
    rag_status
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
WHERE provider_org_name
    = 'UNIVERSITY HOSPITALS BIRMINGHAM NHS FOUNDATION TRUST'
ORDER BY period_date, treatment_function_name;
```

---

## Query 7 - RAG status distribution by month

What proportion of trust-specialty combinations
are Green, Amber, and Red each month?

```sql
SELECT
    month_label,
    period_date,
    rag_status,
    COUNT(*)                                        AS count,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY period_date),
        1
    )                                               AS pct_of_total
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
GROUP BY month_label, period_date, rag_status
ORDER BY period_date, rag_status;
```

---

## Query 8 - Surgical vs medical performance

Do surgical specialties perform differently
from medical specialties?

```sql
SELECT
    specialty_group,
    is_surgical,
    period_date,
    month_label,
    SUM(total_incomplete)                           AS total_waiting,
    ROUND(
        SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ), 1
    )                                               AS pct_within_18_weeks,
    SUM(patients_waiting_over_104_weeks)            AS over_2_years
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
GROUP BY specialty_group, is_surgical, period_date, month_label
ORDER BY period_date, specialty_group;
```

---

## Query 9 - Month-on-month waiting list change

Is the national waiting list growing or shrinking?
Uses LAG window function to calculate change.

```sql
WITH monthly_totals AS (
    SELECT
        period_date,
        month_label,
        SUM(total_incomplete)                       AS total_waiting,
        SUM(patients_within_18_weeks)               AS within_18_weeks,
        SUM(patients_waiting_over_104_weeks)        AS over_2_years
    FROM
        `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
    GROUP BY period_date, month_label
)

SELECT
    month_label,
    period_date,
    total_waiting,
    total_waiting
        - LAG(total_waiting) OVER (ORDER BY period_date)
                                                    AS mom_change,
    ROUND(
        SAFE_DIVIDE(
            (total_waiting
                - LAG(total_waiting) OVER (ORDER BY period_date)
            ) * 100.0,
            LAG(total_waiting) OVER (ORDER BY period_date)
        ),
        2
    )                                               AS mom_pct_change,
    within_18_weeks,
    over_2_years
FROM monthly_totals
ORDER BY period_date;
```

---

## Query 10 - Complete trust scorecard

A full performance scorecard for every trust
for January 2026. Ready to export to Excel or PowerPoint.

```sql
SELECT
    provider_org_code,
    provider_org_name,
    provider_parent_name                            AS icb,
    SUM(total_incomplete)                           AS total_waiting,
    SUM(patients_within_18_weeks)                   AS within_18_weeks,
    SUM(patients_beyond_18_weeks)                   AS beyond_18_weeks,
    ROUND(
        SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ), 1
    )                                               AS pct_within_18_weeks,
    SUM(patients_waiting_52_plus_weeks)             AS over_1_year,
    SUM(patients_waiting_over_104_weeks)            AS over_2_years,
    CASE
        WHEN SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ) >= 92 THEN 'Green — Meeting standard'
        WHEN SAFE_DIVIDE(
            SUM(patients_within_18_weeks) * 100.0,
            SUM(total_incomplete)
        ) >= 85 THEN 'Amber — Underperforming'
        ELSE 'Red — Formal breach'
    END                                             AS performance_status,
    COUNT(DISTINCT treatment_function_code)         AS specialties_reported
FROM `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
WHERE period_date = '2026-01-01'
GROUP BY
    provider_org_code,
    provider_org_name,
    provider_parent_name
ORDER BY pct_within_18_weeks ASC;
```
