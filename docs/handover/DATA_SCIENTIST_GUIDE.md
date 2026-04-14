# NHS RTT — Data Scientist Guide

## Datasets for data scientists

You have access to two tables:

**Table 1 — Analyst mart (start here):** nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance

311,800 rows. Incomplete pathways only. Pre-calculated metrics.
Use this for your target variables and organisational features.

**Table 2 — Staging view (for feature engineering):** nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_staging.stg_nhs_rtt\_\_full_extract

1,092,767 rows. All pathway types. All 104 individual week-band
columns available. Use this when you need the full waiting time
distribution as features.

**Important:** When using the staging table, always filter:

```sql
WHERE rtt_part_type = 'Part_2'
```

to get Incomplete Pathways only — the equivalent of the mart table.

---

## What this dataset enables

| Task                                  | Feasibility           | Notes                                 |
| ------------------------------------- | --------------------- | ------------------------------------- |
| Waiting list size forecasting         | Possible with caveats | Only 6 months — limited seasonality   |
| 18-week breach prediction             | Good candidate        | Clear binary target, rich features    |
| Trust performance clustering          | Strong candidate      | 517 trusts × 24 specialties           |
| Anomaly detection                     | Good candidate        | Identify unusual trust patterns       |
| Waiting time distribution modelling   | Strong candidate      | 104 week-band columns available       |
| Individual patient outcome prediction | Not possible          | Data is aggregated, not patient-level |

---

## Target variables

| Variable                          | Type                | Table | Use case                                  |
| --------------------------------- | ------------------- | ----- | ----------------------------------------- |
| `pct_within_18_weeks`             | Continuous 0-100    | Mart  | Regression — predict performance %        |
| `meets_18_week_standard`          | Boolean             | Mart  | Binary classification — breach prediction |
| `rag_status`                      | 3-class categorical | Mart  | Multi-class classification                |
| `total_incomplete`                | Integer             | Mart  | Time series — waiting list forecasting    |
| `patients_waiting_over_104_weeks` | Integer             | Mart  | Regression — very long waiter prediction  |

---

## Feature inventory

### From `fact_rtt_performance` (mart)

**Temporal features:**

- `period_date` — monthly timestamp — use as time index
- `calendar_month` — integer 1-12 — seasonality signal
- `financial_year` — string — NHS year context

**Organisational features:**

- `provider_org_code` — 517 unique trusts — encode as category
- `provider_parent_org_code` — 42 ICBs — encode as category
- `is_surgical` — boolean — specialty type signal
- `specialty_group` — 4 categories — Surgical/Medical/Diagnostic/Other
- `treatment_function_code` — 23 specialties — encode as category

**Volume features:**

- `total_incomplete` — total waiting list size
- `patients_within_18_weeks` — within standard count
- `patients_beyond_18_weeks` — breach count
- `patients_waiting_52_plus_weeks` — long waiter count
- `patients_waiting_over_104_weeks` — very long waiter count

### From `stg_nhs_rtt__full_extract` (staging)

**Week-band distribution features (104 columns):**

- `wks_00_to_01` through `wks_17_to_18` — within standard bands
- `wks_18_to_19` through `wks_over_104` — beyond standard bands

These columns give you the full waiting time distribution.
Useful for distribution-based features such as:

- Median wait weeks (estimated from band midpoints)
- Proportion waiting in each quartile
- Gini coefficient of wait distribution

---

## Recommended modelling approaches

### 1. Breach prediction (Classification)

**Problem:** Will this trust-specialty breach the 92%
standard next month?

**Target:** `meets_18_week_standard` (Boolean, next month T+1)

**Features to engineer:**

```python
# Rolling 3-month average performance
df['rolling_3m_pct'] = df.groupby(
    ['provider_org_code', 'treatment_function_code']
)['pct_within_18_weeks'].transform(
    lambda x: x.shift(1).rolling(3).mean()
)

# Month-on-month waiting list change
df['mom_list_change'] = df.groupby(
    ['provider_org_code', 'treatment_function_code']
)['total_incomplete'].diff()

# Long waiter ratio
df['long_waiter_ratio'] = (
    df['patients_waiting_52_plus_weeks']
    / df['total_incomplete']
)
```

**Recommended model:** XGBoost or LightGBM

**Evaluation:** ROC-AUC, precision-recall curve
Note: Most trusts are currently failing (Red/Amber) so
the positive class (Green = meeting standard) is minority.
Use class weights or SMOTE to handle imbalance.

---

### 2. Waiting list forecasting (Time series)

**Problem:** How many patients will be waiting
at trust X in specialty Y next month?

**Target:** `total_incomplete` (T+1)

**Recommended approach:**

- Per trust-specialty time series using Prophet
- National-level using SARIMA or LSTM

**Limitation:** Only 6 months of data is insufficient
for robust seasonal modelling. Minimum 24 months recommended.
This will improve as monthly data loads continue.

```python
from prophet import Prophet

# Example for one trust-specialty combination
trust_specialty_df = df[
    (df['provider_org_code'] == 'RRK') &
    (df['treatment_function_code'] == 'C_110')
][['period_date', 'total_incomplete']].rename(
    columns={'period_date': 'ds', 'total_incomplete': 'y'}
)

model = Prophet(yearly_seasonality=False, weekly_seasonality=False)
model.fit(trust_specialty_df)
future = model.make_future_dataframe(periods=3, freq='MS')
forecast = model.predict(future)
```

---

### 3. Trust performance clustering

**Problem:** Which trusts are similar in their
performance patterns? Are there distinct performance archetypes?

**Features for clustering:**

```python
cluster_features = [
    'pct_within_18_weeks',      # Performance level
    'total_incomplete',          # List size
    'long_waiter_ratio',         # Proportion of long waiters
    'is_surgical'                # Specialty mix
]
```

**Recommended approach:** K-means with k=4 or 5

- Normalise all features before clustering
- Visualise with PCA (2 components)
- Validate with silhouette score

**Expected archetypes:**

- High performers: >85% within 18 weeks, small lists
- Large struggling trusts: big lists, moderate performance
- Specialist trusts: small lists, variable performance
- Persistently failing: <70% across all specialties

---

### 4. Waiting time distribution modelling

**Use the staging table for this.**

The 104 week-band columns give you the full distribution of
waiting times for each trust-specialty-month combination.

```python
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="nhs-rtt-pipeline-oo-2026")

# Get week-band columns for distribution analysis
query = """
    SELECT
        period_date,
        provider_org_code,
        treatment_function_code,
        wks_00_to_01, wks_01_to_02, wks_02_to_03,
        -- ... all 104 week-band columns ...
        wks_over_104,
        total_all
    FROM
        `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_staging.stg_nhs_rtt__full_extract`
    WHERE rtt_part_type = 'Part_2'
    ORDER BY period_date, provider_org_code, treatment_function_code
"""

df = client.query(query).to_dataframe()
```

---

## Getting the data into Python

```python
import os
from google.cloud import bigquery
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'path/to/keyfile.json'
client = bigquery.Client(project='nhs-rtt-pipeline-oo-2026')

# Load mart table (recommended starting point)
query = """
    SELECT *
    FROM
        `nhs-rtt-pipeline-oo-2026.nhs_rtt_dbt_marts.fact_rtt_performance`
    ORDER BY
        period_date,
        provider_org_code,
        treatment_function_code
"""

df = client.query(query).to_dataframe()
df['period_date'] = pd.to_datetime(df['period_date'])

print(f"Loaded {len(df):,} rows x {len(df.columns)} columns")
print(f"Date range: {df['period_date'].min()} to {df['period_date'].max()}")
print(f"Unique trusts: {df['provider_org_code'].nunique()}")
print(f"Unique specialties: {df['treatment_function_code'].nunique()}")
```

---

## Critical caveats for modelling

**1. Only 6 months of data**
Insufficient for robust seasonal modelling.
Do not make strong claims about seasonality.
Data grows monthly — revisit models as more months load.

**2. Revised data (Aug25 and Sep25)**
NHS England revised these months on 12th February 2026.
The `_is_revised` flag in the staging table identifies them.
Month-on-month changes involving these months may reflect
revisions rather than real trends. Consider:

- Excluding Aug25-Sep25 from training data, or
- Including `_is_revised` as a feature

**3. Missing trust submissions**
A missing row means no submission — not zero patients.
Do not impute missing rows as zeros.
Use `provider_org_code` counts per month to identify
trusts with incomplete submission histories.

**4. Aggregated data only**
Trust-specialty-month level only.
No individual patient records. No demographic data.
No clinical outcome data. No staffing or capacity data.
All models are ecological — relationships at aggregate level
may not hold at individual patient level.

**5. Confounding factors not in dataset**
Performance is affected by staffing levels, capital investment,
local deprivation, surgical capacity, and winter pressures.
Without these variables, models will have limited explanatory
power. Treat outputs as hypotheses, not causal explanations.

---

## Contact and collaboration

For pipeline questions or additional data requests:
raise an issue at:
https://github.com/insightful-algorithms/nhs-rtt-cwt-elt-pipeline

This dataset is the foundation for a separate Data Analyst
portfolio project which uses it for production analytical work.
Collaboration between the engineering and analytical portfolios
is explicitly designed into the project architecture.
