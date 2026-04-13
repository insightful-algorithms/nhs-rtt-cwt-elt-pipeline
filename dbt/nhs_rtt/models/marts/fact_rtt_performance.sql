-- ─────────────────────────────────────────────────────────────────────────────
-- Mart: RTT Performance Fact Table
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Analyst-ready fact table for NHS RTT 18-week performance.
--          Joins incomplete pathway metrics to treatment function
--          reference data. One row per trust-specialty per month.
--
-- Primary consumers:
--   - Looker Studio RTT performance dashboard
--   - NHS England board reporting
--   - ICB performance monitoring teams
--
-- Grain: One row per provider_org_code + treatment_function_code + period_date
--
-- Materialisation: table, partitioned by period_date, clustered by
--                  provider_parent_org_code for efficient trust-level queries
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'period_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by = ['provider_parent_org_code', 'treatment_function_code'],
    labels = {
      'domain': 'nhs_rtt',
      'sensitivity': 'official',
      'refresh': 'monthly'
    }
  )
}}

with

incomplete_pathways as (
    select * from {{ ref('int_rtt__incomplete_pathways') }}
),

treatment_functions as (
    select * from {{ ref('treatment_functions') }}
),

final as (

    select

        -- ── Surrogate key ─────────────────────────────────────────────────────
        -- Uniquely identifies each row in this fact table
        {{ dbt_utils.generate_surrogate_key([
            'ip.period_date',
            'ip.provider_org_code',
            'ip.commissioner_org_code',
            'ip.treatment_function_code'
        ]) }}                                       as rtt_performance_id,

        -- ── Date dimension ────────────────────────────────────────────────────
        ip.period_date,
        ip.financial_year,
        extract(year from ip.period_date)           as calendar_year,
        extract(month from ip.period_date)          as calendar_month,
        format_date('%B', ip.period_date)           as month_name,
        format_date('%b %Y', ip.period_date)        as month_label,

        -- ── Provider dimension ────────────────────────────────────────────────
        ip.provider_parent_org_code,
        ip.provider_parent_name,
        ip.provider_org_code,
        ip.provider_org_name,

        -- ── Commissioner dimension ────────────────────────────────────────────
        ip.commissioner_parent_org_code,
        ip.commissioner_parent_name,
        ip.commissioner_org_code,
        ip.commissioner_org_name,

        -- ── Treatment function dimension ──────────────────────────────────────
        ip.treatment_function_code,
        ip.treatment_function_name,
        tf.specialty_group,
        tf.is_surgical,

        -- ── Patient count metrics ─────────────────────────────────────────────
        ip.total_incomplete,
        ip.patients_within_18_weeks,
        ip.patients_beyond_18_weeks,
        ip.patients_waiting_52_plus_weeks,
        ip.patients_waiting_over_104_weeks,

        -- ── Performance metrics ───────────────────────────────────────────────
        ip.pct_within_18_weeks,
        ip.meets_18_week_standard,

        -- ── Performance banding ───────────────────────────────────────────────
        -- Used for traffic-light RAG status in dashboards
        case
            when ip.pct_within_18_weeks is null     then 'Unknown'
            when ip.pct_within_18_weeks >= 92.0     then 'Green'
            when ip.pct_within_18_weeks >= 85.0     then 'Amber'
            else                                         'Red'
        end                                         as rag_status,

        -- ── Breach severity ───────────────────────────────────────────────────
        case
            when ip.patients_waiting_over_104_weeks > 0  then 'Over 2 Years'
            when ip.patients_waiting_52_plus_weeks > 0   then 'Over 1 Year'
            when ip.patients_beyond_18_weeks > 0         then 'Over 18 Weeks'
            else                                              'Within Standard'
        end                                         as breach_severity,

        -- ── Audit ─────────────────────────────────────────────────────────────
        ip._loaded_at,
        ip._source

    from incomplete_pathways ip

    left join treatment_functions tf
        on ip.treatment_function_code = tf.treatment_function_code

)

select * from final