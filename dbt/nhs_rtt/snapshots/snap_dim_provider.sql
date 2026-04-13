-- ─────────────────────────────────────────────────────────────────────────────
-- Snapshot: Provider Dimension (SCD Type 2)
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Track changes to NHS trust names and ICB assignments over time.
--
-- Why SCD Type 2?
-- NHS trusts change names, merge, and move between ICBs over time.
-- Without SCD Type 2, historical performance data would be
-- retroactively attributed to the wrong trust name.
--
-- SCD Type 2 adds:
--   dbt_scd_id      — surrogate key for each version
--   dbt_valid_from  — when this version of the record became active
--   dbt_valid_to    — when this version expired (null = currently active)
--   dbt_is_current  — boolean flag for the current version
--
-- Example: If Barnet Hospital Trust renamed itself in March 2025,
-- records before March 2025 show the old name, after show the new name.
-- ─────────────────────────────────────────────────────────────────────────────

{% snapshot snap_dim_provider %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'provider_org_code',
        strategy = 'check',
        check_cols = [
            'provider_org_name',
            'provider_parent_org_code',
            'provider_parent_name'
        ],
        invalidate_hard_deletes = True
    )
}}

select distinct
    provider_org_code,
    provider_org_name,
    provider_parent_org_code,
    provider_parent_name,
    current_timestamp()     as snapshot_taken_at

from {{ ref('stg_nhs_rtt__full_extract') }}

where provider_org_code is not null

{% endsnapshot %}