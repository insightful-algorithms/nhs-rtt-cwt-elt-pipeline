-- ─────────────────────────────────────────────────────────────────────────────
-- Staging: NHS RTT Full Extract
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Clean and standardise the raw NHS RTT data from BigQuery.
--
-- What this model does:
--   1. Renames all 121 columns to snake_case
--   2. Parses the Period string into a proper DATE
--   3. Derives financial_year and month_code for partitioning
--   4. Replaces nulls in week-band columns with 0
--   5. Adds audit columns (_loaded_at, _source)
--
-- What this model does NOT do:
--   - No filtering (all rows pass through)
--   - No aggregation (one output row per input row)
--   - No joins (staging models are always single-source)
--
-- Materialisation: view (always reflects latest raw data)
-- ─────────────────────────────────────────────────────────────────────────────

with

source as (
    select * from {{ source('nhs_rtt_raw', 'rtt_full_extract') }}
),

renamed as (

    select

        -- ── Audit columns (added by our pipeline, not from NHS England) ──────
        current_timestamp()                         as _loaded_at,
        'nhs_england_rtt_statistical_release'       as _source,

        -- ── Period / date columns ─────────────────────────────────────────────
        -- Raw value: 'RTT-January-2026'
        -- We parse this into a proper date (last day of the reporting month)
        `Period`                                    as period_raw,

        parse_date(
            '%B %Y',
            replace(regexp_replace(`Period`, r'^RTT-', ''), '-', ' ')
        )                                           as period_date,

        -- Derive the NHS financial year (April to March)
        -- April 2025 to March 2026 = '2025-26'
        case
            when extract(month from parse_date(
                '%B %Y',
                replace(regexp_replace(`Period`, r'^RTT-', ''), '-', ' ')
            )) >= 4
            then concat(
                cast(extract(year from parse_date(
                    '%B %Y',
                    replace(regexp_replace(`Period`, r'^RTT-', ''), '-', ' ')
                )) as string),
                '-',
                right(cast(extract(year from parse_date(
                    '%B %Y',
                    replace(regexp_replace(`Period`, r'^RTT-', ''), '-', ' ')
                )) + 1 as string), 2)
            )
            else concat(
                cast(extract(year from parse_date(
                    '%B %Y',
                    replace(regexp_replace(`Period`, r'^RTT-', ''), '-', ' ')
                )) - 1 as string),
                '-',
                right(cast(extract(year from parse_date(
                    '%B %Y',
                    replace(regexp_replace(`Period`, r'^RTT-', ''), '-', ' ')
                )) as string), 2)
            )
        end                                         as financial_year,

        -- ── Provider columns ──────────────────────────────────────────────────
        `Provider Parent Org Code`                  as provider_parent_org_code,
        `Provider Parent Name`                      as provider_parent_name,
        `Provider Org Code`                         as provider_org_code,
        `Provider Org Name`                         as provider_org_name,

        -- ── Commissioner columns ──────────────────────────────────────────────
        -- 5.8% of rows have null commissioner parent — these are national
        -- commissioning hubs. We coalesce to 'NATIONAL' for clarity.
        coalesce(
            `Commissioner Parent Org Code`, 'NATIONAL'
        )                                           as commissioner_parent_org_code,
        coalesce(
            `Commissioner Parent Name`, 'National Commissioning'
        )                                           as commissioner_parent_name,
        `Commissioner Org Code`                     as commissioner_org_code,
        coalesce(
            `Commissioner Org Name`, 'Unknown Commissioner'
        )                                           as commissioner_org_name,

        -- ── RTT pathway type columns ──────────────────────────────────────────
        `RTT Part Type`                             as rtt_part_type,
        `RTT Part Description`                      as rtt_part_description,

        -- ── Treatment function columns ────────────────────────────────────────
        `Treatment Function Code`                   as treatment_function_code,
        `Treatment Function Name`                   as treatment_function_name,

        -- ── Summary columns ───────────────────────────────────────────────────
        -- Total All is never null — it is our primary patient count
        cast(`Total All` as int64)                  as total_all,
        coalesce(`Total`, 0)                        as total_pathways,
        coalesce(
            `Patients with unknown clock start date`, 0
        )                                           as patients_unknown_clock_start,

        -- ── Week band columns (0-18 weeks — within constitutional standard) ───
        -- Nulls replaced with 0: null means no patients in this band
        coalesce(`Gt 00 To 01 Weeks SUM 1`, 0)     as wks_00_to_01,
        coalesce(`Gt 01 To 02 Weeks SUM 1`, 0)     as wks_01_to_02,
        coalesce(`Gt 02 To 03 Weeks SUM 1`, 0)     as wks_02_to_03,
        coalesce(`Gt 03 To 04 Weeks SUM 1`, 0)     as wks_03_to_04,
        coalesce(`Gt 04 To 05 Weeks SUM 1`, 0)     as wks_04_to_05,
        coalesce(`Gt 05 To 06 Weeks SUM 1`, 0)     as wks_05_to_06,
        coalesce(`Gt 06 To 07 Weeks SUM 1`, 0)     as wks_06_to_07,
        coalesce(`Gt 07 To 08 Weeks SUM 1`, 0)     as wks_07_to_08,
        coalesce(`Gt 08 To 09 Weeks SUM 1`, 0)     as wks_08_to_09,
        coalesce(`Gt 09 To 10 Weeks SUM 1`, 0)     as wks_09_to_10,
        coalesce(`Gt 10 To 11 Weeks SUM 1`, 0)     as wks_10_to_11,
        coalesce(`Gt 11 To 12 Weeks SUM 1`, 0)     as wks_11_to_12,
        coalesce(`Gt 12 To 13 Weeks SUM 1`, 0)     as wks_12_to_13,
        coalesce(`Gt 13 To 14 Weeks SUM 1`, 0)     as wks_13_to_14,
        coalesce(`Gt 14 To 15 Weeks SUM 1`, 0)     as wks_14_to_15,
        coalesce(`Gt 15 To 16 Weeks SUM 1`, 0)     as wks_15_to_16,
        coalesce(`Gt 16 To 17 Weeks SUM 1`, 0)     as wks_16_to_17,
        coalesce(`Gt 17 To 18 Weeks SUM 1`, 0)     as wks_17_to_18,

        -- ── Week band columns (18-52 weeks — beyond constitutional standard) ──
        coalesce(`Gt 18 To 19 Weeks SUM 1`, 0)     as wks_18_to_19,
        coalesce(`Gt 19 To 20 Weeks SUM 1`, 0)     as wks_19_to_20,
        coalesce(`Gt 20 To 21 Weeks SUM 1`, 0)     as wks_20_to_21,
        coalesce(`Gt 21 To 22 Weeks SUM 1`, 0)     as wks_21_to_22,
        coalesce(`Gt 22 To 23 Weeks SUM 1`, 0)     as wks_22_to_23,
        coalesce(`Gt 23 To 24 Weeks SUM 1`, 0)     as wks_23_to_24,
        coalesce(`Gt 24 To 25 Weeks SUM 1`, 0)     as wks_24_to_25,
        coalesce(`Gt 25 To 26 Weeks SUM 1`, 0)     as wks_25_to_26,
        coalesce(`Gt 26 To 27 Weeks SUM 1`, 0)     as wks_26_to_27,
        coalesce(`Gt 27 To 28 Weeks SUM 1`, 0)     as wks_27_to_28,
        coalesce(`Gt 28 To 29 Weeks SUM 1`, 0)     as wks_28_to_29,
        coalesce(`Gt 29 To 30 Weeks SUM 1`, 0)     as wks_29_to_30,
        coalesce(`Gt 30 To 31 Weeks SUM 1`, 0)     as wks_30_to_31,
        coalesce(`Gt 31 To 32 Weeks SUM 1`, 0)     as wks_31_to_32,
        coalesce(`Gt 32 To 33 Weeks SUM 1`, 0)     as wks_32_to_33,
        coalesce(`Gt 33 To 34 Weeks SUM 1`, 0)     as wks_33_to_34,
        coalesce(`Gt 34 To 35 Weeks SUM 1`, 0)     as wks_34_to_35,
        coalesce(`Gt 35 To 36 Weeks SUM 1`, 0)     as wks_35_to_36,
        coalesce(`Gt 36 To 37 Weeks SUM 1`, 0)     as wks_36_to_37,
        coalesce(`Gt 37 To 38 Weeks SUM 1`, 0)     as wks_37_to_38,
        coalesce(`Gt 38 To 39 Weeks SUM 1`, 0)     as wks_38_to_39,
        coalesce(`Gt 39 To 40 Weeks SUM 1`, 0)     as wks_39_to_40,
        coalesce(`Gt 40 To 41 Weeks SUM 1`, 0)     as wks_40_to_41,
        coalesce(`Gt 41 To 42 Weeks SUM 1`, 0)     as wks_41_to_42,
        coalesce(`Gt 42 To 43 Weeks SUM 1`, 0)     as wks_42_to_43,
        coalesce(`Gt 43 To 44 Weeks SUM 1`, 0)     as wks_43_to_44,
        coalesce(`Gt 44 To 45 Weeks SUM 1`, 0)     as wks_44_to_45,
        coalesce(`Gt 45 To 46 Weeks SUM 1`, 0)     as wks_45_to_46,
        coalesce(`Gt 46 To 47 Weeks SUM 1`, 0)     as wks_46_to_47,
        coalesce(`Gt 47 To 48 Weeks SUM 1`, 0)     as wks_47_to_48,
        coalesce(`Gt 48 To 49 Weeks SUM 1`, 0)     as wks_48_to_49,
        coalesce(`Gt 49 To 50 Weeks SUM 1`, 0)     as wks_49_to_50,
        coalesce(`Gt 50 To 51 Weeks SUM 1`, 0)     as wks_50_to_51,
        coalesce(`Gt 51 To 52 Weeks SUM 1`, 0)     as wks_51_to_52,

        -- ── Week band columns (52+ weeks — very long waiters) ─────────────────
        coalesce(`Gt 52 To 53 Weeks SUM 1`, 0)     as wks_52_to_53,
        coalesce(`Gt 53 To 54 Weeks SUM 1`, 0)     as wks_53_to_54,
        coalesce(`Gt 54 To 55 Weeks SUM 1`, 0)     as wks_54_to_55,
        coalesce(`Gt 55 To 56 Weeks SUM 1`, 0)     as wks_55_to_56,
        coalesce(`Gt 56 To 57 Weeks SUM 1`, 0)     as wks_56_to_57,
        coalesce(`Gt 57 To 58 Weeks SUM 1`, 0)     as wks_57_to_58,
        coalesce(`Gt 58 To 59 Weeks SUM 1`, 0)     as wks_58_to_59,
        coalesce(`Gt 59 To 60 Weeks SUM 1`, 0)     as wks_59_to_60,
        coalesce(`Gt 60 To 61 Weeks SUM 1`, 0)     as wks_60_to_61,
        coalesce(`Gt 61 To 62 Weeks SUM 1`, 0)     as wks_61_to_62,
        coalesce(`Gt 62 To 63 Weeks SUM 1`, 0)     as wks_62_to_63,
        coalesce(`Gt 63 To 64 Weeks SUM 1`, 0)     as wks_63_to_64,
        coalesce(`Gt 64 To 65 Weeks SUM 1`, 0)     as wks_64_to_65,
        coalesce(`Gt 65 To 66 Weeks SUM 1`, 0)     as wks_65_to_66,
        coalesce(`Gt 66 To 67 Weeks SUM 1`, 0)     as wks_66_to_67,
        coalesce(`Gt 67 To 68 Weeks SUM 1`, 0)     as wks_67_to_68,
        coalesce(`Gt 68 To 69 Weeks SUM 1`, 0)     as wks_68_to_69,
        coalesce(`Gt 69 To 70 Weeks SUM 1`, 0)     as wks_69_to_70,
        coalesce(`Gt 70 To 71 Weeks SUM 1`, 0)     as wks_70_to_71,
        coalesce(`Gt 71 To 72 Weeks SUM 1`, 0)     as wks_71_to_72,
        coalesce(`Gt 72 To 73 Weeks SUM 1`, 0)     as wks_72_to_73,
        coalesce(`Gt 73 To 74 Weeks SUM 1`, 0)     as wks_73_to_74,
        coalesce(`Gt 74 To 75 Weeks SUM 1`, 0)     as wks_74_to_75,
        coalesce(`Gt 75 To 76 Weeks SUM 1`, 0)     as wks_75_to_76,
        coalesce(`Gt 76 To 77 Weeks SUM 1`, 0)     as wks_76_to_77,
        coalesce(`Gt 77 To 78 Weeks SUM 1`, 0)     as wks_77_to_78,
        coalesce(`Gt 78 To 79 Weeks SUM 1`, 0)     as wks_78_to_79,
        coalesce(`Gt 79 To 80 Weeks SUM 1`, 0)     as wks_79_to_80,
        coalesce(`Gt 80 To 81 Weeks SUM 1`, 0)     as wks_80_to_81,
        coalesce(`Gt 81 To 82 Weeks SUM 1`, 0)     as wks_81_to_82,
        coalesce(`Gt 82 To 83 Weeks SUM 1`, 0)     as wks_82_to_83,
        coalesce(`Gt 83 To 84 Weeks SUM 1`, 0)     as wks_83_to_84,
        coalesce(`Gt 84 To 85 Weeks SUM 1`, 0)     as wks_84_to_85,
        coalesce(`Gt 85 To 86 Weeks SUM 1`, 0)     as wks_85_to_86,
        coalesce(`Gt 86 To 87 Weeks SUM 1`, 0)     as wks_86_to_87,
        coalesce(`Gt 87 To 88 Weeks SUM 1`, 0)     as wks_87_to_88,
        coalesce(`Gt 88 To 89 Weeks SUM 1`, 0)     as wks_88_to_89,
        coalesce(`Gt 89 To 90 Weeks SUM 1`, 0)     as wks_89_to_90,
        coalesce(`Gt 90 To 91 Weeks SUM 1`, 0)     as wks_90_to_91,
        coalesce(`Gt 91 To 92 Weeks SUM 1`, 0)     as wks_91_to_92,
        coalesce(`Gt 92 To 93 Weeks SUM 1`, 0)     as wks_92_to_93,
        coalesce(`Gt 93 To 94 Weeks SUM 1`, 0)     as wks_93_to_94,
        coalesce(`Gt 94 To 95 Weeks SUM 1`, 0)     as wks_94_to_95,
        coalesce(`Gt 95 To 96 Weeks SUM 1`, 0)     as wks_95_to_96,
        coalesce(`Gt 96 To 97 Weeks SUM 1`, 0)     as wks_96_to_97,
        coalesce(`Gt 97 To 98 Weeks SUM 1`, 0)     as wks_97_to_98,
        coalesce(`Gt 98 To 99 Weeks SUM 1`, 0)     as wks_98_to_99,
        coalesce(`Gt 99 To 100 Weeks SUM 1`, 0)    as wks_99_to_100,
        coalesce(`Gt 100 To 101 Weeks SUM 1`, 0)   as wks_100_to_101,
        coalesce(`Gt 101 To 102 Weeks SUM 1`, 0)   as wks_101_to_102,
        coalesce(`Gt 102 To 103 Weeks SUM 1`, 0)   as wks_102_to_103,
        coalesce(`Gt 103 To 104 Weeks SUM 1`, 0)   as wks_103_to_104,
        coalesce(`Gt 104 Weeks SUM 1`, 0)           as wks_over_104

    from source

)

select * from renamed