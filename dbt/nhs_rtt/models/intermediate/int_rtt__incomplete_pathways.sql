-- ─────────────────────────────────────────────────────────────────────────────
-- Intermediate: RTT Incomplete Pathways
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Filter to incomplete pathways (Part_2) and calculate
--          18-week performance metrics for each trust-specialty combination.
--
-- Key business logic:
--   - Constitutional standard: 92% of incomplete pathways within 18 weeks
--   - Within 18 weeks: sum of wks_00_to_01 through wks_17_to_18
--   - Beyond 18 weeks: sum of wks_18_to_19 through wks_over_104
--   - Performance: within_18_weeks / total_all * 100
--
-- Why filter here and not in staging?
--   Staging is source-faithful — it reflects the full raw dataset.
--   Business filtering belongs in intermediate so it is explicit,
--   documented, and testable as a separate step.
--
-- Why calculate metrics here and not in marts?
--   Marts should be thin — they join and present, not calculate.
--   Heavy arithmetic lives in intermediate where it can be tested
--   independently of the mart's join logic.
--
-- Materialisation: table (unpivot is expensive — compute once)
-- ─────────────────────────────────────────────────────────────────────────────

with

incomplete_pathways as (

    select *
    from {{ ref('stg_nhs_rtt__full_extract') }}
    where rtt_part_type = 'Part_2'  -- Incomplete Pathways only
      and treatment_function_code != 'C_999'  -- Exclude summary/total rows

),

with_metrics as (

    select

        -- ── Dimension columns ─────────────────────────────────────────────────
        period_date,
        financial_year,
        provider_parent_org_code,
        provider_parent_name,
        provider_org_code,
        provider_org_name,
        commissioner_parent_org_code,
        commissioner_parent_name,
        commissioner_org_code,
        commissioner_org_name,
        treatment_function_code,
        treatment_function_name,

        -- ── Total patients ────────────────────────────────────────────────────
        total_all                                   as total_incomplete,

        -- ── Within 18 weeks (constitutional standard threshold) ───────────────
        -- Sum of all week bands from 0 to 18 weeks
        (
            wks_00_to_01 + wks_01_to_02 + wks_02_to_03 +
            wks_03_to_04 + wks_04_to_05 + wks_05_to_06 +
            wks_06_to_07 + wks_07_to_08 + wks_08_to_09 +
            wks_09_to_10 + wks_10_to_11 + wks_11_to_12 +
            wks_12_to_13 + wks_13_to_14 + wks_14_to_15 +
            wks_15_to_16 + wks_16_to_17 + wks_17_to_18
        )                                           as patients_within_18_weeks,

        -- ── Beyond 18 weeks (constitutional standard breach) ─────────────────
        (
            wks_18_to_19 + wks_19_to_20 + wks_20_to_21 +
            wks_21_to_22 + wks_22_to_23 + wks_23_to_24 +
            wks_24_to_25 + wks_25_to_26 + wks_26_to_27 +
            wks_27_to_28 + wks_28_to_29 + wks_29_to_30 +
            wks_30_to_31 + wks_31_to_32 + wks_32_to_33 +
            wks_33_to_34 + wks_34_to_35 + wks_35_to_36 +
            wks_36_to_37 + wks_37_to_38 + wks_38_to_39 +
            wks_39_to_40 + wks_40_to_41 + wks_41_to_42 +
            wks_42_to_43 + wks_43_to_44 + wks_44_to_45 +
            wks_45_to_46 + wks_46_to_47 + wks_47_to_48 +
            wks_48_to_49 + wks_49_to_50 + wks_50_to_51 +
            wks_51_to_52 + wks_52_to_53 + wks_53_to_54 +
            wks_54_to_55 + wks_55_to_56 + wks_56_to_57 +
            wks_57_to_58 + wks_58_to_59 + wks_59_to_60 +
            wks_60_to_61 + wks_61_to_62 + wks_62_to_63 +
            wks_63_to_64 + wks_64_to_65 + wks_65_to_66 +
            wks_66_to_67 + wks_67_to_68 + wks_68_to_69 +
            wks_69_to_70 + wks_70_to_71 + wks_71_to_72 +
            wks_72_to_73 + wks_73_to_74 + wks_74_to_75 +
            wks_75_to_76 + wks_76_to_77 + wks_77_to_78 +
            wks_78_to_79 + wks_79_to_80 + wks_80_to_81 +
            wks_81_to_82 + wks_82_to_83 + wks_83_to_84 +
            wks_84_to_85 + wks_85_to_86 + wks_86_to_87 +
            wks_87_to_88 + wks_88_to_89 + wks_89_to_90 +
            wks_90_to_91 + wks_91_to_92 + wks_92_to_93 +
            wks_93_to_94 + wks_94_to_95 + wks_95_to_96 +
            wks_96_to_97 + wks_97_to_98 + wks_98_to_99 +
            wks_99_to_100 + wks_100_to_101 + wks_101_to_102 +
            wks_102_to_103 + wks_103_to_104 + wks_over_104
        )                                           as patients_beyond_18_weeks,

        -- ── Very long waiters (52+ weeks) ─────────────────────────────────────
        -- Politically significant — reported separately to Parliament
        (
            wks_52_to_53 + wks_53_to_54 + wks_54_to_55 +
            wks_55_to_56 + wks_56_to_57 + wks_57_to_58 +
            wks_58_to_59 + wks_59_to_60 + wks_60_to_61 +
            wks_61_to_62 + wks_62_to_63 + wks_63_to_64 +
            wks_64_to_65 + wks_65_to_66 + wks_66_to_67 +
            wks_67_to_68 + wks_68_to_69 + wks_69_to_70 +
            wks_70_to_71 + wks_71_to_72 + wks_72_to_73 +
            wks_73_to_74 + wks_74_to_75 + wks_75_to_76 +
            wks_76_to_77 + wks_77_to_78 + wks_78_to_79 +
            wks_79_to_80 + wks_80_to_81 + wks_81_to_82 +
            wks_82_to_83 + wks_83_to_84 + wks_84_to_85 +
            wks_85_to_86 + wks_86_to_87 + wks_87_to_88 +
            wks_88_to_89 + wks_89_to_90 + wks_90_to_91 +
            wks_91_to_92 + wks_92_to_93 + wks_93_to_94 +
            wks_94_to_95 + wks_95_to_96 + wks_96_to_97 +
            wks_97_to_98 + wks_98_to_99 + wks_99_to_100 +
            wks_100_to_101 + wks_101_to_102 + wks_102_to_103 +
            wks_103_to_104 + wks_over_104
        )                                           as patients_waiting_52_plus_weeks,

        -- ── Over 104 weeks (2+ years — most severe breach) ───────────────────
        wks_over_104                                as patients_waiting_over_104_weeks,

        -- ── 18-week performance percentage ───────────────────────────────────
        -- Safe division: avoid divide-by-zero when total_all = 0
        -- Result is percentage (0-100), rounded to 1 decimal place
        case
            when total_all = 0 then null
            else round(
                safe_divide(
                    (
                        wks_00_to_01 + wks_01_to_02 + wks_02_to_03 +
                        wks_03_to_04 + wks_04_to_05 + wks_05_to_06 +
                        wks_06_to_07 + wks_07_to_08 + wks_08_to_09 +
                        wks_09_to_10 + wks_10_to_11 + wks_11_to_12 +
                        wks_12_to_13 + wks_13_to_14 + wks_14_to_15 +
                        wks_15_to_16 + wks_16_to_17 + wks_17_to_18
                    ) * 100.0,
                    total_all
                ),
                1
            )
        end                                         as pct_within_18_weeks,

        -- ── Constitutional standard flag ──────────────────────────────────────
        -- True if trust-specialty meets the 92% standard
        case
            when total_all = 0 then null
            when safe_divide(
                (
                    wks_00_to_01 + wks_01_to_02 + wks_02_to_03 +
                    wks_03_to_04 + wks_04_to_05 + wks_05_to_06 +
                    wks_06_to_07 + wks_07_to_08 + wks_08_to_09 +
                    wks_09_to_10 + wks_10_to_11 + wks_11_to_12 +
                    wks_12_to_13 + wks_13_to_14 + wks_14_to_15 +
                    wks_15_to_16 + wks_16_to_17 + wks_17_to_18
                ) * 100.0,
                total_all
            ) >= 92.0 then true
            else false
        end                                         as meets_18_week_standard,

        -- ── Audit ─────────────────────────────────────────────────────────────
        _loaded_at,
        _source

    from incomplete_pathways

)

select * from with_metrics