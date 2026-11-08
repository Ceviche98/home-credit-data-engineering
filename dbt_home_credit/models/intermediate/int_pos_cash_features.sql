{{ config(materialized='view') }}

SELECT
    SK_ID_CURR,
    COUNT(SK_ID_PREV) AS pos_cash_records_count,
    COUNT(DISTINCT SK_ID_PREV) AS pos_unique_contracts_count,
    COUNT(MONTHS_BALANCE) AS pos_months_balance_size,
    MIN(MONTHS_BALANCE) AS pos_months_balance_min,
    MAX(MONTHS_BALANCE) AS pos_months_balance_max,
    MAX(SK_DPD) AS pos_sk_dpd_max,
    AVG(SK_DPD) AS pos_sk_dpd_avg,
    MAX(SK_DPD_DEF) AS pos_sk_dpd_def_max,
    AVG(SK_DPD_DEF) AS pos_sk_dpd_def_avg,
    SUM(CASE WHEN SK_DPD > 0 THEN 1 ELSE 0 END) AS pos_dpd_nonzero_count,
    SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Active' THEN 1 ELSE 0 END) AS pos_active_status_count,
    SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Completed' THEN 1 ELSE 0 END) AS pos_completed_status_count,
    ROUND(CAST(SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Completed' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(DISTINCT SK_ID_PREV), 0), 4) AS pos_completed_ratio,
    AVG(CNT_INSTALMENT_FUTURE / NULLIF(CNT_INSTALMENT, 0)) AS pos_future_installment_ratio_avg
FROM {{ ref('stg_pos_cash') }}
GROUP BY SK_ID_CURR
