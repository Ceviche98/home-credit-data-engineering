{{ config(materialized='view') }}

SELECT
    SK_ID_CURR,
    COUNT(SK_ID_PREV) AS total_installment_checks,
    COUNT(DISTINCT SK_ID_PREV) AS installments_unique_contracts,
    SUM(CASE WHEN DAYS_LATE > 0 THEN 1 ELSE 0 END) AS count_late_installments,
    SUM(CASE WHEN DAYS_LATE > 30 THEN 1 ELSE 0 END) AS count_late_30d_installments,
    SUM(CASE WHEN DAYS_LATE > 60 THEN 1 ELSE 0 END) AS count_late_60d_installments,
    MAX(DAYS_LATE) AS max_days_late,
    AVG(DAYS_LATE) AS avg_days_late,
    AVG(CASE WHEN DAYS_LATE < 0 THEN ABS(DAYS_LATE) ELSE 0 END) AS avg_days_before_due,
    SUM(AMT_UNDERPAYMENT) AS total_underpayment_amount,
    MAX(AMT_UNDERPAYMENT) AS max_underpayment_amount,
    CAST(SUM(CASE WHEN AMT_UNDERPAYMENT > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(SK_ID_PREV), 0) AS underpayment_ratio,
    AVG(AMT_PAYMENT / NULLIF(AMT_INSTALMENT, 0)) AS payment_to_inst_ratio_avg,
    MAX(AMT_PAYMENT / NULLIF(AMT_INSTALMENT, 0)) AS payment_to_inst_ratio_max,
    SUM(AMT_INSTALMENT - AMT_PAYMENT) AS payment_diff_sum,
    AVG(AMT_INSTALMENT - AMT_PAYMENT) AS payment_diff_avg,
    SUM(AMT_INSTALMENT) AS total_amount_installment_sum,
    SUM(AMT_PAYMENT) AS total_amount_payment_sum
FROM {{ ref('stg_installments') }}
GROUP BY SK_ID_CURR
