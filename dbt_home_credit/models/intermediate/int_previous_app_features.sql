{{ config(materialized='view') }}

SELECT
    SK_ID_CURR,
    COUNT(SK_ID_PREV) AS total_prev_applications,
    SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Approved' THEN 1 ELSE 0 END) AS prev_approved_count,
    SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Refused' THEN 1 ELSE 0 END) AS prev_refused_count,
    SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Canceled' THEN 1 ELSE 0 END) AS prev_canceled_count,
    ROUND(CAST(SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Approved' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(SK_ID_PREV), 0), 4) AS prev_approval_ratio,
    ROUND(CAST(SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Refused' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(SK_ID_PREV), 0), 4) AS prev_refusal_ratio,
    SUM(AMT_APPLICATION) AS prev_amt_application_sum,
    AVG(AMT_APPLICATION) AS prev_amt_application_avg,
    MAX(AMT_APPLICATION) AS prev_amt_application_max,
    SUM(AMT_CREDIT) AS prev_amt_credit_sum,
    AVG(AMT_CREDIT) AS prev_amt_credit_avg,
    MAX(AMT_CREDIT) AS prev_amt_credit_max,
    SUM(AMT_ANNUITY) AS prev_amt_annuity_sum,
    AVG(AMT_ANNUITY) AS prev_amt_annuity_avg,
    MAX(AMT_ANNUITY) AS prev_amt_annuity_max,
    SUM(AMT_DOWN_PAYMENT) AS prev_amt_down_payment_sum,
    AVG(AMT_DOWN_PAYMENT) AS prev_amt_down_payment_avg,
    AVG(AMT_APPLICATION / NULLIF(AMT_CREDIT, 0)) AS prev_app_to_credit_ratio_avg,
    MIN(DAYS_DECISION) AS prev_days_decision_min,
    MAX(DAYS_DECISION) AS prev_days_decision_max,
    AVG(DAYS_DECISION) AS prev_days_decision_avg,
    AVG(CNT_PAYMENT) AS prev_cnt_payment_avg,
    SUM(CNT_PAYMENT) AS prev_cnt_payment_sum,
    AVG(RATE_DOWN_PAYMENT) AS prev_rate_down_payment_avg,
    MAX(RATE_DOWN_PAYMENT) AS prev_rate_down_payment_max,
    SUM(COALESCE(NFLAG_INSURED_ON_APPROVAL, 0)) AS prev_insured_count
FROM {{ ref('stg_previous_application') }}
GROUP BY SK_ID_CURR
