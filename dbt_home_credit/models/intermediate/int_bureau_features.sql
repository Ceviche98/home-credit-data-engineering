{{ config(materialized='view') }}

WITH bureau_metrics AS (
    SELECT
        SK_ID_CURR,
        COUNT(SK_ID_BUREAU) AS total_bureau_loans,
        SUM(CASE WHEN CREDIT_ACTIVE = 'Active' THEN 1 ELSE 0 END) AS active_bureau_loans,
        SUM(CASE WHEN CREDIT_ACTIVE = 'Closed' THEN 1 ELSE 0 END) AS closed_bureau_loans,
        ROUND(CAST(SUM(CASE WHEN CREDIT_ACTIVE = 'Active' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(SK_ID_BUREAU), 0), 4) AS active_bureau_ratio,
        SUM(AMT_CREDIT_SUM) AS total_bureau_credit_sum,
        AVG(AMT_CREDIT_SUM) AS avg_bureau_credit_sum,
        MAX(AMT_CREDIT_SUM) AS max_bureau_credit_sum,
        SUM(AMT_CREDIT_SUM_DEBT) AS total_bureau_debt_sum,
        AVG(AMT_CREDIT_SUM_DEBT) AS avg_bureau_debt_sum,
        SUM(AMT_CREDIT_SUM_LIMIT) AS total_bureau_limit_sum,
        SUM(AMT_CREDIT_SUM_OVERDUE) AS total_bureau_overdue_sum,
        MAX(AMT_CREDIT_SUM_OVERDUE) AS max_bureau_overdue_sum,
        MAX(CREDIT_DAY_OVERDUE) AS max_bureau_days_overdue,
        AVG(CREDIT_DAY_OVERDUE) AS avg_bureau_days_overdue,
        MIN(DAYS_CREDIT) AS min_days_credit,
        MAX(DAYS_CREDIT) AS max_days_credit,
        AVG(DAYS_CREDIT) AS avg_days_credit,
        MIN(DAYS_CREDIT_ENDDATE) AS min_days_credit_enddate,
        MAX(DAYS_CREDIT_ENDDATE) AS max_days_credit_enddate,
        SUM(CNT_CREDIT_PROLONG) AS cnt_credit_prolong_sum,
        CASE 
            WHEN SUM(AMT_CREDIT_SUM) > 0 THEN ROUND(SUM(AMT_CREDIT_SUM_DEBT) / SUM(AMT_CREDIT_SUM), 4)
            ELSE 0.0 
        END AS bureau_debt_to_credit_ratio,
        CASE 
            WHEN SUM(AMT_CREDIT_SUM) > 0 THEN ROUND(SUM(AMT_CREDIT_SUM_OVERDUE) / SUM(AMT_CREDIT_SUM), 4)
            ELSE 0.0 
        END AS bureau_overdue_to_credit_ratio
    FROM {{ ref('stg_bureau') }}
    GROUP BY SK_ID_CURR
),

bureau_balance_metrics AS (
    SELECT
        SK_ID_CURR,
        COUNT(MONTHS_BALANCE) AS bb_months_balance_size,
        MIN(MONTHS_BALANCE) AS bb_months_balance_min,
        SUM(CASE WHEN STATUS = '0' THEN 1 ELSE 0 END) AS bb_status_0_count,
        SUM(CASE WHEN STATUS IN ('1', '2', '3', '4', '5') THEN 1 ELSE 0 END) AS bb_status_dpd_count,
        SUM(CASE WHEN STATUS = 'C' THEN 1 ELSE 0 END) AS bb_status_c_count,
        SUM(CASE WHEN STATUS = 'X' THEN 1 ELSE 0 END) AS bb_status_x_count,
        ROUND(CAST(SUM(CASE WHEN STATUS IN ('1', '2', '3', '4', '5') THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(MONTHS_BALANCE), 0), 4) AS bb_delinquency_ratio
    FROM {{ ref('stg_bureau_balance') }}
    GROUP BY SK_ID_CURR
)

SELECT
    b.*,
    COALESCE(bb.bb_months_balance_size, 0) AS bb_months_balance_size,
    COALESCE(bb.bb_months_balance_min, 0) AS bb_months_balance_min,
    COALESCE(bb.bb_status_0_count, 0) AS bb_status_0_count,
    COALESCE(bb.bb_status_dpd_count, 0) AS bb_status_dpd_count,
    COALESCE(bb.bb_status_c_count, 0) AS bb_status_c_count,
    COALESCE(bb.bb_status_x_count, 0) AS bb_status_x_count,
    COALESCE(bb.bb_delinquency_ratio, 0.0) AS bb_delinquency_ratio
FROM bureau_metrics b
LEFT JOIN bureau_balance_metrics bb ON b.SK_ID_CURR = bb.SK_ID_CURR
