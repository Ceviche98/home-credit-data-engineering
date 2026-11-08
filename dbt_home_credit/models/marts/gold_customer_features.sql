{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH base_app AS (
    SELECT * FROM {{ ref('stg_application') }}
),

bureau_feats AS (
    SELECT * FROM {{ ref('int_bureau_features') }}
),

prev_app_feats AS (
    SELECT * FROM {{ ref('int_previous_app_features') }}
),

pos_cash_feats AS (
    SELECT * FROM {{ ref('int_pos_cash_features') }}
),

installments_feats AS (
    SELECT * FROM {{ ref('int_installments_features') }}
),

credit_card_feats AS (
    SELECT * FROM {{ ref('int_credit_card_features') }}
)

SELECT
    -- ========================================================
    -- 1. IDENTIFIERS & BASE DEMOGRAPHICS
    -- ========================================================
    app.SK_ID_CURR,
    app.TARGET,
    app.NAME_CONTRACT_TYPE,
    app.CODE_GENDER,
    app.FLAG_OWN_CAR,
    app.FLAG_OWN_REALTY,
    app.CNT_CHILDREN,
    app.CNT_FAM_MEMBERS,
    app.NAME_INCOME_TYPE,
    app.NAME_EDUCATION_TYPE,
    app.NAME_FAMILY_STATUS,
    app.NAME_HOUSING_TYPE,
    app.REGION_RATING_CLIENT,
    app.REGION_RATING_CLIENT_W_CITY,
    
    -- Client Stability
    app.AGE_YEARS,
    app.YEARS_EMPLOYED,
    app.IS_PENSIONER_OR_UNEMPLOYED,
    app.YEARS_REGISTRATION,
    app.YEARS_ID_PUBLISH,
    ROUND(app.YEARS_EMPLOYED / NULLIF(app.AGE_YEARS, 0), 4) AS employed_to_age_ratio,
    
    -- ========================================================
    -- 2. APPLICATION FINANCIAL METRICS & LEVERAGE RATIOS
    -- ========================================================
    app.AMT_INCOME_TOTAL,
    app.AMT_CREDIT,
    app.AMT_ANNUITY,
    app.AMT_GOODS_PRICE,
    ROUND(app.AMT_INCOME_TOTAL / NULLIF(app.CNT_FAM_MEMBERS, 0), 2) AS income_per_person,
    ROUND(app.AMT_CREDIT / NULLIF(app.AMT_INCOME_TOTAL, 0), 4) AS debt_to_income_ratio,
    ROUND(app.AMT_ANNUITY / NULLIF(app.AMT_INCOME_TOTAL, 0), 4) AS payment_to_income_ratio,
    ROUND(app.AMT_ANNUITY / NULLIF(app.AMT_CREDIT, 0), 4) AS annuity_to_credit_ratio,
    ROUND(app.AMT_CREDIT / NULLIF(app.AMT_GOODS_PRICE, 0), 4) AS loan_to_value_ratio,
    
    -- Contact & Document Compliance Signals
    (
        COALESCE(app.FLAG_MOBIL, 0) +
        COALESCE(app.FLAG_EMP_PHONE, 0) +
        COALESCE(app.FLAG_WORK_PHONE, 0) +
        COALESCE(app.FLAG_CONT_MOBILE, 0) +
        COALESCE(app.FLAG_PHONE, 0) +
        COALESCE(app.FLAG_EMAIL, 0)
    ) AS contact_flags_sum,
    (
        COALESCE(app.FLAG_DOCUMENT_2, 0) +
        COALESCE(app.FLAG_DOCUMENT_3, 0) +
        COALESCE(app.FLAG_DOCUMENT_4, 0) +
        COALESCE(app.FLAG_DOCUMENT_5, 0) +
        COALESCE(app.FLAG_DOCUMENT_6, 0) +
        COALESCE(app.FLAG_DOCUMENT_7, 0) +
        COALESCE(app.FLAG_DOCUMENT_8, 0) +
        COALESCE(app.FLAG_DOCUMENT_9, 0) +
        COALESCE(app.FLAG_DOCUMENT_10, 0) +
        COALESCE(app.FLAG_DOCUMENT_11, 0) +
        COALESCE(app.FLAG_DOCUMENT_12, 0) +
        COALESCE(app.FLAG_DOCUMENT_13, 0) +
        COALESCE(app.FLAG_DOCUMENT_14, 0) +
        COALESCE(app.FLAG_DOCUMENT_15, 0) +
        COALESCE(app.FLAG_DOCUMENT_16, 0) +
        COALESCE(app.FLAG_DOCUMENT_17, 0) +
        COALESCE(app.FLAG_DOCUMENT_18, 0) +
        COALESCE(app.FLAG_DOCUMENT_19, 0) +
        COALESCE(app.FLAG_DOCUMENT_20, 0) +
        COALESCE(app.FLAG_DOCUMENT_21, 0)
    ) AS document_flags_sum,
    
    -- ========================================================
    -- 3. EXTERNAL CREDIT SCORES TRIFECTA (KAGGLE TOP SIGNAL)
    -- ========================================================
    app.EXT_SOURCE_1,
    app.EXT_SOURCE_2,
    app.EXT_SOURCE_3,
    ROUND((COALESCE(app.EXT_SOURCE_1, 0.0) + COALESCE(app.EXT_SOURCE_2, 0.0) + COALESCE(app.EXT_SOURCE_3, 0.0)) / 3.0, 4) AS ext_source_mean,
    ROUND(COALESCE(app.EXT_SOURCE_1, 0.5) * COALESCE(app.EXT_SOURCE_2, 0.5) * COALESCE(app.EXT_SOURCE_3, 0.5), 6) AS ext_source_product,
    ROUND(app.EXT_SOURCE_1 / NULLIF(app.EXT_SOURCE_2, 0), 4) AS ext_source_1_to_2_ratio,
    ROUND(app.EXT_SOURCE_2 / NULLIF(app.EXT_SOURCE_3, 0), 4) AS ext_source_2_to_3_ratio,
    
    -- ========================================================
    -- 4. BUREAU & BUREAU BALANCE FEATURES
    -- ========================================================
    COALESCE(b.total_bureau_loans, 0) AS total_bureau_loans,
    COALESCE(b.active_bureau_loans, 0) AS active_bureau_loans,
    COALESCE(b.closed_bureau_loans, 0) AS closed_bureau_loans,
    COALESCE(b.active_bureau_ratio, 0.0) AS active_bureau_ratio,
    COALESCE(b.total_bureau_credit_sum, 0.0) AS total_bureau_credit_sum,
    COALESCE(b.avg_bureau_credit_sum, 0.0) AS avg_bureau_credit_sum,
    COALESCE(b.max_bureau_credit_sum, 0.0) AS max_bureau_credit_sum,
    COALESCE(b.total_bureau_debt_sum, 0.0) AS total_bureau_debt_sum,
    COALESCE(b.avg_bureau_debt_sum, 0.0) AS avg_bureau_debt_sum,
    COALESCE(b.total_bureau_limit_sum, 0.0) AS total_bureau_limit_sum,
    COALESCE(b.total_bureau_overdue_sum, 0.0) AS total_bureau_overdue_sum,
    COALESCE(b.max_bureau_overdue_sum, 0.0) AS max_bureau_overdue_sum,
    COALESCE(b.max_bureau_days_overdue, 0) AS max_bureau_days_overdue,
    COALESCE(b.avg_bureau_days_overdue, 0.0) AS avg_bureau_days_overdue,
    COALESCE(b.min_days_credit, 0) AS min_days_credit,
    COALESCE(b.max_days_credit, 0) AS max_days_credit,
    COALESCE(b.avg_days_credit, 0.0) AS avg_days_credit,
    COALESCE(b.cnt_credit_prolong_sum, 0) AS cnt_credit_prolong_sum,
    COALESCE(b.bureau_debt_to_credit_ratio, 0.0) AS bureau_debt_to_credit_ratio,
    COALESCE(b.bureau_overdue_to_credit_ratio, 0.0) AS bureau_overdue_to_credit_ratio,
    COALESCE(b.bb_months_balance_size, 0) AS bb_months_balance_size,
    COALESCE(b.bb_status_0_count, 0) AS bb_status_0_count,
    COALESCE(b.bb_status_dpd_count, 0) AS bb_status_dpd_count,
    COALESCE(b.bb_status_c_count, 0) AS bb_status_c_count,
    COALESCE(b.bb_status_x_count, 0) AS bb_status_x_count,
    COALESCE(b.bb_delinquency_ratio, 0.0) AS bb_delinquency_ratio,
    
    -- ========================================================
    -- 5. PREVIOUS APPLICATION FEATURES
    -- ========================================================
    COALESCE(p.total_prev_applications, 0) AS total_prev_applications,
    COALESCE(p.prev_approved_count, 0) AS prev_approved_count,
    COALESCE(p.prev_refused_count, 0) AS prev_refused_count,
    COALESCE(p.prev_canceled_count, 0) AS prev_canceled_count,
    COALESCE(p.prev_approval_ratio, 0.0) AS prev_approval_ratio,
    COALESCE(p.prev_refusal_ratio, 0.0) AS prev_refusal_ratio,
    COALESCE(p.prev_amt_application_sum, 0.0) AS prev_amt_application_sum,
    COALESCE(p.prev_amt_application_avg, 0.0) AS prev_amt_application_avg,
    COALESCE(p.prev_amt_application_max, 0.0) AS prev_amt_application_max,
    COALESCE(p.prev_amt_credit_sum, 0.0) AS prev_amt_credit_sum,
    COALESCE(p.prev_amt_credit_avg, 0.0) AS prev_amt_credit_avg,
    COALESCE(p.prev_amt_credit_max, 0.0) AS prev_amt_credit_max,
    COALESCE(p.prev_amt_annuity_sum, 0.0) AS prev_amt_annuity_sum,
    COALESCE(p.prev_amt_annuity_avg, 0.0) AS prev_amt_annuity_avg,
    COALESCE(p.prev_amt_annuity_max, 0.0) AS prev_amt_annuity_max,
    COALESCE(p.prev_amt_down_payment_sum, 0.0) AS prev_amt_down_payment_sum,
    COALESCE(p.prev_amt_down_payment_avg, 0.0) AS prev_amt_down_payment_avg,
    COALESCE(p.prev_app_to_credit_ratio_avg, 0.0) AS prev_app_to_credit_ratio_avg,
    COALESCE(p.prev_days_decision_min, 0) AS prev_days_decision_min,
    COALESCE(p.prev_days_decision_max, 0) AS prev_days_decision_max,
    COALESCE(p.prev_days_decision_avg, 0.0) AS prev_days_decision_avg,
    COALESCE(p.prev_cnt_payment_avg, 0.0) AS prev_cnt_payment_avg,
    COALESCE(p.prev_cnt_payment_sum, 0.0) AS prev_cnt_payment_sum,
    COALESCE(p.prev_rate_down_payment_avg, 0.0) AS prev_rate_down_payment_avg,
    COALESCE(p.prev_rate_down_payment_max, 0.0) AS prev_rate_down_payment_max,
    COALESCE(p.prev_insured_count, 0) AS prev_insured_count,
    
    -- ========================================================
    -- 6. POS CASH BALANCE FEATURES
    -- ========================================================
    COALESCE(pos.pos_cash_records_count, 0) AS pos_cash_records_count,
    COALESCE(pos.pos_unique_contracts_count, 0) AS pos_unique_contracts_count,
    COALESCE(pos.pos_months_balance_size, 0) AS pos_months_balance_size,
    COALESCE(pos.pos_months_balance_min, 0) AS pos_months_balance_min,
    COALESCE(pos.pos_months_balance_max, 0) AS pos_months_balance_max,
    COALESCE(pos.pos_sk_dpd_max, 0) AS pos_sk_dpd_max,
    COALESCE(pos.pos_sk_dpd_avg, 0.0) AS pos_sk_dpd_avg,
    COALESCE(pos.pos_sk_dpd_def_max, 0) AS pos_sk_dpd_def_max,
    COALESCE(pos.pos_sk_dpd_def_avg, 0.0) AS pos_sk_dpd_def_avg,
    COALESCE(pos.pos_dpd_nonzero_count, 0) AS pos_dpd_nonzero_count,
    COALESCE(pos.pos_active_status_count, 0) AS pos_active_status_count,
    COALESCE(pos.pos_completed_status_count, 0) AS pos_completed_status_count,
    COALESCE(pos.pos_completed_ratio, 0.0) AS pos_completed_ratio,
    COALESCE(pos.pos_future_installment_ratio_avg, 0.0) AS pos_future_installment_ratio_avg,
    
    -- ========================================================
    -- 7. REPAYMENT INSTALLMENTS FEATURES
    -- ========================================================
    COALESCE(i.total_installment_checks, 0) AS total_installment_checks,
    COALESCE(i.installments_unique_contracts, 0) AS installments_unique_contracts,
    COALESCE(i.count_late_installments, 0) AS count_late_installments,
    COALESCE(i.count_late_30d_installments, 0) AS count_late_30d_installments,
    COALESCE(i.count_late_60d_installments, 0) AS count_late_60d_installments,
    COALESCE(i.max_days_late, 0.0) AS max_days_late,
    COALESCE(i.avg_days_late, 0.0) AS avg_days_late,
    COALESCE(i.avg_days_before_due, 0.0) AS avg_days_before_due,
    COALESCE(i.total_underpayment_amount, 0.0) AS total_underpayment_amount,
    COALESCE(i.max_underpayment_amount, 0.0) AS max_underpayment_amount,
    COALESCE(i.underpayment_ratio, 0.0) AS underpayment_ratio,
    COALESCE(i.payment_to_inst_ratio_avg, 0.0) AS payment_to_inst_ratio_avg,
    COALESCE(i.payment_to_inst_ratio_max, 0.0) AS payment_to_inst_ratio_max,
    COALESCE(i.payment_diff_sum, 0.0) AS payment_diff_sum,
    COALESCE(i.payment_diff_avg, 0.0) AS payment_diff_avg,
    COALESCE(i.total_amount_installment_sum, 0.0) AS total_amount_installment_sum,
    COALESCE(i.total_amount_payment_sum, 0.0) AS total_amount_payment_sum,
    
    -- ========================================================
    -- 8. CREDIT CARD BALANCE FEATURES
    -- ========================================================
    COALESCE(cc.cc_records_count, 0) AS cc_records_count,
    COALESCE(cc.cc_unique_cards_count, 0) AS cc_unique_cards_count,
    COALESCE(cc.cc_months_balance_size, 0) AS cc_months_balance_size,
    COALESCE(cc.cc_months_balance_min, 0) AS cc_months_balance_min,
    COALESCE(cc.cc_months_balance_max, 0) AS cc_months_balance_max,
    COALESCE(cc.cc_amt_balance_avg, 0.0) AS cc_amt_balance_avg,
    COALESCE(cc.cc_amt_balance_max, 0.0) AS cc_amt_balance_max,
    COALESCE(cc.cc_amt_credit_limit_avg, 0.0) AS cc_amt_credit_limit_avg,
    COALESCE(cc.cc_amt_credit_limit_max, 0.0) AS cc_amt_credit_limit_max,
    COALESCE(cc.cc_utilization_ratio_avg, 0.0) AS cc_utilization_ratio_avg,
    COALESCE(cc.cc_drawings_atm_sum, 0.0) AS cc_drawings_atm_sum,
    COALESCE(cc.cc_drawings_atm_avg, 0.0) AS cc_drawings_atm_avg,
    COALESCE(cc.cc_drawings_current_sum, 0.0) AS cc_drawings_current_sum,
    COALESCE(cc.cc_drawings_current_avg, 0.0) AS cc_drawings_current_avg,
    COALESCE(cc.cc_drawings_pos_sum, 0.0) AS cc_drawings_pos_sum,
    COALESCE(cc.cc_drawings_pos_avg, 0.0) AS cc_drawings_pos_avg,
    COALESCE(cc.cc_cnt_drawings_atm_sum, 0.0) AS cc_cnt_drawings_atm_sum,
    COALESCE(cc.cc_cnt_drawings_atm_avg, 0.0) AS cc_cnt_drawings_atm_avg,
    COALESCE(cc.cc_cnt_drawings_current_sum, 0.0) AS cc_cnt_drawings_current_sum,
    COALESCE(cc.cc_cnt_drawings_current_avg, 0.0) AS cc_cnt_drawings_current_avg,
    COALESCE(cc.cc_payment_to_min_reg_ratio_avg, 0.0) AS cc_payment_to_min_reg_ratio_avg,
    COALESCE(cc.cc_amt_total_receivable_avg, 0.0) AS cc_amt_total_receivable_avg,
    COALESCE(cc.cc_amt_total_receivable_max, 0.0) AS cc_amt_total_receivable_max,
    COALESCE(cc.cc_amt_receivable_principal_avg, 0.0) AS cc_amt_receivable_principal_avg,
    COALESCE(cc.cc_amt_receivable_principal_max, 0.0) AS cc_amt_receivable_principal_max,
    COALESCE(cc.cc_sk_dpd_max, 0) AS cc_sk_dpd_max,
    COALESCE(cc.cc_sk_dpd_avg, 0.0) AS cc_sk_dpd_avg,
    COALESCE(cc.cc_sk_dpd_def_max, 0) AS cc_sk_dpd_def_max,
    COALESCE(cc.cc_sk_dpd_def_avg, 0.0) AS cc_sk_dpd_def_avg,
    COALESCE(cc.cc_dpd_nonzero_count, 0) AS cc_dpd_nonzero_count,
    
    -- ========================================================
    -- 9. EVENT TIMESTAMP FOR FEATURE STORE (FEAST)
    -- ========================================================
    current_timestamp() AS event_timestamp

FROM base_app app
LEFT JOIN bureau_feats b ON app.SK_ID_CURR = b.SK_ID_CURR
LEFT JOIN prev_app_feats p ON app.SK_ID_CURR = p.SK_ID_CURR
LEFT JOIN pos_cash_feats pos ON app.SK_ID_CURR = pos.SK_ID_CURR
LEFT JOIN installments_feats i ON app.SK_ID_CURR = i.SK_ID_CURR
LEFT JOIN credit_card_feats cc ON app.SK_ID_CURR = cc.SK_ID_CURR
