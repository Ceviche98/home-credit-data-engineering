"""
Gold Layer Materializer: Executes the comprehensive 150+ Feature Mart in PySpark.

Capabilities:
  - Loads all 7 Silver Delta tables into Spark SQL context:
    (application_train, bureau, bureau_balance, previous_application,
     pos_cash_balance, installments_payments, credit_card_balance).
  - Computes the Master Credit Risk Feature Mart with 150+ engineered signals:
    * Banking leverage & solvency ratios (debt-to-income, loan-to-value, annuity-to-credit).
    * Kaggle winning external score non-linear interaction terms (mean, product, ratios).
    * Comprehensive historical aggregations across all 6 auxiliary credit tables.
  - Materializes into data/lakehouse/gold/customer_features as an ACID Delta Table.
  - Applies Z-Ordering on SK_ID_CURR for low-latency Feature Store lookups.
"""

import logging
from pathlib import Path

from delta.tables import DeltaTable

from home_credit_de.ingestion.bronze_loader import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def materialize_gold_features(lakehouse_root: Path = Path("data/lakehouse")):
    logger.info("=== Starting Comprehensive Gold Feature Mart Materialization (150+ Features) ===")
    spark = get_spark_session("HomeCredit_Gold_Mart_Comprehensive")

    silver_dir = lakehouse_root / "silver"
    gold_dir = lakehouse_root / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    # 1. Register all 7 Silver Delta tables as Spark SQL views
    logger.info("Registering all 7 Silver Delta tables in Spark SQL context...")
    tables = [
        ("silver_application_train", "application_train"),
        ("silver_bureau", "bureau"),
        ("silver_bureau_balance", "bureau_balance"),
        ("silver_previous_application", "previous_application"),
        ("silver_pos_cash_balance", "pos_cash_balance"),
        ("silver_installments_payments", "installments_payments"),
        ("silver_credit_card_balance", "credit_card_balance"),
    ]
    for view_name, table_folder in tables:
        path = str(silver_dir / table_folder)
        spark.read.format("delta").load(path).createOrReplaceTempView(view_name)
        logger.info(f"Registered view: {view_name}")

    # 2. Execute the Full 150+ Feature SQL Pipeline
    logger.info("Executing comprehensive feature engineering across all 7 tables...")
    gold_sql = """
    WITH base_app AS (
        SELECT
            SK_ID_CURR,
            TARGET,
            NAME_CONTRACT_TYPE,
            CODE_GENDER,
            FLAG_OWN_CAR,
            FLAG_OWN_REALTY,
            CNT_CHILDREN,
            CNT_FAM_MEMBERS,
            NAME_INCOME_TYPE,
            NAME_EDUCATION_TYPE,
            NAME_FAMILY_STATUS,
            NAME_HOUSING_TYPE,
            REGION_RATING_CLIENT,
            REGION_RATING_CLIENT_W_CITY,
            ROUND(ABS(DAYS_BIRTH) / 365.25, 1) AS AGE_YEARS,
            ROUND(ABS(DAYS_EMPLOYED) / 365.25, 1) AS YEARS_EMPLOYED,
            IS_PENSIONER_OR_UNEMPLOYED,
            ROUND(ABS(DAYS_REGISTRATION) / 365.25, 1) AS YEARS_REGISTRATION,
            ROUND(ABS(DAYS_ID_PUBLISH) / 365.25, 1) AS YEARS_ID_PUBLISH,
            ROUND(ROUND(ABS(DAYS_EMPLOYED) / 365.25, 1) / NULLIF(ROUND(ABS(DAYS_BIRTH) / 365.25, 1), 0), 4) AS employed_to_age_ratio,
            AMT_INCOME_TOTAL,
            AMT_CREDIT,
            AMT_ANNUITY,
            AMT_GOODS_PRICE,
            ROUND(AMT_INCOME_TOTAL / NULLIF(CNT_FAM_MEMBERS, 0), 2) AS income_per_person,
            ROUND(AMT_CREDIT / NULLIF(AMT_INCOME_TOTAL, 0), 4) AS debt_to_income_ratio,
            ROUND(AMT_ANNUITY / NULLIF(AMT_INCOME_TOTAL, 0), 4) AS payment_to_income_ratio,
            ROUND(AMT_ANNUITY / NULLIF(AMT_CREDIT, 0), 4) AS annuity_to_credit_ratio,
            ROUND(AMT_CREDIT / NULLIF(AMT_GOODS_PRICE, 0), 4) AS loan_to_value_ratio,
            (
                COALESCE(FLAG_MOBIL, 0) + COALESCE(FLAG_EMP_PHONE, 0) +
                COALESCE(FLAG_WORK_PHONE, 0) + COALESCE(FLAG_CONT_MOBILE, 0) +
                COALESCE(FLAG_PHONE, 0) + COALESCE(FLAG_EMAIL, 0)
            ) AS contact_flags_sum,
            (
                COALESCE(FLAG_DOCUMENT_2, 0) + COALESCE(FLAG_DOCUMENT_3, 0) +
                COALESCE(FLAG_DOCUMENT_4, 0) + COALESCE(FLAG_DOCUMENT_5, 0) +
                COALESCE(FLAG_DOCUMENT_6, 0) + COALESCE(FLAG_DOCUMENT_7, 0) +
                COALESCE(FLAG_DOCUMENT_8, 0) + COALESCE(FLAG_DOCUMENT_9, 0) +
                COALESCE(FLAG_DOCUMENT_10, 0) + COALESCE(FLAG_DOCUMENT_11, 0) +
                COALESCE(FLAG_DOCUMENT_12, 0) + COALESCE(FLAG_DOCUMENT_13, 0) +
                COALESCE(FLAG_DOCUMENT_14, 0) + COALESCE(FLAG_DOCUMENT_15, 0) +
                COALESCE(FLAG_DOCUMENT_16, 0) + COALESCE(FLAG_DOCUMENT_17, 0) +
                COALESCE(FLAG_DOCUMENT_18, 0) + COALESCE(FLAG_DOCUMENT_19, 0) +
                COALESCE(FLAG_DOCUMENT_20, 0) + COALESCE(FLAG_DOCUMENT_21, 0)
            ) AS document_flags_sum,
            EXT_SOURCE_1,
            EXT_SOURCE_2,
            EXT_SOURCE_3,
            ROUND((COALESCE(EXT_SOURCE_1, 0.0) + COALESCE(EXT_SOURCE_2, 0.0) + COALESCE(EXT_SOURCE_3, 0.0)) / 3.0, 4) AS ext_source_mean,
            ROUND(COALESCE(EXT_SOURCE_1, 0.5) * COALESCE(EXT_SOURCE_2, 0.5) * COALESCE(EXT_SOURCE_3, 0.5), 6) AS ext_source_product,
            ROUND(EXT_SOURCE_1 / NULLIF(EXT_SOURCE_2, 0), 4) AS ext_source_1_to_2_ratio,
            ROUND(EXT_SOURCE_2 / NULLIF(EXT_SOURCE_3, 0), 4) AS ext_source_2_to_3_ratio
        FROM silver_application_train
    ),

    bureau_agg AS (
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
            SUM(CNT_CREDIT_PROLONG) AS cnt_credit_prolong_sum,
            CASE 
                WHEN SUM(AMT_CREDIT_SUM) > 0 THEN ROUND(SUM(AMT_CREDIT_SUM_DEBT) / SUM(AMT_CREDIT_SUM), 4)
                ELSE 0.0 
            END AS bureau_debt_to_credit_ratio,
            CASE 
                WHEN SUM(AMT_CREDIT_SUM) > 0 THEN ROUND(SUM(AMT_CREDIT_SUM_OVERDUE) / SUM(AMT_CREDIT_SUM), 4)
                ELSE 0.0 
            END AS bureau_overdue_to_credit_ratio
        FROM silver_bureau
        GROUP BY SK_ID_CURR
    ),

    bureau_balance_agg AS (
        SELECT
            SK_ID_CURR,
            COUNT(MONTHS_BALANCE) AS bb_months_balance_size,
            MIN(MONTHS_BALANCE) AS bb_months_balance_min,
            SUM(CASE WHEN STATUS = '0' THEN 1 ELSE 0 END) AS bb_status_0_count,
            SUM(CASE WHEN STATUS IN ('1', '2', '3', '4', '5') THEN 1 ELSE 0 END) AS bb_status_dpd_count,
            SUM(CASE WHEN STATUS = 'C' THEN 1 ELSE 0 END) AS bb_status_c_count,
            SUM(CASE WHEN STATUS = 'X' THEN 1 ELSE 0 END) AS bb_status_x_count,
            ROUND(CAST(SUM(CASE WHEN STATUS IN ('1', '2', '3', '4', '5') THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(MONTHS_BALANCE), 0), 4) AS bb_delinquency_ratio
        FROM silver_bureau_balance
        GROUP BY SK_ID_CURR
    ),

    prev_app_agg AS (
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
        FROM silver_previous_application
        GROUP BY SK_ID_CURR
    ),

    pos_cash_agg AS (
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
        FROM silver_pos_cash_balance
        GROUP BY SK_ID_CURR
    ),

    installments_agg AS (
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
        FROM silver_installments_payments
        GROUP BY SK_ID_CURR
    ),

    credit_card_agg AS (
        SELECT
            SK_ID_CURR,
            COUNT(SK_ID_PREV) AS cc_records_count,
            COUNT(DISTINCT SK_ID_PREV) AS cc_unique_cards_count,
            COUNT(MONTHS_BALANCE) AS cc_months_balance_size,
            MIN(MONTHS_BALANCE) AS cc_months_balance_min,
            MAX(MONTHS_BALANCE) AS cc_months_balance_max,
            AVG(AMT_BALANCE) AS cc_amt_balance_avg,
            MAX(AMT_BALANCE) AS cc_amt_balance_max,
            AVG(AMT_CREDIT_LIMIT_ACTUAL) AS cc_amt_credit_limit_avg,
            MAX(AMT_CREDIT_LIMIT_ACTUAL) AS cc_amt_credit_limit_max,
            AVG(AMT_BALANCE / NULLIF(AMT_CREDIT_LIMIT_ACTUAL, 0)) AS cc_utilization_ratio_avg,
            SUM(AMT_DRAWINGS_ATM_CURRENT) AS cc_drawings_atm_sum,
            AVG(AMT_DRAWINGS_ATM_CURRENT) AS cc_drawings_atm_avg,
            SUM(AMT_DRAWINGS_CURRENT) AS cc_drawings_current_sum,
            AVG(AMT_DRAWINGS_CURRENT) AS cc_drawings_current_avg,
            SUM(AMT_DRAWINGS_POS_CURRENT) AS cc_drawings_pos_sum,
            AVG(AMT_DRAWINGS_POS_CURRENT) AS cc_drawings_pos_avg,
            SUM(CNT_DRAWINGS_ATM_CURRENT) AS cc_cnt_drawings_atm_sum,
            AVG(CNT_DRAWINGS_ATM_CURRENT) AS cc_cnt_drawings_atm_avg,
            SUM(CNT_DRAWINGS_CURRENT) AS cc_cnt_drawings_current_sum,
            AVG(CNT_DRAWINGS_CURRENT) AS cc_cnt_drawings_current_avg,
            AVG(AMT_PAYMENT_CURRENT / NULLIF(AMT_INST_MIN_REGULARITY, 0)) AS cc_payment_to_min_reg_ratio_avg,
            AVG(AMT_TOTAL_RECEIVABLE) AS cc_amt_total_receivable_avg,
            MAX(AMT_TOTAL_RECEIVABLE) AS cc_amt_total_receivable_max,
            AVG(AMT_RECEIVABLE_PRINCIPAL) AS cc_amt_receivable_principal_avg,
            MAX(AMT_RECEIVABLE_PRINCIPAL) AS cc_amt_receivable_principal_max,
            MAX(SK_DPD) AS cc_sk_dpd_max,
            AVG(SK_DPD) AS cc_sk_dpd_avg,
            MAX(SK_DPD_DEF) AS cc_sk_dpd_def_max,
            AVG(SK_DPD_DEF) AS cc_sk_dpd_def_avg,
            SUM(CASE WHEN SK_DPD > 0 THEN 1 ELSE 0 END) AS cc_dpd_nonzero_count
        FROM silver_credit_card_balance
        GROUP BY SK_ID_CURR
    )

    SELECT
        -- Identifiers & Base Demographics
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
        app.AGE_YEARS,
        app.YEARS_EMPLOYED,
        app.IS_PENSIONER_OR_UNEMPLOYED,
        app.YEARS_REGISTRATION,
        app.YEARS_ID_PUBLISH,
        app.employed_to_age_ratio,
        
        -- Financial Ratios
        app.AMT_INCOME_TOTAL,
        app.AMT_CREDIT,
        app.AMT_ANNUITY,
        app.AMT_GOODS_PRICE,
        app.income_per_person,
        app.debt_to_income_ratio,
        app.payment_to_income_ratio,
        app.annuity_to_credit_ratio,
        app.loan_to_value_ratio,
        app.contact_flags_sum,
        app.document_flags_sum,
        
        -- External Scores Trifecta
        app.EXT_SOURCE_1,
        app.EXT_SOURCE_2,
        app.EXT_SOURCE_3,
        app.ext_source_mean,
        app.ext_source_product,
        app.ext_source_1_to_2_ratio,
        app.ext_source_2_to_3_ratio,
        
        -- Bureau Features
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
        COALESCE(bb.bb_months_balance_size, 0) AS bb_months_balance_size,
        COALESCE(bb.bb_status_0_count, 0) AS bb_status_0_count,
        COALESCE(bb.bb_status_dpd_count, 0) AS bb_status_dpd_count,
        COALESCE(bb.bb_status_c_count, 0) AS bb_status_c_count,
        COALESCE(bb.bb_status_x_count, 0) AS bb_status_x_count,
        COALESCE(bb.bb_delinquency_ratio, 0.0) AS bb_delinquency_ratio,
        
        -- Previous Application Features
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
        
        -- POS Cash Features
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
        
        -- Repayment Installments Features
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
        
        -- Credit Card Features
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
        
        -- Feature Store Timestamp
        current_timestamp() AS event_timestamp

    FROM base_app app
    LEFT JOIN bureau_agg b ON app.SK_ID_CURR = b.SK_ID_CURR
    LEFT JOIN bureau_balance_agg bb ON app.SK_ID_CURR = bb.SK_ID_CURR
    LEFT JOIN prev_app_agg p ON app.SK_ID_CURR = p.SK_ID_CURR
    LEFT JOIN pos_cash_agg pos ON app.SK_ID_CURR = pos.SK_ID_CURR
    LEFT JOIN installments_agg i ON app.SK_ID_CURR = i.SK_ID_CURR
    LEFT JOIN credit_card_agg cc ON app.SK_ID_CURR = cc.SK_ID_CURR
    """

    gold_df = spark.sql(gold_sql)
    target_gold_path = str(gold_dir / "customer_features")

    logger.info(f"Writing upgraded Comprehensive Gold Feature Mart to Delta Lake at: {target_gold_path}...")
    gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_gold_path)

    logger.info("Applying OPTIMIZE ZORDER BY (SK_ID_CURR) on Gold Feature Table...")
    try:
        gold_delta = DeltaTable.forPath(spark, target_gold_path.replace("\\", "/"))
        gold_delta.optimize().executeZOrderBy("SK_ID_CURR")
    except Exception as e:
        logger.warning(f"OPTIMIZE ZORDER skipped: {e}")

    row_count = gold_df.count()
    col_count = len(gold_df.columns)
    logger.info(f"=== COMPREHENSIVE GOLD FEATURE TABLE SUCCESS: {row_count:,} rows x {col_count} columns! ===")
    spark.stop()


if __name__ == "__main__":
    materialize_gold_features()
