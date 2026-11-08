"""
Feast Feature Definitions for Home Credit Default Risk.

Maps our Lakehouse Gold Delta/Parquet table to Feast:
  - Entity: customer (SK_ID_CURR)
  - Offline Source: FileSource pointing to data/lakehouse/gold/customer_features
  - Feature View: customer_credit_features (Banking leverage, Kaggle signals, Delinquency)
"""

from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float32, Float64, Int64, String

# Resolve absolute path to Gold customer_features Delta table
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_FEATURES_PATH = str(PROJECT_ROOT / "data" / "lakehouse" / "gold" / "customer_features").replace("\\", "/")

# 1. Define Entity
customer = Entity(
    name="customer",
    join_keys=["SK_ID_CURR"],
    value_type=ValueType.INT64,
    description="Home Credit Loan Applicant Entity",
)

# 2. Define Offline Data Source
customer_features_source = FileSource(
    name="customer_gold_features_source",
    path=GOLD_FEATURES_PATH,
    timestamp_field="event_timestamp",
)

# 3. Define Feature View for Real-Time & Historical Serving
customer_credit_features = FeatureView(
    name="customer_credit_features",
    entities=[customer],
    ttl=timedelta(days=365 * 5),
    schema=[
        # Demographics & Stability
        Field(name="AGE_YEARS", dtype=Float64),
        Field(name="YEARS_EMPLOYED", dtype=Float64),
        Field(name="employed_to_age_ratio", dtype=Float64),
        
        # Financial Leverage Ratios
        Field(name="debt_to_income_ratio", dtype=Float64),
        Field(name="payment_to_income_ratio", dtype=Float64),
        Field(name="annuity_to_credit_ratio", dtype=Float64),
        Field(name="loan_to_value_ratio", dtype=Float64),
        Field(name="income_per_person", dtype=Float64),
        
        # External Risk Scores Trifecta
        Field(name="ext_source_mean", dtype=Float64),
        Field(name="ext_source_product", dtype=Float64),
        Field(name="ext_source_1_to_2_ratio", dtype=Float64),
        Field(name="ext_source_2_to_3_ratio", dtype=Float64),
        
        # Bureau & Bureau Balance Delinquency
        Field(name="total_bureau_loans", dtype=Int64),
        Field(name="active_bureau_loans", dtype=Int64),
        Field(name="bureau_debt_to_credit_ratio", dtype=Float64),
        Field(name="bb_delinquency_ratio", dtype=Float64),
        Field(name="max_bureau_days_overdue", dtype=Int64),
        
        # Previous Applications
        Field(name="total_prev_applications", dtype=Int64),
        Field(name="prev_approval_ratio", dtype=Float64),
        Field(name="prev_refusal_ratio", dtype=Float64),
        Field(name="prev_app_to_credit_ratio_avg", dtype=Float64),
        
        # POS Cash Performance
        Field(name="pos_cash_records_count", dtype=Int64),
        Field(name="pos_sk_dpd_max", dtype=Int64),
        Field(name="pos_completed_ratio", dtype=Float64),
        
        # Repayment Installments Delinquency
        Field(name="total_installment_checks", dtype=Int64),
        Field(name="count_late_installments", dtype=Int64),
        Field(name="count_late_30d_installments", dtype=Int64),
        Field(name="underpayment_ratio", dtype=Float64),
        Field(name="max_days_late", dtype=Float64),
        
        # Credit Card Revolving Utilization
        Field(name="cc_records_count", dtype=Int64),
        Field(name="cc_utilization_ratio_avg", dtype=Float64),
        Field(name="cc_drawings_atm_sum", dtype=Float64),
        Field(name="cc_sk_dpd_max", dtype=Int64),
    ],
    online=True,
    source=customer_features_source,
    tags={"team": "credit_risk_ml", "tier": "gold"},
)
