"""
Automated Data Quality Circuit Breaker for Home Credit Lakehouse.

In modern enterprise data platforms, a "Circuit Breaker" is an active quality gate.
Instead of passively monitoring bad data after it reaches production, the circuit
breaker evaluates deterministic data contracts and immediately halts downstream
materialization (such as syncing to the Feast Online Feature Store or training ML models)
if critical invariants are violated.

Capabilities:
  1. Primary Key Uniqueness & Nullability Checks (Zero duplicate or null SK_ID_CURR).
  2. Legacy Sentinel Anomaly Leakage Checks (Zero occurrences of 365243).
  3. Domain Bounds & Financial Sanity Checks (AMT_CREDIT > 0, AMT_INCOME > 0, leverage ratios >= 0).
  4. Normalized Score Boundary Validations (0.0 <= EXT_SOURCE <= 1.0).
  5. Feature Store Contract Verification (event_timestamp present, column count >= 140).
  6. Audit Report Generation (JSON report written to data/lakehouse/_meta/quality_reports/).
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from home_credit_de.ingestion.bronze_loader import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class CheckSeverity(str, Enum):
    CRITICAL = "CRITICAL"  # Fails the pipeline immediately
    WARNING = "WARNING"    # Logs warning in audit ledger, pipeline proceeds


@dataclass
class QualityCheckResult:
    check_name: str
    target_table: str
    column: str | None
    severity: CheckSeverity
    passed: bool
    details: str
    metric_value: Any | None = None
    threshold: Any | None = None


class DataQualityCircuitBreaker:
    def __init__(self, lakehouse_root: Path = Path("data/lakehouse")):
        self.lakehouse_root = lakehouse_root
        self.reports_dir = lakehouse_root / "_meta" / "quality_reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.results: list[QualityCheckResult] = []

    def check_primary_key(self, df: DataFrame, table_name: str, pk_col: str = "SK_ID_CURR") -> bool:
        """Verifies that the primary key is 100% unique and non-null."""
        logger.info(f"Running Primary Key check on {table_name}.{pk_col}...")
        total_rows = df.count()
        null_pks = df.filter(F.col(pk_col).isNull()).count()
        distinct_pks = df.select(pk_col).distinct().count()
        duplicate_pks = total_rows - distinct_pks

        # 1. Null PK Check
        null_passed = null_pks == 0
        self.results.append(
            QualityCheckResult(
                check_name="primary_key_not_null",
                target_table=table_name,
                column=pk_col,
                severity=CheckSeverity.CRITICAL,
                passed=null_passed,
                details=f"Found {null_pks:,} NULL primary keys out of {total_rows:,} rows",
                metric_value=null_pks,
                threshold=0,
            )
        )

        # 2. Duplicate PK Check
        dup_passed = duplicate_pks == 0
        self.results.append(
            QualityCheckResult(
                check_name="primary_key_unique",
                target_table=table_name,
                column=pk_col,
                severity=CheckSeverity.CRITICAL,
                passed=dup_passed,
                details=f"Found {duplicate_pks:,} duplicate primary keys (total: {total_rows:,}, distinct: {distinct_pks:,})",
                metric_value=duplicate_pks,
                threshold=0,
            )
        )
        return null_passed and dup_passed

    def check_sentinel_leakage(self, df: DataFrame, table_name: str) -> bool:
        """Verifies that the legacy 365243 sentinel did not leak into Silver or Gold."""
        logger.info(f"Running Sentinel Value Leakage check (365243) on {table_name}...")
        cols_to_check = [c for c in df.columns if "EMPLOYED" in c.upper() or "DAY" in c.upper() or "YEAR" in c.upper()]
        
        sentinel_found = 0
        for c in cols_to_check:
            # Check if 365243 exists in this column
            count = df.filter(F.col(c) == 365243).count()
            if count > 0:
                sentinel_found += count

        passed = sentinel_found == 0
        self.results.append(
            QualityCheckResult(
                check_name="zero_sentinel_leakage_365243",
                target_table=table_name,
                column=str(cols_to_check[:3]),
                severity=CheckSeverity.CRITICAL,
                passed=passed,
                details=f"Found {sentinel_found} occurrences of legacy sentinel 365243 across date/employment columns",
                metric_value=sentinel_found,
                threshold=0,
            )
        )
        return passed

    def check_target_distribution(self, df: DataFrame, table_name: str) -> bool:
        """Verifies binary label integrity: values strictly in {0, 1} and reasonable default rate."""
        logger.info(f"Checking TARGET column integrity on {table_name}...")
        total_rows = df.count()
        invalid_labels = df.filter(~F.col("TARGET").isin([0, 1])).count()
        default_count = df.filter(F.col("TARGET") == 1).count()
        default_rate = round(default_count / total_rows, 4)

        # 1. Invalid values
        values_passed = invalid_labels == 0
        self.results.append(
            QualityCheckResult(
                check_name="target_binary_domain",
                target_table=table_name,
                column="TARGET",
                severity=CheckSeverity.CRITICAL,
                passed=values_passed,
                details=f"Found {invalid_labels} non-binary values in TARGET column",
                metric_value=invalid_labels,
                threshold=0,
            )
        )

        # 2. Realistic default rate (banking warning: between 3% and 20%)
        rate_passed = 0.03 <= default_rate <= 0.20
        self.results.append(
            QualityCheckResult(
                check_name="target_default_rate_bounds",
                target_table=table_name,
                column="TARGET",
                severity=CheckSeverity.WARNING,
                passed=rate_passed,
                details=f"Default rate is {default_rate:.2%} ({default_count:,} / {total_rows:,})",
                metric_value=default_rate,
                threshold="[0.03, 0.20]",
            )
        )
        return values_passed

    def check_financial_bounds(self, df: DataFrame, table_name: str) -> bool:
        """Verifies strictly positive financial amounts and valid non-negative leverage ratios."""
        logger.info(f"Checking financial bounds & solvency ratios on {table_name}...")
        negative_incomes = df.filter(F.col("AMT_INCOME_TOTAL") <= 0).count()
        negative_credits = df.filter(F.col("AMT_CREDIT") <= 0).count()
        negative_dti = df.filter(F.col("debt_to_income_ratio") < 0).count()
        negative_ltv = df.filter(F.col("loan_to_value_ratio") < 0).count()

        all_passed = True
        for name, count, col_name in [
            ("amt_income_positive", negative_incomes, "AMT_INCOME_TOTAL"),
            ("amt_credit_positive", negative_credits, "AMT_CREDIT"),
            ("debt_to_income_non_negative", negative_dti, "debt_to_income_ratio"),
            ("loan_to_value_non_negative", negative_ltv, "loan_to_value_ratio"),
        ]:
            passed = count == 0
            if not passed:
                all_passed = False
            self.results.append(
                QualityCheckResult(
                    check_name=name,
                    target_table=table_name,
                    column=col_name,
                    severity=CheckSeverity.CRITICAL,
                    passed=passed,
                    details=f"Found {count:,} non-positive / invalid records",
                    metric_value=count,
                    threshold=0,
                )
            )
        return all_passed

    def check_ext_source_boundaries(self, df: DataFrame, table_name: str) -> bool:
        """Verifies that external normalized risk scores strictly respect the [0.0, 1.0] interval."""
        logger.info(f"Checking normalized EXT_SOURCE score intervals on {table_name}...")
        all_passed = True
        for score_col in ["EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "ext_source_mean"]:
            out_of_bounds = df.filter(
                (F.col(score_col).isNotNull()) & ((F.col(score_col) < 0.0) | (F.col(score_col) > 1.0))
            ).count()
            passed = out_of_bounds == 0
            if not passed:
                all_passed = False
            self.results.append(
                QualityCheckResult(
                    check_name=f"{score_col.lower()}_probability_bounds",
                    target_table=table_name,
                    column=score_col,
                    severity=CheckSeverity.CRITICAL,
                    passed=passed,
                    details=f"Found {out_of_bounds} values outside [0.0, 1.0]",
                    metric_value=out_of_bounds,
                    threshold=0,
                )
            )
        return all_passed

    def check_feature_store_readiness(self, df: DataFrame, table_name: str) -> bool:
        """Verifies prerequisites for the Feast Feature Store (event_timestamp, column count)."""
        logger.info(f"Checking Feast Feature Store contract readiness on {table_name}...")
        col_count = len(df.columns)
        has_timestamp = "event_timestamp" in df.columns
        null_timestamps = df.filter(F.col("event_timestamp").isNull()).count() if has_timestamp else -1

        # Check timestamp
        ts_passed = has_timestamp and (null_timestamps == 0)
        self.results.append(
            QualityCheckResult(
                check_name="feast_event_timestamp_valid",
                target_table=table_name,
                column="event_timestamp",
                severity=CheckSeverity.CRITICAL,
                passed=ts_passed,
                details="event_timestamp present and non-null" if ts_passed else "event_timestamp missing or contains nulls",
                metric_value=null_timestamps,
                threshold=0,
            )
        )

        # Check column count >= 140
        cols_passed = col_count >= 140
        self.results.append(
            QualityCheckResult(
                check_name="feature_count_threshold",
                target_table=table_name,
                column=None,
                severity=CheckSeverity.CRITICAL,
                passed=cols_passed,
                details=f"Gold table has {col_count} columns (required >= 140)",
                metric_value=col_count,
                threshold=140,
            )
        )
        return ts_passed and cols_passed

    def run_all_checks(self, fail_on_critical: bool = True) -> bool:
        """Executes the full suite of data quality circuit breakers and writes an audit report."""
        logger.info("=== Starting Data Quality Circuit Breaker Audit ===")
        spark = get_spark_session("HomeCredit_Circuit_Breaker")

        gold_path = str(self.lakehouse_root / "gold" / "customer_features")
        logger.info(f"Loading Gold Feature Table from: {gold_path}...")
        gold_df = spark.read.format("delta").load(gold_path)

        # Execute check suite
        self.check_primary_key(gold_df, "gold_customer_features", "SK_ID_CURR")
        self.check_sentinel_leakage(gold_df, "gold_customer_features")
        self.check_target_distribution(gold_df, "gold_customer_features")
        self.check_financial_bounds(gold_df, "gold_customer_features")
        self.check_ext_source_boundaries(gold_df, "gold_customer_features")
        self.check_feature_store_readiness(gold_df, "gold_customer_features")

        # Compile audit summary
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.passed)
        failed_critical = sum(1 for r in self.results if not r.passed and r.severity == CheckSeverity.CRITICAL)
        failed_warnings = sum(1 for r in self.results if not r.passed and r.severity == CheckSeverity.WARNING)

        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        report_path = self.reports_dir / f"circuit_breaker_{timestamp_str}.json"

        report_payload = {
            "audit_timestamp": datetime.now(timezone.utc).isoformat(),
            "target_table": "gold/customer_features",
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_critical": failed_critical,
            "failed_warnings": failed_warnings,
            "overall_status": "PASSED" if failed_critical == 0 else "FAILED",
            "checks": [asdict(r) for r in self.results],
        }

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report_payload, f, indent=2)

        logger.info(f"Audit report saved to: {report_path}")
        logger.info(
            f"=== Quality Audit Results: {passed_checks}/{total_checks} checks passed. "
            f"(Critical Failures: {failed_critical}, Warnings: {failed_warnings}) ==="
        )

        spark.stop()

        if failed_critical > 0 and fail_on_critical:
            raise RuntimeError(
                f"Data Quality Circuit Breaker TRIPPED! {failed_critical} critical checks failed. "
                f"Aborting downstream pipeline. See report: {report_path}"
            )

        return failed_critical == 0


if __name__ == "__main__":
    breaker = DataQualityCircuitBreaker()
    breaker.run_all_checks(fail_on_critical=True)
