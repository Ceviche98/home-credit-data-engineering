"""
Unit Tests for Data Quality Circuit Breaker Rules.
"""

from home_credit_de.quality.circuit_breaker import CheckSeverity, QualityCheckResult


def test_quality_check_result_structure():
    """Verifies QualityCheckResult dataclass properties and serialization."""
    res = QualityCheckResult(
        check_name="primary_key_unique",
        target_table="gold_customer_features",
        column="SK_ID_CURR",
        severity=CheckSeverity.CRITICAL,
        passed=True,
        details="Found 0 duplicate primary keys",
        metric_value=0,
        threshold=0,
    )
    assert res.passed is True
    assert res.severity == CheckSeverity.CRITICAL
    assert res.metric_value == 0
