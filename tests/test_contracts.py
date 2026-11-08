"""
Unit Tests for Lakehouse Schemas and Data Contracts.
"""

from home_credit_de.contracts.schemas import (
    APPLICATION_TRAIN_SCHEMA,
    BRONZE_AUDIT_FIELDS,
    BUREAU_SCHEMA,
)


def test_schema_has_audit_columns():
    """Verifies that the standard audit fields contract contains lineage columns."""
    audit_field_names = [f.name for f in BRONZE_AUDIT_FIELDS]
    assert "_batch_id" in audit_field_names
    assert "_source_sha256" in audit_field_names
    assert "_ingested_at" in audit_field_names


def test_primary_keys_present():
    """Verifies primary/foreign key definitions exist in contracts."""
    app_fields = [f.name for f in APPLICATION_TRAIN_SCHEMA.fields]
    assert "SK_ID_CURR" in app_fields
    assert "TARGET" in app_fields

    bureau_fields = [f.name for f in BUREAU_SCHEMA.fields]
    assert "SK_ID_CURR" in bureau_fields
    assert "SK_ID_BUREAU" in bureau_fields
