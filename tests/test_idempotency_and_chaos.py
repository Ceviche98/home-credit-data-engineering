"""
Integration & Chaos Recovery Tests: Idempotency, SHA-256 Invariants, and Chunked Uploads.
"""

import hashlib
import tempfile
import time
from pathlib import Path

import pytest

from home_credit_de.ingestion.azure_uploader import (
    compute_file_sha256,
    get_blob_service_client,
    upload_batch,
    upload_single_file_chunked,
)


def test_sha256_computation_deterministic():
    """Verifies SHA-256 checksum is mathematically deterministic in binary mode."""
    content = b"HomeCredit_Data_Engineering_Sample_Line_1\nLine_2\n"
    with tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)

    expected_hash = hashlib.sha256(content).hexdigest()
    actual_hash = compute_file_sha256(tmp_path)
    assert actual_hash == expected_hash
    tmp_path.unlink()


def test_azure_uploader_idempotency():
    """
    Verifies that re-uploading an already uploaded batch skips redundant uploads.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        unique_batch_name = f"idempotent_test_batch_{time.time_ns()}"
        batch_dir = Path(temp_dir) / unique_batch_name
        batch_dir.mkdir(parents=True)
        (batch_dir / "table_a.csv").write_bytes(b"col1,col2\n1,2\n3,4\n")
        (batch_dir / "table_b.csv").write_bytes(b"col1,col2\n5,6\n7,8\n")

        # 1. First upload (all new files -> uploaded)
        result_1 = upload_batch(batch_dir, container_name="test-landing")
        assert result_1["files_count"] == 2
        assert result_1["manifest"]["table_a.csv"]["status"] == "uploaded"
        assert result_1["manifest"]["table_b.csv"]["status"] == "uploaded"

        # 2. Second upload (idempotent run -> skipped)
        result_2 = upload_batch(batch_dir, container_name="test-landing")
        assert result_2["files_count"] == 2
        assert result_2["manifest"]["table_a.csv"]["status"] == "skipped"
        assert result_2["manifest"]["table_b.csv"]["status"] == "skipped"


def test_chaos_interrupted_upload_and_recovery():
    """
    Verifies that a mid-upload crash cleanly aborts without committing uncommitted blocks,
    and subsequent retry resumes and succeeds without corrupted state.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        test_file = Path(temp_dir) / "large_payload.csv"
        # Create a file > 4MB (5MB payload to test 2 blocks)
        chunk_4mb = b"0" * (4 * 1024 * 1024)
        test_file.write_bytes(chunk_4mb + b"1" * (1024 * 1024))

        service_client = get_blob_service_client()
        container_name = "test-landing"
        blob_path = f"chaos_test_{time.time_ns()}/large_payload.csv"
        blob_client = service_client.get_blob_client(container=container_name, blob=blob_path)

        # 1. Simulate crash at block 1 (0-indexed second block)
        with pytest.raises(ConnectionResetError, match="Simulated network disruption"):
            upload_single_file_chunked(test_file, blob_client, simulate_fail_at_block=1)

        # Verify blob was NOT committed in Azure Landing
        assert not blob_client.exists()

        # 2. Recovery: Re-upload without simulation
        result = upload_single_file_chunked(test_file, blob_client)
        assert result["status"] == "uploaded"
        assert blob_client.exists()
        assert blob_client.get_blob_properties().size == test_file.stat().st_size
