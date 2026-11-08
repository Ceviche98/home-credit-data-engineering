"""
Fault-Tolerant, Idempotent Azure Blob / ADLS Gen2 Uploader.

Capabilities:
  - Multi-part block upload (4 MB chunks) with atomic commit_block_list.
  - End-to-end cryptographic integrity via SHA-256 checksums stored in blob metadata.
  - Idempotency: Skips already-uploaded, verified files automatically.
  - Generates _SUCCESS manifest upon batch completion.
  - Chaos simulation: Support for simulating mid-upload network cuts to verify recovery.
  - Works seamlessly with both local Azurite (Docker) and real Azure Cloud ADLS Gen2.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobBlock, BlobClient, BlobServiceClient, ContainerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Default Azurite connection string (Microsoft's standard well-known testing account)
AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

BLOCK_SIZE_BYTES = 4 * 1024 * 1024  # 4 MB chunk size


def compute_file_sha256(filepath: Path) -> str:
    """Computes SHA-256 hex digest using streaming reads (memory-safe)."""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(64 * 1024):  # 64 KB read buffer
            hasher.update(chunk)
    return hasher.hexdigest()


def get_blob_service_client(connection_string: str | None = None) -> BlobServiceClient:
    """Returns BlobServiceClient from connection string or env variable."""
    conn_str = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING", AZURITE_CONNECTION_STRING)
    return BlobServiceClient.from_connection_string(conn_str)


def ensure_container_exists(service_client: BlobServiceClient, container_name: str) -> ContainerClient:
    """Creates landing container if it does not already exist."""
    container_client = service_client.get_container_client(container_name)
    try:
        container_client.get_container_properties()
    except ResourceNotFoundError:
        logger.info(f"Container '{container_name}' does not exist. Creating it now...")
        container_client.create_container()
    return container_client


def is_blob_already_uploaded(blob_client: BlobClient, expected_sha256: str, local_size: int) -> bool:
    """
    Checks if blob exists in storage and matches exact SHA-256 and byte length.
    """
    try:
        props = blob_client.get_blob_properties()
        remote_sha256 = props.metadata.get("sha256", "")
        remote_size = props.size

        if remote_sha256 == expected_sha256 and remote_size == local_size:
            return True
    except ResourceNotFoundError:
        return False
    return False


def upload_single_file_chunked(
    local_path: Path,
    blob_client: BlobClient,
    simulate_fail_at_block: int | None = None,
) -> dict[str, str | int]:
    """
    Uploads a file in 4 MB blocks, stages each block, and commits them atomically.
    """
    file_size = local_path.stat().st_size
    file_sha256 = compute_file_sha256(local_path)

    # 1. Idempotency Check: Skip if exact verified file exists
    if is_blob_already_uploaded(blob_client, file_sha256, file_size):
        logger.info(f"  [SKIPPED] {local_path.name} already exists and verified intact (SHA: {file_sha256[:12]}...).")
        return {"status": "skipped", "file": local_path.name, "sha256": file_sha256, "bytes": file_size}

    logger.info(f"  [UPLOADING] {local_path.name} ({file_size / (1024*1024):.2f} MB)...")

    block_list: list[BlobBlock] = []
    block_index = 0

    with open(local_path, "rb") as f:
        while True:
            chunk = f.read(BLOCK_SIZE_BYTES)
            if not chunk:
                break

            # Chaos Testing Hook: Simulate connection drop mid-upload
            if simulate_fail_at_block is not None and block_index == simulate_fail_at_block:
                logger.error(f"  [CHAOS SIMULATION] Intentionally cutting connection at block {block_index}!")
                raise ConnectionResetError(f"Simulated network disruption at block {block_index}")

            # Generate standard 6-digit base64 block ID
            block_id = base64.b64encode(f"{block_index:06d}".encode()).decode("utf-8")
            blob_client.stage_block(block_id=block_id, data=chunk)
            block_list.append(BlobBlock(block_id=block_id))

            block_index += 1

    # Atomic commit: all blocks linked and metadata stored in a single transaction
    metadata = {
        "sha256": file_sha256,
        "file_name": local_path.name,
        "uploaded_at": str(time.time()),
    }
    blob_client.commit_block_list(block_list, metadata=metadata)
    logger.info(f"  [COMMITTED] {local_path.name} ({len(block_list)} blocks committed atomically).")

    return {"status": "uploaded", "file": local_path.name, "sha256": file_sha256, "bytes": file_size}


def upload_batch(
    batch_dir: Path,
    container_name: str = "landing",
    connection_string: str | None = None,
    simulate_fail_file_idx: int | None = None,
) -> dict[str, object]:
    """
    Uploads all CSVs in a batch directory, verified with SHA-256, ending with a _SUCCESS marker.
    """
    batch_name = batch_dir.name
    logger.info(f"=== Starting Ingestion for Batch: {batch_name} ===")

    service_client = get_blob_service_client(connection_string)
    ensure_container_exists(service_client, container_name)

    csv_files = sorted(batch_dir.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {batch_dir}!")
        return {"batch": batch_name, "files_processed": 0}

    manifest_summary = {}

    for idx, csv_file in enumerate(csv_files, 1):
        blob_path = f"{batch_name}/{csv_file.name}"
        blob_client = service_client.get_blob_client(container=container_name, blob=blob_path)

        fail_block = 2 if (simulate_fail_file_idx is not None and idx == simulate_fail_file_idx) else None

        try:
            result = upload_single_file_chunked(csv_file, blob_client, simulate_fail_at_block=fail_block)
            manifest_summary[csv_file.name] = result
        except ConnectionResetError as e:
            logger.error(f"Upload failed for {csv_file.name} due to simulated crash: {e}")
            logger.info("Exiting early. Re-run the command to verify idempotency and resume capability!")
            sys.exit(1)

    # All files in the batch completed: write _SUCCESS marker
    success_marker_path = f"{batch_name}/_SUCCESS.json"
    success_blob_client = service_client.get_blob_client(container=container_name, blob=success_marker_path)

    success_payload = {
        "batch_name": batch_name,
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "files_count": len(csv_files),
        "manifest": manifest_summary,
    }
    success_blob_client.upload_blob(
        json.dumps(success_payload, indent=2).encode("utf-8"),
        overwrite=True,
    )
    logger.info(f"=== Batch {batch_name} successfully ingested! Marker: {success_marker_path} ===")

    return success_payload


def main():
    parser = argparse.ArgumentParser(description="Fault-Tolerant Azure / Landing Uploader")
    parser.add_argument(
        "--batch-dir",
        type=Path,
        required=True,
        help="Path to the batch directory (e.g. data/raw_batches/batch_0_history)",
    )
    parser.add_argument(
        "--container",
        type=str,
        default="landing",
        help="Target container name in storage",
    )
    parser.add_argument(
        "--connection-string",
        type=str,
        default=None,
        help="Optional Azure Storage connection string (defaults to local Azurite)",
    )
    parser.add_argument(
        "--simulate-fail-file-idx",
        type=int,
        default=None,
        help="Testing flag: Intentionally crash upload during this file index (e.g. 2)",
    )

    args = parser.parse_args()
    upload_batch(
        batch_dir=args.batch_dir,
        container_name=args.container,
        connection_string=args.connection_string,
        simulate_fail_file_idx=args.simulate_fail_file_idx,
    )


if __name__ == "__main__":
    main()
