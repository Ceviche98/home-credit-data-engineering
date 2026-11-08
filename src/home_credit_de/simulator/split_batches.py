"""
Temporal Source Simulator for Home Credit Lakehouse.

Splits static Kaggle CSVs into 4 sequential delivery batches:
  - batch_0_history: Baseline history (older than 3 months, <= 2025-09)
  - batch_1_2025_10: Month -3 drop (October 2025)
  - batch_2_2025_11: Month -2 drop (November 2025)
  - batch_3_2025_12: Month -1/0 drop (December 2025)

Principles:
  - Streaming chunk-based processing to avoid Out-Of-Memory (OOM) on large tables.
  - Deterministic batch assignment.
  - Generates manifest metadata tracking row counts per batch.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Batch folder names
BATCHES = [
    "batch_0_history",
    "batch_1_2025_10",
    "batch_2_2025_11",
    "batch_3_2025_12",
]


def determine_batch_by_month(month: float) -> str:
    """Assigns batch based on MONTHS_BALANCE (-96 to 0)."""
    if pd.isna(month) or month <= -4:
        return "batch_0_history"
    elif month == -3:
        return "batch_1_2025_10"
    elif month == -2:
        return "batch_2_2025_11"
    else:  # -1 or 0
        return "batch_3_2025_12"


def determine_batch_by_days(days: float) -> str:
    """Assigns batch based on relative days offset (e.g. DAYS_INSTALMENT, DAYS_CREDIT)."""
    if pd.isna(days) or days <= -91:
        return "batch_0_history"
    elif -90 <= days <= -61:
        return "batch_1_2025_10"
    elif -60 <= days <= -31:
        return "batch_2_2025_11"
    else:
        return "batch_3_2025_12"


def split_table(
    input_csv: Path,
    output_dir: Path,
    time_col: str,
    time_type: str,
    chunk_size: int = 100_000,
    limit_chunks: int | None = None,
) -> dict[str, int]:
    """
    Streams through a large CSV in chunks and writes rows to the corresponding batch CSV.
    """
    table_name = input_csv.name
    logger.info(f"Processing {table_name} (time column: {time_col}, type: {time_type})...")

    # Prepare output directories and track row counts
    counts: dict[str, int] = {b: 0 for b in BATCHES}
    first_write: dict[str, bool] = {b: True for b in BATCHES}

    for b in BATCHES:
        (output_dir / b).mkdir(parents=True, exist_ok=True)

    reader = pd.read_csv(input_csv, chunksize=chunk_size, low_memory=False)

    for chunk_idx, chunk in enumerate(reader):
        if limit_chunks and chunk_idx >= limit_chunks:
            logger.info(f"Reached dry-run limit of {limit_chunks} chunks for {table_name}.")
            break

        # Route chunk by time logic
        if time_type == "month":
            batch_assignments = chunk[time_col].apply(determine_batch_by_month)
        elif time_type == "day":
            batch_assignments = chunk[time_col].apply(determine_batch_by_days)
        elif time_type == "application":
            # For applications: 90% to batch_0, and ~3.33% to each of the 3 incremental batches
            # Use deterministic hash of SK_ID_CURR
            hash_mod = chunk["SK_ID_CURR"] % 100
            batch_assignments = hash_mod.apply(
                lambda m: "batch_0_history"
                if m < 90
                else ("batch_1_2025_10" if m < 93 else ("batch_2_2025_11" if m < 96 else "batch_3_2025_12"))
            )
        else:
            raise ValueError(f"Unknown time_type: {time_type}")

        # Group and write out
        for batch_name in BATCHES:
            sub_df = chunk[batch_assignments == batch_name]
            if len(sub_df) == 0:
                continue

            target_file = output_dir / batch_name / table_name
            mode = "w" if first_write[batch_name] else "a"
            header = first_write[batch_name]

            sub_df.to_csv(target_file, mode=mode, header=header, index=False)
            first_write[batch_name] = False
            counts[batch_name] += len(sub_df)

        if (chunk_idx + 1) % 10 == 0:
            logger.info(f"  Processed {(chunk_idx + 1) * chunk_size:,} rows for {table_name}...")

    logger.info(f"Completed {table_name}: {counts}")
    return counts


def run_simulator(
    source_dir: Path,
    output_dir: Path,
    chunk_size: int = 100_000,
    dry_run: bool = False,
):
    """Orchestrates splitting for all 7 Home Credit tables."""
    output_dir.mkdir(parents=True, exist_ok=True)
    limit = 2 if dry_run else None  # In dry run, only process 2 chunks (200k rows) per table

    # Mapping of tables to their primary time column and routing rule
    table_configs = [
        ("application_train.csv", "SK_ID_CURR", "application"),
        ("application_test.csv", "SK_ID_CURR", "application"),
        ("bureau.csv", "DAYS_CREDIT", "day"),
        ("bureau_balance.csv", "MONTHS_BALANCE", "month"),
        ("previous_application.csv", "DAYS_DECISION", "day"),
        ("POS_CASH_balance.csv", "MONTHS_BALANCE", "month"),
        ("installments_payments.csv", "DAYS_INSTALMENT", "day"),
        ("credit_card_balance.csv", "MONTHS_BALANCE", "month"),
    ]

    total_manifest: dict[str, dict[str, int]] = {}

    for file_name, time_col, time_type in table_configs:
        input_path = source_dir / file_name
        if not input_path.exists():
            logger.warning(f"File {input_path} not found, skipping...")
            continue

        counts = split_table(
            input_csv=input_path,
            output_dir=output_dir,
            time_col=time_col,
            time_type=time_type,
            chunk_size=chunk_size,
            limit_chunks=limit,
        )
        total_manifest[file_name] = counts

    # Write summary manifest JSON
    manifest_path = output_dir / "simulator_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(total_manifest, f, indent=2)

    logger.info(f"All tables split successfully! Manifest written to: {manifest_path}")


def main():
    parser = argparse.ArgumentParser(description="Home Credit Temporal Batch Simulator")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("home-credit-default-risk"),
        help="Path to unzipped raw Kaggle CSVs",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw_batches"),
        help="Target folder for batch drops",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Number of rows per chunk for memory management",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Quick test on 200k rows instead of the full dataset",
    )

    args = parser.parse_args()
    run_simulator(
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
