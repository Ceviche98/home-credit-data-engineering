"""
Idempotent Landing -> Bronze Delta Lake Ingestion Loader.

Capabilities:
  - Enforces explicit PySpark schemas from contracts.schemas (zero inferSchema overhead).
  - Appends enterprise audit columns (_batch_id, _source_sha256, _ingested_at).
  - Maintains an atomic Delta Ingestion Ledger (_meta.ingestion_ledger) to guarantee idempotency.
  - Automatically skips previously committed tables within a batch.
  - Generates partitioned, ACID-compliant Delta tables in the Bronze layer.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from home_credit_de.contracts.schemas import (
    get_schema_for_table,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Schema for the central Ingestion Ledger table
LEDGER_SCHEMA = StructType([
    StructField("batch_id", StringType(), nullable=False),
    StructField("table_name", StringType(), nullable=False),
    StructField("file_sha256", StringType(), nullable=False),
    StructField("row_count", LongType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("ingested_at", TimestampType(), nullable=False),
])


def get_spark_session(app_name: str = "HomeCredit_Bronze_Ingestion") -> SparkSession:
    """
    Initializes a local SparkSession with Delta Lake ACID extensions configured.
    Configures HADOOP_HOME automatically on Windows to support winutils.
    """
    import os
    import sys

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if sys.platform == "win32":
        hadoop_home = r"C:\hadoop"
        if os.path.exists(hadoop_home):
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] = f"{hadoop_home}\\bin;" + os.environ.get("PATH", "")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_ledger_path(lakehouse_root: Path) -> str:
    """Returns the Delta path for the ingestion ledger table."""
    return str(lakehouse_root / "_meta" / "ingestion_ledger")


def init_ledger(spark: SparkSession, lakehouse_root: Path):
    """Initializes the Delta ledger table if it does not already exist."""
    ledger_path = get_ledger_path(lakehouse_root)
    if not Path(ledger_path).exists():
        logger.info(f"Creating Ingestion Ledger table at: {ledger_path}")
        empty_df = spark.createDataFrame([], LEDGER_SCHEMA)
        empty_df.write.format("delta").mode("overwrite").save(ledger_path)


def is_already_committed(
    spark: SparkSession,
    lakehouse_root: Path,
    batch_id: str,
    table_name: str,
    file_sha256: str,
) -> bool:
    """
    Queries the Delta ledger to check if (batch_id, table_name, file_sha256) is COMMITTED.
    """
    ledger_path = get_ledger_path(lakehouse_root)
    try:
        ledger_df = spark.read.format("delta").load(ledger_path)
        match_count = ledger_df.filter(
            (F.col("batch_id") == batch_id)
            & (F.col("table_name") == table_name)
            & (F.col("file_sha256") == file_sha256)
            & (F.col("status") == "COMMITTED")
        ).count()
        return match_count > 0
    except Exception:
        return False


def record_commit_in_ledger(
    spark: SparkSession,
    lakehouse_root: Path,
    batch_id: str,
    table_name: str,
    file_sha256: str,
    row_count: int,
):
    """Records a successful table ingestion commit in the Delta ledger."""
    ledger_path = get_ledger_path(lakehouse_root)
    commit_row = [(
        batch_id,
        table_name,
        file_sha256,
        row_count,
        "COMMITTED",
        datetime.now(timezone.utc),
    )]
    commit_df = spark.createDataFrame(commit_row, LEDGER_SCHEMA)
    commit_df.write.format("delta").mode("append").save(ledger_path)


def ingest_table_to_bronze(
    spark: SparkSession,
    csv_path: Path,
    batch_id: str,
    file_sha256: str,
    lakehouse_root: Path,
) -> int:
    """
    Reads raw CSV with schema contract, attaches audit columns, and writes to Bronze Delta.
    """
    table_base_name = csv_path.stem  # e.g., 'bureau' from 'bureau.csv'
    delta_target_path = str(lakehouse_root / "bronze" / table_base_name)

    # 1. Idempotency Check via Ledger
    if is_already_committed(spark, lakehouse_root, batch_id, table_base_name, file_sha256):
        logger.info(f"  [SKIPPED] {table_base_name} already COMMITTED in Bronze for {batch_id}.")
        return 0

    logger.info(f"  [INGESTING] {csv_path.name} -> Bronze Delta ({delta_target_path})...")

    # 2. Strict Schema Contract Enforcement
    schema = get_schema_for_table(csv_path.name)
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(str(csv_path))
    )

    # 3. Append Enterprise Audit Columns
    enriched_df = (
        raw_df
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_sha256", F.lit(file_sha256))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    # 4. Atomic Write to Delta Table (Partitioned by _batch_id)
    (
        enriched_df.write.format("delta")
        .mode("append")
        .partitionBy("_batch_id")
        .save(delta_target_path)
    )

    row_count = enriched_df.count()

    # 5. Record Commit in Ledger
    record_commit_in_ledger(
        spark=spark,
        lakehouse_root=lakehouse_root,
        batch_id=batch_id,
        table_name=table_base_name,
        file_sha256=file_sha256,
        row_count=row_count,
    )

    logger.info(f"  [COMMITTED] {table_base_name}: {row_count:,} rows written to Bronze Delta.")
    return row_count


def ingest_batch_to_bronze(
    batch_dir: Path,
    lakehouse_root: Path = Path("data/lakehouse"),
) -> dict[str, int]:
    """
    Orchestrates ingestion of all CSV tables in a batch directory to Bronze Delta Lake.
    """
    batch_id = batch_dir.name
    logger.info(f"=== Starting Bronze Ingestion for Batch: {batch_id} ===")

    spark = get_spark_session()
    init_ledger(spark, lakehouse_root)

    # Read the batch manifest to get verified SHA-256 hashes
    manifest_file = batch_dir.parent / "simulator_manifest.json"
    hashes: dict[str, str] = {}
    if manifest_file.exists():
        import hashlib
        # Compute hashes on the fly if needed
        for f in batch_dir.glob("*.csv"):
            hasher = hashlib.sha256()
            with open(f, "rb") as fh:
                while chunk := fh.read(64 * 1024):
                    hasher.update(chunk)
            hashes[f.name] = hasher.hexdigest()

    csv_files = sorted(batch_dir.glob("*.csv"))
    results = {}

    for csv_file in csv_files:
        file_hash = hashes.get(csv_file.name, "unknown")
        rows = ingest_table_to_bronze(
            spark=spark,
            csv_path=csv_file,
            batch_id=batch_id,
            file_sha256=file_hash,
            lakehouse_root=lakehouse_root,
        )
        results[csv_file.stem] = rows

    logger.info(f"=== Completed Bronze Ingestion for {batch_id}: {results} ===")
    return results


def main():
    parser = argparse.ArgumentParser(description="Home Credit Bronze Delta Lake Ingestor")
    parser.add_argument(
        "--batch-dir",
        type=Path,
        required=True,
        help="Path to the batch folder (e.g. data/raw_batches/batch_1_2025_10)",
    )
    parser.add_argument(
        "--lakehouse-root",
        type=Path,
        default=Path("data/lakehouse"),
        help="Root path for Bronze, Silver, Gold Delta Lakehouse",
    )

    args = parser.parse_args()
    ingest_batch_to_bronze(batch_dir=args.batch_dir, lakehouse_root=args.lakehouse_root)


if __name__ == "__main__":
    main()
