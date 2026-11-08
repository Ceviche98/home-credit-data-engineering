"""
Silver Layer Processor: Distributed Cleaning, Entity Resolution & Optimization.

Capabilities:
  - Fixes legacy banking anomalies (DAYS_EMPLOYED = 365243 -> NULL + flag).
  - Resolves 27.2M-row bureau_balance join with bureau to attach SK_ID_CURR.
  - Computes payment-level behavioral metrics (days late, underpayment amount).
  - Enforces deterministic primary keys and idempotency via Delta MERGE INTO.
  - Applies physical Z-Ordering on SK_ID_CURR for sub-second data skipping.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from home_credit_de.ingestion.bronze_loader import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Sentinel value used by legacy banking systems for retired/unemployed
SENTINEL_EMPLOYED_DAYS = 365243


def clean_application_data(df: DataFrame) -> DataFrame:
    """
    Cleans application train/test data:
      - Isolates DAYS_EMPLOYED anomaly into IS_PENSIONER_OR_UNEMPLOYED boolean.
      - Calculates AGE_YEARS and YEARS_EMPLOYED.
      - Imputes clean NULLs where legacy sentinel codes existed.
    """
    return (
        df
        .withColumn(
            "IS_PENSIONER_OR_UNEMPLOYED",
            F.when(F.col("DAYS_EMPLOYED") == SENTINEL_EMPLOYED_DAYS, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "DAYS_EMPLOYED_CLEAN",
            F.when(F.col("DAYS_EMPLOYED") == SENTINEL_EMPLOYED_DAYS, F.lit(None)).otherwise(F.col("DAYS_EMPLOYED"))
        )
        .withColumn("AGE_YEARS", F.round(-F.col("DAYS_BIRTH") / 365.25, 1))
        .withColumn(
            "YEARS_EMPLOYED",
            F.when(F.col("DAYS_EMPLOYED_CLEAN").isNotNull(), F.round(-F.col("DAYS_EMPLOYED_CLEAN") / 365.25, 1))
            .otherwise(F.lit(0.0))
        )
    )


def clean_previous_applications(df: DataFrame) -> DataFrame:
    """
    Cleans previous_application date columns where 365243 was used as a placeholder.
    """
    date_cols = [
        "DAYS_FIRST_DRAWING",
        "DAYS_FIRST_DUE",
        "DAYS_LAST_DUE_1ST_VERSION",
        "DAYS_LAST_DUE",
        "DAYS_TERMINATION",
    ]
    cleaned_df = df
    for col_name in date_cols:
        cleaned_df = cleaned_df.withColumn(
            col_name,
            F.when(F.col(col_name) == SENTINEL_EMPLOYED_DAYS, F.lit(None)).otherwise(F.col(col_name))
        )
    return cleaned_df


def enrich_bureau_balance(bureau_balance_df: DataFrame, bureau_df: DataFrame) -> DataFrame:
    """
    Resolves the 27.2M-row join: Attaches SK_ID_CURR from bureau onto bureau_balance.
    Applies column pruning to avoid memory pressure during shuffle.
    """
    # Select only required columns from bureau to minimize network transfer
    bureau_lookup = bureau_df.select("SK_ID_BUREAU", "SK_ID_CURR").dropDuplicates(["SK_ID_BUREAU"])
    
    # Broadcast join if lookup is optimized, or distributed hash join
    return bureau_balance_df.join(
        F.broadcast(bureau_lookup) if bureau_lookup.count() < 2_000_000 else bureau_lookup,
        on="SK_ID_BUREAU",
        how="inner"
    )


def clean_installments(df: DataFrame) -> DataFrame:
    """
    Computes payment performance metrics per installment row:
      - DAYS_LATE: Positive if payment made after due date.
      - AMT_UNDERPAYMENT: Positive if paid less than required installment amount.
      - INSTALLMENT_ID: Deterministic surrogate key for idempotent Delta MERGE.
    """
    return (
        df
        .withColumn(
            "DAYS_LATE",
            F.when(
                F.col("DAYS_ENTRY_PAYMENT").isNotNull() & F.col("DAYS_INSTALMENT").isNotNull(),
                F.greatest(F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT"), F.lit(0.0))
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "AMT_UNDERPAYMENT",
            F.when(
                F.col("AMT_INSTALMENT").isNotNull() & F.col("AMT_PAYMENT").isNotNull(),
                F.greatest(F.col("AMT_INSTALMENT") - F.col("AMT_PAYMENT"), F.lit(0.0))
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "INSTALLMENT_ID",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("SK_ID_PREV"),
                    F.col("NUM_INSTALMENT_VERSION"),
                    F.col("NUM_INSTALMENT_NUMBER"),
                    F.coalesce(F.col("DAYS_INSTALMENT").cast("string"), F.lit("")),
                    F.coalesce(F.col("DAYS_ENTRY_PAYMENT").cast("string"), F.lit("")),
                    F.coalesce(F.col("AMT_PAYMENT").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
    )


def upsert_delta_table(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: Path,
    primary_keys: list[str],
    zorder_col: str = "SK_ID_CURR",
):
    """
    Performs an atomic Delta MERGE INTO (Upsert) on primary_keys,
    and applies Z-Ordering for data skipping.
    """
    target_path_str = str(target_path)
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)

    # In Delta MERGE INTO, source table must have at most 1 matching row per target row.
    # Deduplicate source by primary_keys to strictly uphold SQL MERGE semantics.
    source_deduped = source_df.dropDuplicates(primary_keys)

    if not DeltaTable.isDeltaTable(spark, target_path_str):
        logger.info(f"  Creating initial Silver Delta table at: {target_path_str}")
        (
            source_deduped.write.format("delta")
            .mode("overwrite")
            .save(target_path_str)
        )
    else:
        logger.info(f"  Executing idempotent Delta MERGE INTO on: {target_path.name}...")
        target_delta = DeltaTable.forPath(spark, target_path_str)
        
        # Build merge condition: e.g. "target.SK_ID_CURR = source.SK_ID_CURR"
        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        
        (
            target_delta.alias("target")
            .merge(source_deduped.alias("source"), merge_condition)
            .whenMatchedUpdateAll(condition="source._ingested_at >= target._ingested_at")
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Physical optimization: Z-Order by client ID
    if zorder_col in source_df.columns:
        logger.info(f"  Running OPTIMIZE ZORDER BY ({zorder_col}) on {target_path.name}...")
        try:
            target_delta = DeltaTable.forPath(spark, target_path_str.replace("\\", "/"))
            target_delta.optimize().executeZOrderBy(zorder_col)
        except Exception as e:
            logger.warning(f"  OPTIMIZE ZORDER skipped for {target_path.name}: {e}")


def process_silver_layer(lakehouse_root: Path = Path("data/lakehouse")):
    """
    Processes all Bronze tables into Silver:
      - Cleans anomalies.
      - Resolves entity relations.
      - Upserts into Silver Delta Lake.
      - Applies Z-Ordering.
    """
    logger.info("=== Starting Bronze -> Silver Layer Processing ===")
    spark = get_spark_session("HomeCredit_Silver_Refinery")
    bronze_dir = lakehouse_root / "bronze"
    silver_dir = lakehouse_root / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    # 1. Application Train & Test
    logger.info("1/7. Processing Silver Applications...")
    train_bronze = spark.read.format("delta").load(str(bronze_dir / "application_train"))
    test_bronze = spark.read.format("delta").load(str(bronze_dir / "application_test"))
    
    train_silver = clean_application_data(train_bronze)
    test_silver = clean_application_data(test_bronze)
    
    upsert_delta_table(spark, train_silver, silver_dir / "application_train", ["SK_ID_CURR"])
    upsert_delta_table(spark, test_silver, silver_dir / "application_test", ["SK_ID_CURR"])

    # 2. Bureau
    logger.info("2/7. Processing Silver Bureau...")
    bureau_bronze = spark.read.format("delta").load(str(bronze_dir / "bureau"))
    upsert_delta_table(spark, bureau_bronze, silver_dir / "bureau", ["SK_ID_BUREAU"])

    # 3. Bureau Balance (with 27.2M-row entity resolution join)
    logger.info("3/7. Processing Silver Bureau Balance (Resolving 27M rows join)...")
    bureau_balance_bronze = spark.read.format("delta").load(str(bronze_dir / "bureau_balance"))
    bureau_balance_silver = enrich_bureau_balance(bureau_balance_bronze, bureau_bronze)
    upsert_delta_table(
        spark,
        bureau_balance_silver,
        silver_dir / "bureau_balance",
        ["SK_ID_BUREAU", "MONTHS_BALANCE"],
        zorder_col="SK_ID_CURR",
    )

    # 4. Previous Applications
    logger.info("4/7. Processing Silver Previous Applications...")
    prev_bronze = spark.read.format("delta").load(str(bronze_dir / "previous_application"))
    prev_silver = clean_previous_applications(prev_bronze)
    upsert_delta_table(spark, prev_silver, silver_dir / "previous_application", ["SK_ID_PREV"])

    # 5. Installments Payments
    logger.info("5/7. Processing Silver Installments Payments...")
    inst_bronze = spark.read.format("delta").load(str(bronze_dir / "installments_payments"))
    inst_silver = clean_installments(inst_bronze)
    upsert_delta_table(
        spark,
        inst_silver,
        silver_dir / "installments_payments",
        ["INSTALLMENT_ID"],
        zorder_col="SK_ID_CURR",
    )

    # 6. POS CASH Balance
    logger.info("6/7. Processing Silver POS CASH Balance...")
    pos_bronze = spark.read.format("delta").load(str(bronze_dir / "POS_CASH_balance"))
    upsert_delta_table(
        spark,
        pos_bronze,
        silver_dir / "POS_CASH_balance",
        ["SK_ID_PREV", "MONTHS_BALANCE"],
        zorder_col="SK_ID_CURR",
    )

    # 7. Credit Card Balance
    logger.info("7/7. Processing Silver Credit Card Balance...")
    cc_bronze = spark.read.format("delta").load(str(bronze_dir / "credit_card_balance"))
    upsert_delta_table(
        spark,
        cc_bronze,
        silver_dir / "credit_card_balance",
        ["SK_ID_PREV", "MONTHS_BALANCE"],
        zorder_col="SK_ID_CURR",
    )

    logger.info("=== Silver Layer Processing & Optimization Complete! ===")
    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Home Credit Silver Layer Refinery")
    parser.add_argument(
        "--lakehouse-root",
        type=Path,
        default=Path("data/lakehouse"),
        help="Root path for Bronze, Silver, Gold Delta Lakehouse",
    )
    args = parser.parse_args()
    process_silver_layer(lakehouse_root=args.lakehouse_root)


if __name__ == "__main__":
    main()
