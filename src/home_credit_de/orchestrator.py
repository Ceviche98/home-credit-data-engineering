"""
Master Lakehouse Orchestrator Pipeline: End-to-End Enterprise Automation.

Executes the complete Medallion + Feature Store workflow:
  Stage 1: Azure Landing Upload (Idempotent 4MB staging + SHA-256 verification)
  Stage 2: Bronze Delta Ingestion (Partitioned Delta + Ingestion Ledger)
  Stage 3: Silver Refinery (Anomaly fixing, Deduplication via MERGE INTO, Z-Order)
  Stage 4: Gold Feature Mart (152 dimensional credit risk features)
  Stage 5: Quality Circuit Breaker (15 critical banking invariants)
  Stage 6: Feast Feature Store Sync (Redis Online Store for <1ms inference)
"""

import argparse
import logging
import time
from pathlib import Path

from home_credit_de.feature_store.feast_manager import HomeCreditFeatureStoreManager
from home_credit_de.ingestion.azure_uploader import upload_batch
from home_credit_de.ingestion.bronze_loader import ingest_batch_to_bronze
from home_credit_de.quality.circuit_breaker import DataQualityCircuitBreaker
from home_credit_de.transformation.gold_materializer import materialize_gold_features
from home_credit_de.transformation.silver_processor import process_silver_layer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class LakehouseOrchestrator:
    def __init__(
        self,
        lakehouse_root: Path = Path("data/lakehouse"),
        raw_batches_root: Path = Path("data/raw_batches"),
    ):
        self.lakehouse_root = lakehouse_root
        self.raw_batches_root = raw_batches_root

    def run_pipeline(
        self,
        batch_id: str,
        sync_feast: bool = True,
        simulate_chaos_drop: int | None = None,
    ) -> dict[str, float]:
        """
        Executes the end-to-end pipeline for the specified batch.
        Returns execution stage timings in seconds.
        """
        pipeline_start = time.perf_counter()
        logger.info("============================================================")
        logger.info(f"   STARTING MASTER LAKEHOUSE PIPELINE FOR BATCH: {batch_id}   ")
        logger.info("============================================================")
        
        timings: dict[str, float] = {}

        # ----------------------------------------------------------------------
        # STAGE 1: Azure Landing Ingestion
        # ----------------------------------------------------------------------
        t0 = time.perf_counter()
        logger.info(">>> [STAGE 1/6] Uploading Batch to Azure Landing (Azurite)...")
        batch_folder = self.raw_batches_root / batch_id
        if not batch_folder.exists():
            raise FileNotFoundError(f"Batch folder not found: {batch_folder}")

        upload_batch(batch_folder, simulate_fail_file_idx=simulate_chaos_drop)
        timings["stage_1_landing_upload"] = time.perf_counter() - t0

        # ----------------------------------------------------------------------
        # STAGE 2: Bronze Delta Ingestion (with Ingestion Ledger)
        # ----------------------------------------------------------------------
        t0 = time.perf_counter()
        logger.info(">>> [STAGE 2/6] Ingesting into Bronze Delta Lake (ACID Ledger)...")
        ingest_batch_to_bronze(batch_dir=batch_folder, lakehouse_root=self.lakehouse_root)
        timings["stage_2_bronze_ingestion"] = time.perf_counter() - t0

        # ----------------------------------------------------------------------
        # STAGE 3: Silver Refinery (MERGE INTO Upserts & Z-Ordering)
        # ----------------------------------------------------------------------
        t0 = time.perf_counter()
        logger.info(">>> [STAGE 3/6] Refining Silver Layer (MERGE INTO & Z-Order)...")
        process_silver_layer(lakehouse_root=self.lakehouse_root)
        timings["stage_3_silver_refinery"] = time.perf_counter() - t0

        # ----------------------------------------------------------------------
        # STAGE 4: Gold Feature Mart Materialization (152 Features)
        # ----------------------------------------------------------------------
        t0 = time.perf_counter()
        logger.info(">>> [STAGE 4/6] Materializing 152 Features into Gold Delta Table...")
        materialize_gold_features(lakehouse_root=self.lakehouse_root)
        timings["stage_4_gold_mart"] = time.perf_counter() - t0

        # ----------------------------------------------------------------------
        # STAGE 5: Automated Data Quality Circuit Breaker
        # ----------------------------------------------------------------------
        t0 = time.perf_counter()
        logger.info(">>> [STAGE 5/6] Running Data Quality Circuit Breaker Gates...")
        breaker = DataQualityCircuitBreaker(lakehouse_root=self.lakehouse_root)
        breaker.run_all_checks(fail_on_critical=True)
        timings["stage_5_circuit_breaker"] = time.perf_counter() - t0

        # ----------------------------------------------------------------------
        # STAGE 6: Feast Dual Feature Store Sync (Redis Online Store)
        # ----------------------------------------------------------------------
        if sync_feast:
            t0 = time.perf_counter()
            logger.info(">>> [STAGE 6/6] Syncing Gold Features to Redis Online Store...")
            feast_mgr = HomeCreditFeatureStoreManager()
            feast_mgr.sync_to_redis(batch_size=5000)
            timings["stage_6_feast_redis_sync"] = time.perf_counter() - t0

        total_elapsed = time.perf_counter() - pipeline_start
        timings["total_pipeline_duration"] = total_elapsed

        logger.info("============================================================")
        logger.info(f"   MASTER PIPELINE COMPLETED SUCCESSFULLY IN {total_elapsed:.2f}s!   ")
        logger.info("============================================================")
        for stage, duration in timings.items():
            logger.info(f"  * {stage}: {duration:.2f}s")

        return timings


def main():
    parser = argparse.ArgumentParser(description="Home Credit Master Lakehouse Orchestrator")
    parser.add_argument("--batch", type=str, default="batch_2_2025_11", help="Batch ID to orchestrate")
    parser.add_argument("--no-feast", action="store_true", help="Skip Feast online store sync")
    parser.add_argument("--chaos-drop", type=int, default=None, help="Simulate crash on Nth file index")
    args = parser.parse_args()

    orchestrator = LakehouseOrchestrator()
    orchestrator.run_pipeline(
        batch_id=args.batch,
        sync_feast=not args.no_feast,
        simulate_chaos_drop=args.chaos_drop,
    )


if __name__ == "__main__":
    main()
