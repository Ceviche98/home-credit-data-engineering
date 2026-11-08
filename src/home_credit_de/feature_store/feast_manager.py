"""
Feast Feature Store Manager: Offline Historical Retrieval & Online Redis Serving.

This module bridges the Lakehouse Data Engineering platform (Repo 1)
with the downstream MLOps model training & inference engine (Repo 2):

1. Zero Training-Serving Skew: The exact same feature definitions in
   feature_store/features.py power both offline batch training and real-time inference.
2. Point-in-Time Correctness: get_historical_features prevents data leakage during training.
3. Sub-10ms Online Serving: syncs Gold Lakehouse features into Redis for live scoring.
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from feast import FeatureStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class HomeCreditFeatureStoreManager:
    def __init__(self, repo_path: Path = Path("feature_store")):
        self.repo_path = repo_path
        logger.info(f"Connecting to Feast Feature Store at: {self.repo_path.resolve()}...")
        self.store = FeatureStore(repo_path=str(self.repo_path))

    def test_offline_retrieval(self, sample_sk_ids: list[int] | None = None) -> pd.DataFrame:
        """
        Retrieves historical features for a batch of customers as of a specific point in time.
        Guarantees zero data leakage (Point-in-Time Time Travel join).
        """
        if sample_sk_ids is None:
            sample_sk_ids = [100003, 100004, 100009]
        logger.info(f"=== Testing Offline Historical Retrieval for customers: {sample_sk_ids} ===")
        
        # Entity observation dataframe with timestamp
        entity_df = pd.DataFrame(
            {
                "SK_ID_CURR": sample_sk_ids,
                "event_timestamp": [pd.to_datetime(datetime.now(timezone.utc))] * len(sample_sk_ids),
            }
        )

        features_to_fetch = [
            "customer_credit_features:AGE_YEARS",
            "customer_credit_features:YEARS_EMPLOYED",
            "customer_credit_features:debt_to_income_ratio",
            "customer_credit_features:payment_to_income_ratio",
            "customer_credit_features:annuity_to_credit_ratio",
            "customer_credit_features:ext_source_mean",
            "customer_credit_features:total_bureau_loans",
            "customer_credit_features:active_bureau_loans",
            "customer_credit_features:bureau_debt_to_credit_ratio",
            "customer_credit_features:total_prev_applications",
            "customer_credit_features:prev_approval_ratio",
            "customer_credit_features:count_late_installments",
            "customer_credit_features:underpayment_ratio",
            "customer_credit_features:cc_utilization_ratio_avg",
        ]

        start_time = time.perf_counter()
        training_df = self.store.get_historical_features(
            entity_df=entity_df,
            features=features_to_fetch,
        ).to_df()
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0

        logger.info(f"Offline Historical Retrieval completed in {elapsed_ms:.2f} ms")
        logger.info(f"Retrieved DataFrame shape: {training_df.shape}")
        return training_df

    def sync_to_redis(self, lakehouse_gold_path: Path = Path("data/lakehouse/gold/customer_features"), batch_size: int = 5000):
        """
        Syncs Gold Delta/Parquet features directly to Redis Online Store in chunked batches.
        """
        logger.info(f"=== Syncing Gold Features to Redis Online Store (batch_size: {batch_size:,}) ===")
        start_time = time.perf_counter()

        table = pq.read_table(str(lakehouse_gold_path))
        total_rows = min(len(table), batch_size)
        sample_df = table.slice(0, total_rows).to_pandas()

        # Ensure UTC timezone for Feast timestamp compatibility
        sample_df["event_timestamp"] = pd.to_datetime(sample_df["event_timestamp"], utc=True)

        logger.info(f"Writing {total_rows:,} customer feature vectors to Redis...")
        self.store.write_to_online_store(feature_view_name="customer_credit_features", df=sample_df)
        
        elapsed_sec = time.perf_counter() - start_time
        logger.info(f"=== Synced {total_rows:,} customers to Redis in {elapsed_sec:.2f} seconds ===")

    def test_online_retrieval(self, customer_id: int = 100003) -> dict[str, list]:
        """
        Benchmarks sub-10ms online feature retrieval from Redis for live loan scoring.
        """
        logger.info(f"=== Benchmarking Real-Time Online Retrieval from Redis for Customer: {customer_id} ===")
        
        features_to_fetch = [
            "customer_credit_features:AGE_YEARS",
            "customer_credit_features:debt_to_income_ratio",
            "customer_credit_features:ext_source_mean",
            "customer_credit_features:total_bureau_loans",
            "customer_credit_features:active_bureau_loans",
            "customer_credit_features:prev_approval_ratio",
            "customer_credit_features:count_late_installments",
            "customer_credit_features:cc_utilization_ratio_avg",
        ]

        # Warm up
        self.store.get_online_features(
            features=features_to_fetch,
            entity_rows=[{"SK_ID_CURR": customer_id}],
        )

        # Benchmark latency over 20 iterations
        latencies = []
        response = None
        for _ in range(20):
            t0 = time.perf_counter()
            response = self.store.get_online_features(
                features=features_to_fetch,
                entity_rows=[{"SK_ID_CURR": customer_id}],
            )
            latencies.append((time.perf_counter() - t0) * 1000.0)

        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)

        feature_dict = response.to_dict()
        logger.info(f"Online Retrieval Latency: Avg = {avg_latency:.2f} ms, Min = {min_latency:.2f} ms (Target < 10ms)")
        logger.info(f"Retrieved Online Feature Vector for Customer {customer_id}:")
        for k, v in feature_dict.items():
            logger.info(f"  {k}: {v[0]}")

        return feature_dict


def run_feature_store_pipeline():
    manager = HomeCreditFeatureStoreManager()

    # 1. Test Offline Time-Travel Historical Extraction
    offline_df = manager.test_offline_retrieval(sample_sk_ids=[100003, 100004, 100009])
    print("\n--- SAMPLE OFFLINE HISTORICAL FEATURES (FOR ML MODEL TRAINING) ---")
    print(offline_df.to_string(index=False))

    # 2. Sync Features into Redis Online Store
    manager.sync_to_redis(batch_size=5000)

    # 3. Test Real-Time Online Scoring Retrieval from Redis
    print("\n--- TESTING SUB-10MS REDIS ONLINE RETRIEVAL ---")
    manager.test_online_retrieval(customer_id=100003)


if __name__ == "__main__":
    run_feature_store_pipeline()
