from __future__ import annotations

import os
import sys

from pathlib import Path

from delta.tables import DeltaTable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


from src.common.spark_session import (
    get_spark_session,
)

from src.features.fraud_feature_assembly import (
    add_hybrid_fraud_rules,
    enrich_with_window_features,
)

# ============================================================
# NEW: STREAMING MONITORING
# ============================================================

from src.monitoring.streaming_metrics_listener import (
    attach_streaming_metrics_listener,
)


# ============================================================
# PATHS
# ============================================================

STATEFUL_FEATURE_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "stateful_transaction_features"
)

WINDOW_FEATURE_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "user_window_features"
)

LIVE_FEATURE_EVENT_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "fraud_feature_events"
)

LIVE_FEATURE_CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "checkpoints"
    / "fraud_feature_events"
)


# ============================================================
# SOURCE
# ============================================================

def read_stateful_stream(
    spark,
) -> DataFrame:

    return (
        spark.readStream
        .format(
            "delta"
        )
        .load(
            str(
                STATEFUL_FEATURE_PATH
            )
        )
    )


# ============================================================
# MICRO-BATCH FEATURE ASSEMBLY
# ============================================================

def write_live_feature_batch(
    batch_df: DataFrame,
    batch_id: int,
) -> None:

    if batch_df.isEmpty():
        return

    spark = (
        batch_df.sparkSession
    )

    if not DeltaTable.isDeltaTable(
        spark,
        str(
            WINDOW_FEATURE_PATH
        ),
    ):

        raise RuntimeError(
            "Window feature table does not exist. "
            "Start window_feature_stream first."
        )

    window_df = (
        spark.read
        .format(
            "delta"
        )
        .load(
            str(
                WINDOW_FEATURE_PATH
            )
        )
    )

    enriched_df = (
        enrich_with_window_features(
            transaction_df=(
                batch_df
            ),
            window_feature_df=(
                window_df
            ),
        )
    )

    feature_df = (
        add_hybrid_fraud_rules(
            enriched_df
        )

        .withColumn(
            "live_feature_batch_id",
            F.lit(
                batch_id
            ),
        )

        .withColumn(
            "live_feature_created_at",
            F.current_timestamp(),
        )
    )

    app_id = os.getenv(
        "LIVE_FEATURE_TXN_APP_ID",
        "fraud-live-feature-events-v1",
    )

    (
        feature_df
        .write
        .format(
            "delta"
        )

        .mode(
            "append"
        )

        .option(
            "txnAppId",
            app_id,
        )

        .option(
            "txnVersion",
            batch_id,
        )

        .partitionBy(
            "event_date"
        )

        .save(
            str(
                LIVE_FEATURE_EVENT_PATH
            )
        )
    )


# ============================================================
# QUERY
# ============================================================

def start_live_feature_query(
    stateful_df: DataFrame,
) -> StreamingQuery:

    trigger_interval = os.getenv(
        "LIVE_FEATURE_TRIGGER_INTERVAL",
        "5 seconds",
    )

    return (
        stateful_df
        .writeStream

        .foreachBatch(
            write_live_feature_batch
        )

        .option(
            "checkpointLocation",
            str(
                LIVE_FEATURE_CHECKPOINT_PATH
            ),
        )

        .trigger(
            processingTime=(
                trigger_interval
            )
        )

        .queryName(
            "live_fraud_feature_events"
        )

        .start()
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print("=" * 78)

    print(
        "REAL-TIME FINANCIAL FRAUD DETECTION"
    )

    print(
        "IMMUTABLE LIVE FRAUD FEATURE EVENTS"
    )

    print("=" * 78)

    print(
        f"Stateful source: "
        f"{STATEFUL_FEATURE_PATH}"
    )

    print(
        f"Window source:   "
        f"{WINDOW_FEATURE_PATH}"
    )

    print(
        f"Feature events:  "
        f"{LIVE_FEATURE_EVENT_PATH}"
    )

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "LiveFeatureEvents"
            )
        )
    )

    # ========================================================
    # NEW: ATTACH STREAMING METRICS LISTENER
    # ========================================================

    metrics_listener = (
        attach_streaming_metrics_listener(
            spark,
            PROJECT_ROOT,
        )
    )

    query: StreamingQuery | None = None

    try:

        stateful_df = (
            read_stateful_stream(
                spark
            )
        )

        query = (
            start_live_feature_query(
                stateful_df
            )
        )

        print()
        print(
            "Live feature-event stream started."
        )

        print(
            f"Query ID: "
            f"{query.id}"
        )

        print()
        print(
            "Press Ctrl+C to stop gracefully."
        )

        query.awaitTermination()

    except KeyboardInterrupt:

        print()
        print(
            "Shutdown requested."
        )

    finally:

        if (
            query is not None
            and query.isActive
        ):

            query.stop()

        spark.stop()

        print(
            "Spark session stopped."
        )


if __name__ == "__main__":
    main()