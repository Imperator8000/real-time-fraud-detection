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

from src.features.window_features import (
    build_user_window_features,
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

SILVER_TRANSACTION_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "transactions"
)

WINDOW_FEATURE_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "user_window_features"
)

WINDOW_FEATURE_CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "checkpoints"
    / "user_window_features"
)


# ============================================================
# SOURCE
# ============================================================

def read_silver_stream(
    spark,
) -> DataFrame:

    return (
        spark.readStream
        .format(
            "delta"
        )
        .load(
            str(
                SILVER_TRANSACTION_PATH
            )
        )
    )


# ============================================================
# DELTA UPSERT
# ============================================================

def upsert_window_features(
    batch_df: DataFrame,
    batch_id: int,
) -> None:

    spark = (
        batch_df.sparkSession
    )

    prepared_df = (
        batch_df

        .withColumn(
            "stream_batch_id",
            F.lit(
                batch_id
            ),
        )

        .withColumn(
            "feature_upserted_at",
            F.current_timestamp(),
        )
    )

    destination = str(
        WINDOW_FEATURE_PATH
    )

    if not DeltaTable.isDeltaTable(
        spark,
        destination,
    ):

        (
            prepared_df
            .write
            .format(
                "delta"
            )
            .mode(
                "append"
            )

            .partitionBy(
                "window_date"
            )

            .save(
                destination
            )
        )

        return

    target = (
        DeltaTable.forPath(
            spark,
            destination,
        )
    )

    (
        target
        .alias(
            "target"
        )

        .merge(
            prepared_df.alias(
                "source"
            ),

            """
            target.user_id = source.user_id
            AND target.window_start = source.window_start
            AND target.window_end = source.window_end
            AND target.window_date = source.window_date
            """,
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )


# ============================================================
# STREAMING QUERY
# ============================================================

def start_feature_query(
    feature_df: DataFrame,
) -> StreamingQuery:

    trigger_interval = os.getenv(
        "WINDOW_FEATURE_TRIGGER_INTERVAL",
        "5 seconds",
    )

    return (
        feature_df
        .writeStream

        .outputMode(
            "update"
        )

        .foreachBatch(
            upsert_window_features
        )

        .option(
            "checkpointLocation",
            str(
                WINDOW_FEATURE_CHECKPOINT_PATH
            ),
        )

        .trigger(
            processingTime=(
                trigger_interval
            )
        )

        .queryName(
            "user_window_fraud_features"
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
        "SILVER -> SLIDING WINDOW FRAUD FEATURES"
    )

    print("=" * 78)

    print(
        f"Silver source: "
        f"{SILVER_TRANSACTION_PATH}"
    )

    print(
        f"Feature table: "
        f"{WINDOW_FEATURE_PATH}"
    )

    print(
        f"Checkpoint:    "
        f"{WINDOW_FEATURE_CHECKPOINT_PATH}"
    )

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "UserWindowFeatures"
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

        silver_df = (
            read_silver_stream(
                spark
            )
        )

        feature_df = (
            build_user_window_features(
                silver_df
            )
        )

        print()
        print(
            "Window feature schema:"
        )

        feature_df.printSchema()

        query = (
            start_feature_query(
                feature_df
            )
        )

        print()
        print(
            "Sliding-window feature stream started."
        )

        print(
            f"Query ID: "
            f"{query.id}"
        )

        print(
            f"Run ID:   "
            f"{query.runId}"
        )

        print()
        print(
            "Window length: 10 minutes"
        )

        print(
            "Slide:         2 minutes"
        )

        print(
            "Watermark:     5 minutes"
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