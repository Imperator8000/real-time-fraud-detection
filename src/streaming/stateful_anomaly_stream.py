from __future__ import annotations

import os
import sys

from pathlib import Path

from pyspark.sql import DataFrame
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

from src.state.user_behavior_state import (
    build_stateful_user_behavior,
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

STATEFUL_FEATURE_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "stateful_transaction_features"
)

STATEFUL_CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "checkpoints"
    / "stateful_transaction_features"
)


# ============================================================
# SILVER SOURCE
# ============================================================

def read_silver_stream(
    spark,
) -> DataFrame:

    max_files_per_trigger = os.getenv(
        "STATEFUL_MAX_FILES_PER_TRIGGER",
        "20",
    )

    return (
        spark.readStream

        .format(
            "delta"
        )

        .option(
            "maxFilesPerTrigger",
            max_files_per_trigger,
        )

        .load(
            str(
                SILVER_TRANSACTION_PATH
            )
        )
    )


# ============================================================
# SINK
# ============================================================

def start_stateful_query(
    feature_df: DataFrame,
) -> StreamingQuery:

    trigger_interval = os.getenv(
        "STATEFUL_TRIGGER_INTERVAL",
        "5 seconds",
    )

    return (
        feature_df
        .writeStream

        .format(
            "delta"
        )

        .outputMode(
            "append"
        )

        .option(
            "checkpointLocation",
            str(
                STATEFUL_CHECKPOINT_PATH
            ),
        )

        .trigger(
            processingTime=(
                trigger_interval
            )
        )

        .queryName(
            "stateful_user_behavior"
        )

        .start(
            str(
                STATEFUL_FEATURE_PATH
            )
        )
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
        "ARBITRARY PER-USER STATEFUL PROCESSING"
    )

    print("=" * 78)

    print(
        f"Source:      "
        f"{SILVER_TRANSACTION_PATH}"
    )

    print(
        f"Output:      "
        f"{STATEFUL_FEATURE_PATH}"
    )

    print(
        f"Checkpoint:  "
        f"{STATEFUL_CHECKPOINT_PATH}"
    )

    print()

    print(
        "State algorithm:  Welford online statistics"
    )

    print(
        "Grouping key:     user_id"
    )

    print(
        "Watermark:        5 minutes"
    )

    print(
        "State timeout:    30 minutes event time"
    )

    print(
        "Anomaly rule:     amount > 3x historical average"
    )

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "StatefulUserBehavior"
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

        stateful_df = (
            build_stateful_user_behavior(
                silver_df
            )
        )

        print()
        print(
            "Stateful output schema:"
        )

        stateful_df.printSchema()

        query = (
            start_stateful_query(
                stateful_df
            )
        )

        print()
        print(
            "Stateful query started."
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