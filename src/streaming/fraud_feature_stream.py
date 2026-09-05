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

FRAUD_FEATURE_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "fraud_features"
)

FRAUD_FEATURE_CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "checkpoints"
    / "fraud_features"
)


# ============================================================
# SOURCE STREAM
# ============================================================

def read_stateful_transaction_stream(
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

def process_fraud_feature_batch(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """
    Enrich each new transaction batch with the current snapshot
    of the 10-minute feature table.

    The transaction stream is append-only.

    The window table is updated with MERGE, so reading it as a
    fresh static Delta snapshot within foreachBatch avoids
    treating those updates as append-only stream events.
    """

    if batch_df.isEmpty():
        return

    spark = (
        batch_df.sparkSession
    )

    # --------------------------------------------------------
    # LOAD CURRENT WINDOW SNAPSHOT
    # --------------------------------------------------------

    if DeltaTable.isDeltaTable(
        spark,
        str(
            WINDOW_FEATURE_PATH
        ),
    ):

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

    else:

        # ----------------------------------------------------
        # EMPTY FALLBACK
        # ----------------------------------------------------
        #
        # In normal operation the window table should already
        # exist.
        #
        # If it does not, delay processing this micro-batch by
        # failing explicitly rather than silently generating an
        # incomplete fraud dataset.
        # ----------------------------------------------------

        raise RuntimeError(
            "Window feature Delta table does not exist. "
            "Start window_feature_stream before "
            "fraud_feature_stream."
        )

    # --------------------------------------------------------
    # FEATURE ASSEMBLY
    # --------------------------------------------------------

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

    fraud_feature_df = (
        add_hybrid_fraud_rules(
            enriched_df
        )

        .withColumn(
            "feature_stream_batch_id",
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
        FRAUD_FEATURE_PATH
    )

    # --------------------------------------------------------
    # FIRST WRITE
    # --------------------------------------------------------

    if not DeltaTable.isDeltaTable(
        spark,
        destination,
    ):

        (
            fraud_feature_df

            .write
            .format(
                "delta"
            )

            .mode(
                "append"
            )

            .partitionBy(
                "event_date"
            )

            .save(
                destination
            )
        )

        return

    # --------------------------------------------------------
    # IDEMPOTENT TRANSACTION-LEVEL MERGE
    # --------------------------------------------------------
    #
    # transaction_id is our business identifier.
    #
    # If foreachBatch retries after a failure, the transaction
    # updates the existing feature row instead of creating a
    # duplicate.
    # --------------------------------------------------------

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
            fraud_feature_df.alias(
                "source"
            ),

            """
            target.transaction_id =
                source.transaction_id
            AND target.event_date =
                source.event_date
            """,
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )


# ============================================================
# STREAMING QUERY
# ============================================================

def start_fraud_feature_query(
    stateful_stream_df: DataFrame,
) -> StreamingQuery:

    trigger_interval = os.getenv(
        "FRAUD_FEATURE_TRIGGER_INTERVAL",
        "5 seconds",
    )

    return (
        stateful_stream_df

        .writeStream

        .foreachBatch(
            process_fraud_feature_batch
        )

        .option(
            "checkpointLocation",
            str(
                FRAUD_FEATURE_CHECKPOINT_PATH
            ),
        )

        .trigger(
            processingTime=(
                trigger_interval
            )
        )

        .queryName(
            "hybrid_fraud_feature_stream"
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
        "HYBRID FRAUD FEATURE ENGINE"
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
        f"Gold output:     "
        f"{FRAUD_FEATURE_PATH}"
    )

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "HybridFraudFeatures"
            )
        )
    )

    query: StreamingQuery | None = None

    try:

        stateful_stream_df = (
            read_stateful_transaction_stream(
                spark
            )
        )

        query = (
            start_fraud_feature_query(
                stateful_stream_df
            )
        )

        print()
        print(
            "Hybrid fraud-feature stream started."
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