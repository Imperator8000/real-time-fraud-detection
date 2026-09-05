from __future__ import annotations

import os
import sys

from pathlib import Path

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

from src.quality.transaction_quality import (
    add_quality_columns,
    select_quarantine_transactions,
    select_valid_transactions,
)

from src.monitoring.streaming_metrics_listener import (
    attach_streaming_metrics_listener,
)


# ============================================================
# DATA LOCATIONS
# ============================================================

BRONZE_TRANSACTION_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "transactions"
)

SILVER_TRANSACTION_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "transactions"
)

QUARANTINE_TRANSACTION_PATH = (
    PROJECT_ROOT
    / "data"
    / "quarantine"
    / "transactions"
)


# ============================================================
# CHECKPOINT LOCATIONS
# ============================================================

SILVER_CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "checkpoints"
    / "silver_transactions"
)

QUARANTINE_CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "checkpoints"
    / "quarantine_transactions"
)


# ============================================================
# READ BRONZE DELTA AS A STREAM
# ============================================================

def read_bronze_stream(
    spark,
) -> DataFrame:
    """
    Read the Bronze Delta transaction table as a streaming
    source.
    """

    return (
        spark.readStream
        .format(
            "delta"
        )
        .load(
            str(
                BRONZE_TRANSACTION_PATH
            )
        )
    )


# ============================================================
# BUILD SILVER DATAFRAME
# ============================================================

def build_silver_dataframe(
    valid_df: DataFrame,
) -> DataFrame:
    """
    Apply event-time watermarking and streaming
    transaction-ID deduplication.

    Valid business events become the trusted Silver layer.
    """

    watermarked_df = (
        valid_df
        .withWatermark(
            "event_timestamp",
            "5 minutes",
        )
    )

    deduplicated_df = (
        watermarked_df
        .dropDuplicatesWithinWatermark(
            [
                "transaction_id",
            ]
        )
    )

    return (
        deduplicated_df

        .withColumn(
            "silver_processed_at",
            F.current_timestamp(),
        )

        .select(
            # ------------------------------------------------
            # BUSINESS COLUMNS
            # ------------------------------------------------

            "transaction_id",
            "user_id",
            "event_timestamp",
            "amount",
            "merchant_category",
            "device_id",

            # ------------------------------------------------
            # SOURCE LINEAGE
            # ------------------------------------------------

            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",

            # ------------------------------------------------
            # PROCESSING METADATA
            # ------------------------------------------------

            "bronze_ingested_at",
            "quality_checked_at",
            "silver_processed_at",
        )
    )


# ============================================================
# QUARANTINE QUERY
# ============================================================

def start_quarantine_query(
    quarantine_df: DataFrame,
) -> StreamingQuery:
    """
    Write invalid transactions to the quarantine Delta table.
    """

    trigger_interval = os.getenv(
        "SILVER_TRIGGER_INTERVAL",
        "5 seconds",
    )

    return (
        quarantine_df

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
                QUARANTINE_CHECKPOINT_PATH
            ),
        )

        .trigger(
            processingTime=(
                trigger_interval
            )
        )

        .queryName(
            "quarantine_transactions"
        )

        .start(
            str(
                QUARANTINE_TRANSACTION_PATH
            )
        )
    )


# ============================================================
# SILVER QUERY
# ============================================================

def start_silver_query(
    silver_df: DataFrame,
) -> StreamingQuery:
    """
    Write trusted deduplicated transactions to Silver Delta.
    """

    trigger_interval = os.getenv(
        "SILVER_TRIGGER_INTERVAL",
        "5 seconds",
    )

    return (
        silver_df

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
                SILVER_CHECKPOINT_PATH
            ),
        )

        .trigger(
            processingTime=(
                trigger_interval
            )
        )

        .queryName(
            "silver_transactions"
        )

        .start(
            str(
                SILVER_TRANSACTION_PATH
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
        "BRONZE -> QUALITY -> SILVER / QUARANTINE"
    )

    print("=" * 78)

    print(
        f"Bronze:      "
        f"{BRONZE_TRANSACTION_PATH}"
    )

    print(
        f"Silver:      "
        f"{SILVER_TRANSACTION_PATH}"
    )

    print(
        f"Quarantine:  "
        f"{QUARANTINE_TRANSACTION_PATH}"
    )

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "SilverTransactions"
            )
        )
    )

    # ========================================================
    # STREAMING METRICS LISTENER
    # ========================================================

    metrics_listener = (
        attach_streaming_metrics_listener(
            spark,
            PROJECT_ROOT,
        )
    )

    silver_query: StreamingQuery | None = None

    quarantine_query: StreamingQuery | None = None

    try:

        # ====================================================
        # READ BRONZE STREAM
        # ====================================================

        bronze_df = (
            read_bronze_stream(
                spark
            )
        )

        # ====================================================
        # APPLY DATA QUALITY RULES
        # ====================================================

        quality_df = (
            add_quality_columns(
                bronze_df
            )
        )

        # ====================================================
        # SPLIT VALID AND INVALID RECORDS
        # ====================================================

        valid_df = (
            select_valid_transactions(
                quality_df
            )
        )

        quarantine_df = (
            select_quarantine_transactions(
                quality_df
            )
        )

        # ====================================================
        # WATERMARK + DEDUPLICATION
        # ====================================================

        silver_df = (
            build_silver_dataframe(
                valid_df
            )
        )

        print()
        print(
            "Silver streaming schema:"
        )

        silver_df.printSchema()

        print()
        print(
            "Quarantine streaming schema:"
        )

        quarantine_df.printSchema()

        # ====================================================
        # START QUARANTINE STREAM
        # ====================================================

        quarantine_query = (
            start_quarantine_query(
                quarantine_df
            )
        )

        # ====================================================
        # START SILVER STREAM
        # ====================================================

        silver_query = (
            start_silver_query(
                silver_df
            )
        )

        print()
        print(
            "Silver query started:"
        )

        print(
            f"  Query ID: "
            f"{silver_query.id}"
        )

        print(
            f"  Run ID:   "
            f"{silver_query.runId}"
        )

        print()
        print(
            "Quarantine query started:"
        )

        print(
            f"  Query ID: "
            f"{quarantine_query.id}"
        )

        print(
            f"  Run ID:   "
            f"{quarantine_query.runId}"
        )

        print()
        print(
            "Waiting for new Bronze transactions..."
        )

        print(
            "Press Ctrl+C to stop gracefully."
        )

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:

        print()
        print(
            "Shutdown requested."
        )

    finally:

        # ====================================================
        # STOP STREAMING QUERIES
        # ====================================================

        for query in (
            silver_query,
            quarantine_query,
        ):

            if (
                query is not None
                and query.isActive
            ):

                query.stop()

        # ====================================================
        # STOP SPARK
        # ====================================================

        spark.stop()

        print(
            "Spark session stopped."
        )


if __name__ == "__main__":
    main()