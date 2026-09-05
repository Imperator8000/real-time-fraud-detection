from __future__ import annotations

import os
import sys

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery


# ============================================================
# PROJECT PATH
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


from src.common.schemas import TRANSACTION_SCHEMA

from src.common.spark_session import (
    get_spark_session,
)

# ============================================================
# NEW: STREAMING MONITORING
# ============================================================

from src.monitoring.streaming_metrics_listener import (
    attach_streaming_metrics_listener,
)


# ============================================================
# OUTPUT LOCATIONS
# ============================================================

BRONZE_TRANSACTION_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "transactions"
)

BRONZE_CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "checkpoints"
    / "bronze_transactions"
)


# ============================================================
# CONFIGURATION
# ============================================================

def get_kafka_package() -> str:
    """
    Return the Spark Kafka connector matching the project's
    pinned Spark and Scala versions.
    """

    spark_version = os.getenv(
        "SPARK_VERSION",
        "3.5.9",
    )

    scala_binary_version = os.getenv(
        "SPARK_SCALA_BINARY_VERSION",
        "2.12",
    )

    return (
        "org.apache.spark:"
        "spark-sql-kafka-0-10_"
        f"{scala_binary_version}:"
        f"{spark_version}"
    )


# ============================================================
# KAFKA SOURCE
# ============================================================

def read_kafka_stream(
    spark,
) -> DataFrame:
    """
    Create the raw Kafka Structured Streaming DataFrame.

    Spark exposes Kafka lineage columns including:

        key
        value
        topic
        partition
        offset
        timestamp
        timestampType
        headers

    Kafka offsets are managed through Spark's streaming
    checkpoint once the query starts.
    """

    bootstrap_servers = os.getenv(
        "KAFKA_INTERNAL_BOOTSTRAP_SERVERS",
        "broker:19092",
    )

    topic = os.getenv(
        "KAFKA_TRANSACTION_TOPIC",
        "transactions",
    )

    starting_offsets = os.getenv(
        "KAFKA_STARTING_OFFSETS",
        "earliest",
    )

    max_offsets_per_trigger = os.getenv(
        "SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER",
        "5000",
    )

    return (
        spark.readStream
        .format("kafka")

        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers,
        )

        .option(
            "subscribe",
            topic,
        )

        .option(
            "startingOffsets",
            starting_offsets,
        )

        .option(
            "includeHeaders",
            "true",
        )

        .option(
            "maxOffsetsPerTrigger",
            max_offsets_per_trigger,
        )

        .option(
            "failOnDataLoss",
            "true",
        )

        .load()
    )


# ============================================================
# BRONZE TRANSFORMATION
# ============================================================

def build_bronze_dataframe(
    kafka_df: DataFrame,
) -> DataFrame:
    """
    Parse the transaction payload while retaining Kafka lineage.

    Bronze preserves:
        - original JSON
        - Kafka topic
        - partition
        - offset
        - Kafka timestamp
        - producer key
        - Kafka headers
    """

    raw_df = (
        kafka_df
        .select(
            F.col("key")
            .cast("string")
            .alias("kafka_key"),

            F.col("value")
            .cast("string")
            .alias("raw_json"),

            F.col("topic")
            .alias("kafka_topic"),

            F.col("partition")
            .alias("kafka_partition"),

            F.col("offset")
            .alias("kafka_offset"),

            F.col("timestamp")
            .alias("kafka_timestamp"),

            F.col("timestampType")
            .alias("kafka_timestamp_type"),

            F.col("headers")
            .alias("kafka_headers"),
        )
    )

    parsed_df = (
        raw_df
        .withColumn(
            "transaction",
            F.from_json(
                F.col("raw_json"),
                TRANSACTION_SCHEMA,
            ),
        )
    )

    bronze_df = (
        parsed_df
        .select(
            F.col(
                "transaction.transaction_id"
            ).alias(
                "transaction_id"
            ),

            F.col(
                "transaction.user_id"
            ).alias(
                "user_id"
            ),

            F.col(
                "transaction.timestamp"
            ).alias(
                "event_timestamp"
            ),

            F.col(
                "transaction.amount"
            ).alias(
                "amount"
            ),

            F.col(
                "transaction.merchant_category"
            ).alias(
                "merchant_category"
            ),

            F.col(
                "transaction.device_id"
            ).alias(
                "device_id"
            ),

            F.col(
                "raw_json"
            ),

            F.col(
                "kafka_key"
            ),

            F.col(
                "kafka_topic"
            ),

            F.col(
                "kafka_partition"
            ),

            F.col(
                "kafka_offset"
            ),

            F.col(
                "kafka_timestamp"
            ),

            F.col(
                "kafka_timestamp_type"
            ),

            F.col(
                "kafka_headers"
            ),

            F.current_timestamp()
            .alias(
                "bronze_ingested_at"
            ),

            (
                F.col("transaction")
                .isNotNull()
            ).alias(
                "json_parse_success"
            ),
        )
    )

    return bronze_df


# ============================================================
# BRONZE DELTA SINK
# ============================================================

def start_bronze_query(
    bronze_df: DataFrame,
) -> StreamingQuery:

    trigger_interval = os.getenv(
        "BRONZE_TRIGGER_INTERVAL",
        "5 seconds",
    )

    return (
        bronze_df
        .writeStream
        .format("delta")
        .outputMode("append")

        .option(
            "checkpointLocation",
            str(
                BRONZE_CHECKPOINT_PATH
            ),
        )

        .trigger(
            processingTime=trigger_interval
        )

        .queryName(
            "bronze_kafka_transactions"
        )

        .start(
            str(
                BRONZE_TRANSACTION_PATH
            )
        )
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    kafka_package = (
        get_kafka_package()
    )

    print("=" * 78)
    print(
        "REAL-TIME FINANCIAL FRAUD DETECTION"
    )
    print(
        "KAFKA -> PYSPARK -> BRONZE DELTA"
    )
    print("=" * 78)

    print(
        f"Kafka connector: "
        f"{kafka_package}"
    )

    print(
        f"Bronze path:     "
        f"{BRONZE_TRANSACTION_PATH}"
    )

    print(
        f"Checkpoint:      "
        f"{BRONZE_CHECKPOINT_PATH}"
    )

    spark = get_spark_session(
        app_name=(
            "FraudDetection-"
            "BronzeKafkaIngestion"
        ),
        extra_packages=[
            kafka_package,
        ],
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

        kafka_df = (
            read_kafka_stream(
                spark
            )
        )

        print()
        print(
            "Kafka streaming schema:"
        )

        kafka_df.printSchema()

        bronze_df = (
            build_bronze_dataframe(
                kafka_df
            )
        )

        print()
        print(
            "Bronze streaming schema:"
        )

        bronze_df.printSchema()

        query = (
            start_bronze_query(
                bronze_df
            )
        )

        print()
        print(
            "Bronze streaming query started."
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
            "Waiting for Kafka transactions..."
        )

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

            print(
                "Stopping Bronze stream..."
            )

            query.stop()

        spark.stop()

        print(
            "Spark session stopped."
        )


if __name__ == "__main__":
    main()