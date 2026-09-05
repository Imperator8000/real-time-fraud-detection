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

from src.features.final_fraud_decision import (
    add_final_fraud_decision,
)

from src.ml.live_inference import (
    apply_live_ml_inference,
    load_fraud_model,
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

LIVE_FEATURE_EVENT_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "fraud_feature_events"
)

MODEL_PATH = (
    PROJECT_ROOT
    / "models"
    / "fraud_random_forest"
)

FINAL_SCORED_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "scored_transactions"
)

FRAUD_ALERT_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "fraud_alerts"
)

FINAL_SCORING_CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "checkpoints"
    / "final_fraud_scoring"
)


# ============================================================
# SOURCE
# ============================================================

def read_live_feature_stream(
    spark,
) -> DataFrame:

    return (
        spark.readStream
        .format(
            "delta"
        )
        .load(
            str(
                LIVE_FEATURE_EVENT_PATH
            )
        )
    )


# ============================================================
# RECRUITER-FACING OUTPUT
# ============================================================

def build_final_output(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        .select(
            # ------------------------------------------------
            # TRANSACTION
            # ------------------------------------------------

            "transaction_id",
            "user_id",
            "event_timestamp",
            "amount",
            "merchant_category",
            "device_id",

            # ------------------------------------------------
            # HISTORICAL BEHAVIOR
            # ------------------------------------------------

            F.col(
                "historical_mean_before"
            ).alias(
                "historical_avg_amount"
            ),

            F.col(
                "amount_to_historical_avg_ratio"
            ).alias(
                "amount_deviation_ratio"
            ),

            "amount_zscore",

            # ------------------------------------------------
            # 10-MINUTE BEHAVIOR
            # ------------------------------------------------

            F.col(
                "transaction_count_10m"
            ).alias(
                "transactions_last_10m"
            ),

            F.col(
                "total_spending_10m"
            ).alias(
                "spend_last_10m"
            ),

            "transactions_per_minute",
            "unique_devices_10m",

            # ------------------------------------------------
            # DEVICE / RULE FEATURES
            # ------------------------------------------------

            F.col(
                "device_changed_from_last"
            ).alias(
                "new_device_flag"
            ),

            F.col(
                "rule_based_fraud_flag"
            ).alias(
                "rule_anomaly_flag"
            ),

            "fraud_rule_score",

            # ------------------------------------------------
            # MACHINE LEARNING
            # ------------------------------------------------

            "ml_prediction",
            "ml_fraud_probability",

            # ------------------------------------------------
            # FINAL DECISION
            # ------------------------------------------------

            "final_fraud_flag",
            "final_fraud_severity",
            "final_decision_basis",
            "fraud_reasons",

            # ------------------------------------------------
            # OBSERVABILITY
            # ------------------------------------------------

            "window_feature_match_found",

            "kafka_partition",
            "kafka_offset",

            F.col(
                "final_scored_at"
            ).alias(
                "processing_timestamp"
            ),

            F.to_date(
                F.col(
                    "event_timestamp"
                )
            ).alias(
                "event_date"
            ),
        )
    )


# ============================================================
# OUTPUT WRITER
# ============================================================

def write_scoring_batch(
    batch_df: DataFrame,
    batch_id: int,
) -> None:

    if batch_df.isEmpty():
        return

    cached_df = (
        batch_df.persist()
    )

    try:

        prepared_df = (
            cached_df

            .withColumn(
                "final_scoring_batch_id",
                F.lit(
                    batch_id
                ),
            )
        )

        scored_app_id = os.getenv(
            "FINAL_SCORED_TXN_APP_ID",
            "fraud-final-scored-v1",
        )

        alert_app_id = os.getenv(
            "FRAUD_ALERT_TXN_APP_ID",
            "fraud-final-alerts-v1",
        )

        # ====================================================
        # ALL SCORED TRANSACTIONS
        # ====================================================

        (
            prepared_df

            .write
            .format(
                "delta"
            )

            .mode(
                "append"
            )

            .option(
                "txnAppId",
                scored_app_id,
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
                    FINAL_SCORED_PATH
                )
            )
        )

        # ====================================================
        # FRAUD ALERTS ONLY
        # ====================================================

        alert_df = (
            prepared_df
            .filter(
                F.col(
                    "final_fraud_flag"
                )
            )
        )

        if not alert_df.isEmpty():

            (
                alert_df

                .write
                .format(
                    "delta"
                )

                .mode(
                    "append"
                )

                .option(
                    "txnAppId",
                    alert_app_id,
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
                        FRAUD_ALERT_PATH
                    )
                )
            )

    finally:

        cached_df.unpersist()


# ============================================================
# STREAMING QUERY
# ============================================================

def start_final_scoring_query(
    final_df: DataFrame,
) -> StreamingQuery:

    trigger_interval = os.getenv(
        "FINAL_SCORING_TRIGGER_INTERVAL",
        "5 seconds",
    )

    return (
        final_df

        .writeStream

        .foreachBatch(
            write_scoring_batch
        )

        .option(
            "checkpointLocation",
            str(
                FINAL_SCORING_CHECKPOINT_PATH
            ),
        )

        .trigger(
            processingTime=(
                trigger_interval
            )
        )

        .queryName(
            "final_ml_fraud_scoring"
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
        "LIVE MLLIB INFERENCE + FINAL FRAUD DECISION"
    )

    print("=" * 78)

    print(
        f"Features: "
        f"{LIVE_FEATURE_EVENT_PATH}"
    )

    print(
        f"Model:    "
        f"{MODEL_PATH}"
    )

    print(
        f"Scored:   "
        f"{FINAL_SCORED_PATH}"
    )

    print(
        f"Alerts:   "
        f"{FRAUD_ALERT_PATH}"
    )

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "FinalMLScoring"
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

        # ====================================================
        # LOAD TRAINED MODEL ONCE
        # ====================================================

        print()
        print(
            "Loading trained Spark PipelineModel..."
        )

        model = (
            load_fraud_model(
                MODEL_PATH
            )
        )

        print(
            "Model loaded successfully."
        )

        # ====================================================
        # FEATURE STREAM
        # ====================================================

        feature_stream_df = (
            read_live_feature_stream(
                spark
            )
        )

        # ====================================================
        # DISTRIBUTED ML INFERENCE
        # ====================================================

        ml_scored_df = (
            apply_live_ml_inference(
                feature_stream_df,
                model,
            )
        )

        # ====================================================
        # FINAL HYBRID DECISION
        # ====================================================

        decision_df = (
            add_final_fraud_decision(
                ml_scored_df
            )
        )

        final_df = (
            build_final_output(
                decision_df
            )
        )

        print()
        print(
            "Final streaming schema:"
        )

        final_df.printSchema()

        query = (
            start_final_scoring_query(
                final_df
            )
        )

        print()
        print(
            "Live ML fraud scoring started."
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