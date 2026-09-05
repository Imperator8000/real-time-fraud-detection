from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

def get_zscore_threshold() -> float:

    return float(
        os.getenv(
            "FRAUD_ZSCORE_THRESHOLD",
            "3.0",
        )
    )


def get_amount_ratio_threshold() -> float:

    return float(
        os.getenv(
            "FRAUD_AMOUNT_RATIO_THRESHOLD",
            "3.0",
        )
    )


def get_velocity_threshold() -> int:

    return int(
        os.getenv(
            "FRAUD_HIGH_VELOCITY_MIN_COUNT",
            "5",
        )
    )


def get_device_amount_ratio_threshold() -> float:

    return float(
        os.getenv(
            "FRAUD_DEVICE_AMOUNT_RATIO_THRESHOLD",
            "2.0",
        )
    )


def get_merchant_diversity_threshold() -> float:

    return float(
        os.getenv(
            "FRAUD_MERCHANT_DIVERSITY_THRESHOLD",
            "0.80",
        )
    )


def get_rule_score_threshold() -> int:

    return int(
        os.getenv(
            "FRAUD_RULE_SCORE_THRESHOLD",
            "4",
        )
    )


# ============================================================
# WINDOW ENRICHMENT
# ============================================================

def enrich_with_window_features(
    transaction_df: DataFrame,
    window_feature_df: DataFrame,
) -> DataFrame:
    """
    Attach the most recent applicable 10-minute window to each
    individual transaction.

    Because windows overlap, one transaction can match several:

        20:00-20:10
        20:02-20:12
        20:04-20:14
        ...

    We select the matching window with the latest window_start.

    That represents the most recently initiated behavioral
    window containing the transaction.
    """

    transaction_alias = (
        transaction_df.alias(
            "txn"
        )
    )

    window_alias = (
        window_feature_df.alias(
            "win"
        )
    )

    joined_df = (
        transaction_alias
        .join(
            window_alias,

            (
                F.col(
                    "txn.user_id"
                )
                ==
                F.col(
                    "win.user_id"
                )
            )
            &
            (
                F.col(
                    "txn.event_timestamp"
                )
                >=
                F.col(
                    "win.window_start"
                )
            )
            &
            (
                F.col(
                    "txn.event_timestamp"
                )
                <
                F.col(
                    "win.window_end"
                )
            ),

            how="left",
        )
    )

    # --------------------------------------------------------
    # OVERLAPPING WINDOW RESOLUTION
    # --------------------------------------------------------

    match_rank_window = (
        Window
        .partitionBy(
            F.col(
                "txn.transaction_id"
            )
        )
        .orderBy(
            F.col(
                "win.window_start"
            ).desc_nulls_last()
        )
    )

    ranked_df = (
        joined_df

        .withColumn(
            "_window_match_rank",
            F.row_number()
            .over(
                match_rank_window
            ),
        )

        .filter(
            F.col(
                "_window_match_rank"
            )
            == 1
        )
    )

    # --------------------------------------------------------
    # SELECT STATEFUL + WINDOW FEATURES
    # --------------------------------------------------------

    return (
        ranked_df

        .select(
            # =================================================
            # TRANSACTION
            # =================================================

            F.col(
                "txn.transaction_id"
            ).alias(
                "transaction_id"
            ),

            F.col(
                "txn.user_id"
            ).alias(
                "user_id"
            ),

            F.col(
                "txn.event_timestamp"
            ).alias(
                "event_timestamp"
            ),

            F.col(
                "txn.amount"
            ).alias(
                "amount"
            ),

            F.col(
                "txn.merchant_category"
            ).alias(
                "merchant_category"
            ),

            F.col(
                "txn.device_id"
            ).alias(
                "device_id"
            ),

            F.col(
                "txn.kafka_partition"
            ).alias(
                "kafka_partition"
            ),

            F.col(
                "txn.kafka_offset"
            ).alias(
                "kafka_offset"
            ),

            # =================================================
            # HISTORICAL STATE
            # =================================================

            F.col(
                "txn.historical_count_before"
            ),

            F.col(
                "txn.historical_mean_before"
            ),

            F.col(
                "txn.historical_stddev_before"
            ),

            F.col(
                "txn.amount_to_historical_avg_ratio"
            ),

            F.col(
                "txn.amount_zscore"
            ),

            F.col(
                "txn.amount_gt_3x_historical_avg"
            ),

            F.col(
                "txn.historical_baseline_ready"
            ),

            F.col(
                "txn.historical_anomaly_flag"
            ),

            F.col(
                "txn.seconds_since_previous_transaction"
            ),

            F.col(
                "txn.out_of_order_event"
            ),

            F.col(
                "txn.device_changed_from_last"
            ),

            F.col(
                "txn.historical_device_change_count"
            ),

            # =================================================
            # WINDOW IDENTITY
            # =================================================

            F.col(
                "win.window_start"
            ).alias(
                "window_start"
            ),

            F.col(
                "win.window_end"
            ).alias(
                "window_end"
            ),

            # =================================================
            # WINDOW BEHAVIOR
            # =================================================

            F.coalesce(
                F.col(
                    "win.transaction_count_10m"
                ),
                F.lit(
                    1
                ),
            ).alias(
                "transaction_count_10m"
            ),

            F.coalesce(
                F.col(
                    "win.total_spending_10m"
                ),
                F.col(
                    "txn.amount"
                ),
            ).alias(
                "total_spending_10m"
            ),

            F.coalesce(
                F.col(
                    "win.average_transaction_amount_10m"
                ),
                F.col(
                    "txn.amount"
                ),
            ).alias(
                "average_transaction_amount_10m"
            ),

            F.coalesce(
                F.col(
                    "win.maximum_transaction_amount_10m"
                ),
                F.col(
                    "txn.amount"
                ),
            ).alias(
                "maximum_transaction_amount_10m"
            ),

            F.coalesce(
                F.col(
                    "win.transaction_amount_stddev_10m"
                ),
                F.lit(
                    0.0
                ),
            ).alias(
                "transaction_amount_stddev_10m"
            ),

            F.coalesce(
                F.col(
                    "win.unique_devices_10m"
                ),
                F.lit(
                    1
                ),
            ).alias(
                "unique_devices_10m"
            ),

            F.coalesce(
                F.col(
                    "win.unique_merchant_categories_10m"
                ),
                F.lit(
                    1
                ),
            ).alias(
                "unique_merchant_categories_10m"
            ),

            F.coalesce(
                F.col(
                    "win.transactions_per_minute"
                ),
                F.lit(
                    0.1
                ),
            ).alias(
                "transactions_per_minute"
            ),

            F.coalesce(
                F.col(
                    "win.spending_per_minute"
                ),
                (
                    F.col(
                        "txn.amount"
                    )
                    / F.lit(
                        10.0
                    )
                ),
            ).alias(
                "spending_per_minute"
            ),

            F.coalesce(
                F.col(
                    "win.amount_coefficient_of_variation"
                ),
                F.lit(
                    0.0
                ),
            ).alias(
                "amount_coefficient_of_variation"
            ),

            F.coalesce(
                F.col(
                    "win.device_diversity_ratio"
                ),
                F.lit(
                    1.0
                ),
            ).alias(
                "device_diversity_ratio"
            ),

            F.coalesce(
                F.col(
                    "win.merchant_diversity_ratio"
                ),
                F.lit(
                    1.0
                ),
            ).alias(
                "merchant_diversity_ratio"
            ),

            F.coalesce(
                F.col(
                    "win.high_velocity_flag"
                ),
                F.lit(
                    False
                ),
            ).alias(
                "high_velocity_flag"
            ),

            F.coalesce(
                F.col(
                    "win.multi_device_flag"
                ),
                F.lit(
                    False
                ),
            ).alias(
                "multi_device_flag"
            ),

            # =================================================
            # JOIN OBSERVABILITY
            # =================================================

            (
                F.col(
                    "win.window_start"
                ).isNotNull()
            ).alias(
                "window_feature_match_found"
            ),
        )
    )


# ============================================================
# HYBRID RULE FEATURES
# ============================================================

def add_hybrid_fraud_rules(
    feature_df: DataFrame,
) -> DataFrame:
    """
    Convert raw behavioral features into interpretable rule
    signals.

    These rules remain separate columns so recruiters,
    investigators, and ML training code can understand WHY a
    transaction looked suspicious.
    """

    zscore_threshold = (
        get_zscore_threshold()
    )

    amount_ratio_threshold = (
        get_amount_ratio_threshold()
    )

    velocity_threshold = (
        get_velocity_threshold()
    )

    device_amount_ratio_threshold = (
        get_device_amount_ratio_threshold()
    )

    merchant_diversity_threshold = (
        get_merchant_diversity_threshold()
    )

    final_score_threshold = (
        get_rule_score_threshold()
    )

    rules_df = (
        feature_df

        # ----------------------------------------------------
        # RULE 1 — HISTORICAL AMOUNT
        # ----------------------------------------------------

        .withColumn(
            "rule_historical_amount_anomaly",

            F.coalesce(
                F.col(
                    "historical_anomaly_flag"
                ),
                F.lit(
                    False
                ),
            ),
        )

        # ----------------------------------------------------
        # RULE 2 — STATISTICAL DEVIATION
        # ----------------------------------------------------

        .withColumn(
            "rule_extreme_zscore",

            (
                F.coalesce(
                    F.col(
                        "amount_zscore"
                    ),
                    F.lit(
                        0.0
                    ),
                )
                >=
                F.lit(
                    zscore_threshold
                )
            ),
        )

        # ----------------------------------------------------
        # RULE 3 — AMOUNT RATIO
        # ----------------------------------------------------

        .withColumn(
            "rule_high_amount_ratio",

            (
                F.coalesce(
                    F.col(
                        "amount_to_historical_avg_ratio"
                    ),
                    F.lit(
                        0.0
                    ),
                )
                >=
                F.lit(
                    amount_ratio_threshold
                )
            )
            &
            F.col(
                "historical_baseline_ready"
            ),
        )

        # ----------------------------------------------------
        # RULE 4 — RAPID VELOCITY
        # ----------------------------------------------------

        .withColumn(
            "rule_high_velocity",

            (
                F.col(
                    "transaction_count_10m"
                )
                >=
                F.lit(
                    velocity_threshold
                )
            ),
        )

        # ----------------------------------------------------
        # RULE 5 — MULTIPLE DEVICES
        # ----------------------------------------------------

        .withColumn(
            "rule_multi_device_activity",

            F.coalesce(
                F.col(
                    "multi_device_flag"
                ),
                F.lit(
                    False
                ),
            ),
        )

        # ----------------------------------------------------
        # RULE 6 — DEVICE CHANGE + LARGE AMOUNT
        # ----------------------------------------------------

        .withColumn(
            "rule_device_change_high_amount",

            F.col(
                "device_changed_from_last"
            )
            &
            (
                F.coalesce(
                    F.col(
                        "amount_to_historical_avg_ratio"
                    ),
                    F.lit(
                        0.0
                    ),
                )
                >=
                F.lit(
                    device_amount_ratio_threshold
                )
            ),
        )

        # ----------------------------------------------------
        # RULE 7 — HIGH MERCHANT DIVERSITY
        # ----------------------------------------------------
        #
        # This is not yet a learned "unusual merchant category"
        # profile.
        #
        # It represents rapid merchant-category diversity within
        # the recent behavioral window.
        # ----------------------------------------------------

        .withColumn(
            "rule_high_merchant_diversity",

            (
                F.col(
                    "transaction_count_10m"
                )
                >= F.lit(
                    4
                )
            )
            &
            (
                F.col(
                    "merchant_diversity_ratio"
                )
                >=
                F.lit(
                    merchant_diversity_threshold
                )
            ),
        )

        # ----------------------------------------------------
        # RULE 8 — VERY SHORT INTER-TRANSACTION TIME
        # ----------------------------------------------------

        .withColumn(
            "rule_rapid_repeat",

            (
                F.coalesce(
                    F.col(
                        "seconds_since_previous_transaction"
                    ),
                    F.lit(
                        999999.0
                    ),
                )
                <=
                F.lit(
                    15.0
                )
            ),
        )
    )

    # ========================================================
    # WEIGHTED RULE SCORE
    # ========================================================
    #
    # Strong amount anomalies receive more weight than weak
    # contextual signals.
    #
    # This score is interpretable and will later exist alongside
    # the ML probability.
    # ========================================================

    scored_df = (
        rules_df

        .withColumn(
            "fraud_rule_score",

            (
                F.col(
                    "rule_historical_amount_anomaly"
                ).cast(
                    "int"
                )
                * F.lit(
                    3
                )
            )

            +

            (
                F.col(
                    "rule_extreme_zscore"
                ).cast(
                    "int"
                )
                * F.lit(
                    2
                )
            )

            +

            (
                F.col(
                    "rule_high_amount_ratio"
                ).cast(
                    "int"
                )
                * F.lit(
                    2
                )
            )

            +

            (
                F.col(
                    "rule_high_velocity"
                ).cast(
                    "int"
                )
                * F.lit(
                    2
                )
            )

            +

            F.col(
                "rule_multi_device_activity"
            ).cast(
                "int"
            )

            +

            (
                F.col(
                    "rule_device_change_high_amount"
                ).cast(
                    "int"
                )
                * F.lit(
                    2
                )
            )

            +

            F.col(
                "rule_high_merchant_diversity"
            ).cast(
                "int"
            )

            +

            F.col(
                "rule_rapid_repeat"
            ).cast(
                "int"
            ),
        )

        .withColumn(
            "rule_based_fraud_flag",

            (
                F.col(
                    "fraud_rule_score"
                )
                >=
                F.lit(
                    final_score_threshold
                )
            ),
        )

        .withColumn(
            "fraud_feature_generated_at",
            F.current_timestamp(),
        )

        .withColumn(
            "event_date",
            F.to_date(
                F.col(
                    "event_timestamp"
                )
            ),
        )
    )

    return scored_df