from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

def get_window_duration() -> str:
    """
    Length of the behavioral aggregation window.
    """

    return os.getenv(
        "FRAUD_WINDOW_DURATION",
        "10 minutes",
    )


def get_window_slide() -> str:
    """
    Frequency with which a new sliding window begins.
    """

    return os.getenv(
        "FRAUD_WINDOW_SLIDE",
        "2 minutes",
    )


def get_event_time_watermark() -> str:
    """
    Maximum tolerated event-time lateness.
    """

    return os.getenv(
        "EVENT_TIME_WATERMARK",
        "5 minutes",
    )


def get_high_velocity_threshold() -> int:
    """
    Initial feature threshold for unusually frequent activity.

    This produces a feature only and is not the final fraud rule.
    """

    return int(
        os.getenv(
            "HIGH_VELOCITY_TRANSACTION_THRESHOLD",
            "5",
        )
    )


def get_multi_device_threshold() -> int:
    """
    Initial feature threshold for device diversity.
    """

    return int(
        os.getenv(
            "MULTI_DEVICE_THRESHOLD",
            "2",
        )
    )


# ============================================================
# WINDOW FEATURE ENGINE
# ============================================================

def build_user_window_features(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Generate event-time behavioral features per user.

    Window:
        10 minutes

    Slide:
        every 2 minutes

    Watermark:
        5 minutes

    Because the window slides more frequently than its total
    duration, one transaction can contribute to several
    overlapping windows.

    This increases state and shuffle requirements, but provides
    substantially better fraud-velocity sensitivity than using
    non-overlapping tumbling windows.
    """

    window_duration = (
        get_window_duration()
    )

    window_slide = (
        get_window_slide()
    )

    watermark = (
        get_event_time_watermark()
    )

    high_velocity_threshold = (
        get_high_velocity_threshold()
    )

    multi_device_threshold = (
        get_multi_device_threshold()
    )

    # --------------------------------------------------------
    # EVENT-TIME WATERMARK
    # --------------------------------------------------------
    #
    # Silver is being read as a NEW streaming query.
    #
    # Watermark information from the earlier Silver query does
    # not automatically become the watermark for this query.
    #
    # Therefore we explicitly establish event-time semantics
    # again here.
    # --------------------------------------------------------

    watermarked_df = (
        silver_df
        .withWatermark(
            "event_timestamp",
            watermark,
        )
    )

    # --------------------------------------------------------
    # SLIDING WINDOW AGGREGATION
    # --------------------------------------------------------
    #
    # Each user is grouped by:
    #
    #   user_id
    #   +
    #   event-time window
    #
    # Spark maintains streaming aggregation state for active
    # windows until the watermark allows old windows to expire.
    # --------------------------------------------------------

    aggregated_df = (
        watermarked_df

        .groupBy(
            F.col(
                "user_id"
            ),

            F.window(
                F.col(
                    "event_timestamp"
                ),
                windowDuration=(
                    window_duration
                ),
                slideDuration=(
                    window_slide
                ),
            ).alias(
                "event_window"
            ),
        )

        .agg(
            F.count(
                "*"
            ).alias(
                "transaction_count_10m"
            ),

            F.sum(
                "amount"
            ).alias(
                "total_spending_10m"
            ),

            F.avg(
                "amount"
            ).alias(
                "average_transaction_amount_10m"
            ),

            F.max(
                "amount"
            ).alias(
                "maximum_transaction_amount_10m"
            ),

            F.min(
                "amount"
            ).alias(
                "minimum_transaction_amount_10m"
            ),

            F.stddev_samp(
                "amount"
            ).alias(
                "transaction_amount_stddev_10m"
            ),

            F.approx_count_distinct(
                "device_id"
            ).alias(
                "unique_devices_10m"
            ),

            F.approx_count_distinct(
                "merchant_category"
            ).alias(
                "unique_merchant_categories_10m"
            ),
        )
    )

    # --------------------------------------------------------
    # FLATTEN WINDOW STRUCTURE
    # --------------------------------------------------------

    flattened_df = (
        aggregated_df

        .select(
            "user_id",

            F.col(
                "event_window.start"
            ).alias(
                "window_start"
            ),

            F.col(
                "event_window.end"
            ).alias(
                "window_end"
            ),

            "transaction_count_10m",
            "total_spending_10m",
            "average_transaction_amount_10m",
            "maximum_transaction_amount_10m",
            "minimum_transaction_amount_10m",
            "transaction_amount_stddev_10m",
            "unique_devices_10m",
            "unique_merchant_categories_10m",
        )

        .withColumn(
            "transaction_amount_stddev_10m",
            F.coalesce(
                F.col(
                    "transaction_amount_stddev_10m"
                ),
                F.lit(
                    0.0
                ),
            ),
        )
    )

    # --------------------------------------------------------
    # WINDOW DURATION
    # --------------------------------------------------------
    #
    # Calculating this from the actual timestamps instead of
    # hard-coding 10 makes the derived-rate expressions remain
    # correct if the window duration is changed later.
    # --------------------------------------------------------

    feature_df = (
        flattened_df

        .withColumn(
            "window_duration_seconds",

            (
                F.col(
                    "window_end"
                ).cast(
                    "long"
                )
                -
                F.col(
                    "window_start"
                ).cast(
                    "long"
                )
            ).cast(
                "double"
            ),
        )

        .withColumn(
            "window_duration_minutes",

            F.col(
                "window_duration_seconds"
            )
            / F.lit(
                60.0
            ),
        )
    )

    # --------------------------------------------------------
    # VELOCITY FEATURES
    # --------------------------------------------------------

    feature_df = (
        feature_df

        .withColumn(
            "transactions_per_minute",

            F.col(
                "transaction_count_10m"
            )
            /
            F.col(
                "window_duration_minutes"
            ),
        )

        .withColumn(
            "spending_per_minute",

            F.col(
                "total_spending_10m"
            )
            /
            F.col(
                "window_duration_minutes"
            ),
        )

        # ----------------------------------------------------
        # COEFFICIENT OF VARIATION
        # ----------------------------------------------------
        #
        # A scale-independent measure of amount variability.
        #
        # This can help distinguish users whose transaction
        # amounts suddenly become erratic.
        # ----------------------------------------------------

        .withColumn(
            "amount_coefficient_of_variation",

            F.when(
                F.col(
                    "average_transaction_amount_10m"
                )
                > 0,

                F.col(
                    "transaction_amount_stddev_10m"
                )
                /
                F.col(
                    "average_transaction_amount_10m"
                ),
            )
            .otherwise(
                F.lit(
                    0.0
                )
            ),
        )

        # ----------------------------------------------------
        # DEVICE DIVERSITY
        # ----------------------------------------------------

        .withColumn(
            "device_diversity_ratio",

            F.col(
                "unique_devices_10m"
            )
            /
            F.col(
                "transaction_count_10m"
            ),
        )

        # ----------------------------------------------------
        # MERCHANT DIVERSITY
        # ----------------------------------------------------

        .withColumn(
            "merchant_diversity_ratio",

            F.col(
                "unique_merchant_categories_10m"
            )
            /
            F.col(
                "transaction_count_10m"
            ),
        )

        # ----------------------------------------------------
        # LOG TRANSFORMATION
        # ----------------------------------------------------
        #
        # Financial transaction distributions tend to be highly
        # right-skewed.
        #
        # log1p compresses very large values while preserving
        # zero-safe numeric behavior.
        # ----------------------------------------------------

        .withColumn(
            "log_total_spending_10m",

            F.log1p(
                F.col(
                    "total_spending_10m"
                )
            ),
        )

        # ----------------------------------------------------
        # HEURISTIC FEATURES
        # ----------------------------------------------------
        #
        # These flags become model/rule features.
        #
        # They are NOT themselves the final fraud result.
        # ----------------------------------------------------

        .withColumn(
            "high_velocity_flag",

            (
                F.col(
                    "transaction_count_10m"
                )
                >=
                F.lit(
                    high_velocity_threshold
                )
            ),
        )

        .withColumn(
            "multi_device_flag",

            (
                F.col(
                    "unique_devices_10m"
                )
                >=
                F.lit(
                    multi_device_threshold
                )
            ),
        )

        # ----------------------------------------------------
        # DELTA PARTITION KEY
        # ----------------------------------------------------

        .withColumn(
            "window_date",

            F.to_date(
                F.col(
                    "window_start"
                )
            ),
        )

        .withColumn(
            "feature_generated_at",

            F.current_timestamp(),
        )
    )

    return feature_df