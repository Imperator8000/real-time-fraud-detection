from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================
# QUALITY CONSTANTS
# ============================================================

MAX_REASONABLE_TRANSACTION_AMOUNT = 1_000_000.0

MAX_FUTURE_EVENT_SECONDS = 5 * 60


# ============================================================
# QUALITY RULES
# ============================================================

def add_quality_columns(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Add deterministic transaction-quality flags.

    Bronze remains unchanged.

    This function only classifies each Bronze record as valid or
    invalid and provides a machine-readable rejection reason.

    Important:
    Fraudulent-looking transactions are NOT rejected here.

    For example:
        $25,000 electronics purchase

    may be suspicious, but it is still a structurally valid
    transaction and should continue to the fraud engine.
    """

    current_timestamp = (
        F.current_timestamp()
    )

    maximum_future_timestamp = (
        current_timestamp
        + F.expr(
            f"INTERVAL {MAX_FUTURE_EVENT_SECONDS} SECONDS"
        )
    )

    # --------------------------------------------------------
    # INDIVIDUAL QUALITY RULES
    # --------------------------------------------------------

    invalid_json = (
        ~F.coalesce(
            F.col("json_parse_success"),
            F.lit(False),
        )
    )

    missing_transaction_id = (
        F.col("transaction_id").isNull()
        | (
            F.length(
                F.trim(
                    F.col("transaction_id")
                )
            )
            == 0
        )
    )

    missing_user_id = (
        F.col("user_id").isNull()
        | (
            F.length(
                F.trim(
                    F.col("user_id")
                )
            )
            == 0
        )
    )

    missing_event_timestamp = (
        F.col(
            "event_timestamp"
        ).isNull()
    )

    missing_amount = (
        F.col("amount").isNull()
    )

    nan_amount = (
        F.isnan(
            F.col("amount")
        )
    )

    non_positive_amount = (
        F.col("amount") <= 0
    )

    excessive_amount = (
        F.col("amount")
        > MAX_REASONABLE_TRANSACTION_AMOUNT
    )

    missing_merchant_category = (
        F.col(
            "merchant_category"
        ).isNull()
        | (
            F.length(
                F.trim(
                    F.col(
                        "merchant_category"
                    )
                )
            )
            == 0
        )
    )

    missing_device_id = (
        F.col("device_id").isNull()
        | (
            F.length(
                F.trim(
                    F.col("device_id")
                )
            )
            == 0
        )
    )

    event_too_far_in_future = (
        F.col("event_timestamp")
        > maximum_future_timestamp
    )

    # --------------------------------------------------------
    # REJECTION REASONS
    # --------------------------------------------------------
    #
    # One event can violate multiple rules.
    #
    # We retain ALL reasons rather than only the first failure.
    # This makes quarantine analysis much more useful.
    # --------------------------------------------------------

    rejection_reasons = F.array_compact(
        F.array(
            F.when(
                invalid_json,
                F.lit(
                    "INVALID_JSON"
                ),
            ),

            F.when(
                missing_transaction_id,
                F.lit(
                    "MISSING_TRANSACTION_ID"
                ),
            ),

            F.when(
                missing_user_id,
                F.lit(
                    "MISSING_USER_ID"
                ),
            ),

            F.when(
                missing_event_timestamp,
                F.lit(
                    "MISSING_EVENT_TIMESTAMP"
                ),
            ),

            F.when(
                missing_amount,
                F.lit(
                    "MISSING_AMOUNT"
                ),
            ),

            F.when(
                nan_amount,
                F.lit(
                    "NAN_AMOUNT"
                ),
            ),

            F.when(
                non_positive_amount,
                F.lit(
                    "NON_POSITIVE_AMOUNT"
                ),
            ),

            F.when(
                excessive_amount,
                F.lit(
                    "EXCESSIVE_AMOUNT"
                ),
            ),

            F.when(
                missing_merchant_category,
                F.lit(
                    "MISSING_MERCHANT_CATEGORY"
                ),
            ),

            F.when(
                missing_device_id,
                F.lit(
                    "MISSING_DEVICE_ID"
                ),
            ),

            F.when(
                event_too_far_in_future,
                F.lit(
                    "EVENT_TIMESTAMP_TOO_FAR_IN_FUTURE"
                ),
            ),
        )
    )

    return (
        bronze_df

        .withColumn(
            "quality_checked_at",
            current_timestamp,
        )

        .withColumn(
            "rejection_reasons",
            rejection_reasons,
        )

        .withColumn(
            "quality_rule_failure_count",
            F.size(
                F.col(
                    "rejection_reasons"
                )
            ),
        )

        .withColumn(
            "is_valid_transaction",
            (
                F.col(
                    "quality_rule_failure_count"
                )
                == 0
            ),
        )
    )


# ============================================================
# VALID TRANSACTIONS
# ============================================================

def select_valid_transactions(
    quality_df: DataFrame,
) -> DataFrame:
    """
    Return only structurally valid transactions.
    """

    return (
        quality_df
        .filter(
            F.col(
                "is_valid_transaction"
            )
        )
    )


# ============================================================
# QUARANTINE TRANSACTIONS
# ============================================================

def select_quarantine_transactions(
    quality_df: DataFrame,
) -> DataFrame:
    """
    Return rejected events with their failure reasons.

    Kafka lineage and original JSON are retained so every bad
    event can be traced back to its source partition and offset.
    """

    return (
        quality_df
        .filter(
            ~F.col(
                "is_valid_transaction"
            )
        )
        .withColumn(
            "quarantined_at",
            F.current_timestamp(),
        )
    )