from __future__ import annotations

import math
import os

from typing import Iterator

import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.streaming.state import (
    GroupState,
    GroupStateTimeout,
)

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================================
# STATE SCHEMA
# ============================================================
#
# IMPORTANT:
#
# We do NOT store every historical transaction.
#
# Each user keeps only a small fixed-size state record.
#
# Therefore state size is approximately:
#
#       O(number of users)
#
# rather than:
#
#       O(number of transactions)
#
# ============================================================

USER_BEHAVIOR_STATE_SCHEMA = StructType(
    [
        StructField(
            "transaction_count",
            LongType(),
            nullable=False,
        ),

        StructField(
            "mean_amount",
            DoubleType(),
            nullable=False,
        ),

        # ----------------------------------------------------
        # WELFORD M2
        # ----------------------------------------------------
        #
        # M2 stores the running sum of squared deviations from
        # the mean and allows variance to be updated online.
        # ----------------------------------------------------

        StructField(
            "m2_amount",
            DoubleType(),
            nullable=False,
        ),

        StructField(
            "last_event_epoch_ms",
            LongType(),
            nullable=False,
        ),

        StructField(
            "last_device_id",
            StringType(),
            nullable=False,
        ),

        StructField(
            "device_change_count",
            LongType(),
            nullable=False,
        ),
    ]
)


# ============================================================
# OUTPUT SCHEMA
# ============================================================

USER_BEHAVIOR_OUTPUT_SCHEMA = StructType(
    [
        # ----------------------------------------------------
        # ORIGINAL TRANSACTION
        # ----------------------------------------------------

        StructField(
            "transaction_id",
            StringType(),
            nullable=False,
        ),

        StructField(
            "user_id",
            StringType(),
            nullable=False,
        ),

        StructField(
            "event_timestamp",
            TimestampType(),
            nullable=False,
        ),

        StructField(
            "amount",
            DoubleType(),
            nullable=False,
        ),

        StructField(
            "merchant_category",
            StringType(),
            nullable=False,
        ),

        StructField(
            "device_id",
            StringType(),
            nullable=False,
        ),

        # ----------------------------------------------------
        # SOURCE LINEAGE
        # ----------------------------------------------------

        StructField(
            "kafka_partition",
            IntegerType(),
            nullable=False,
        ),

        StructField(
            "kafka_offset",
            LongType(),
            nullable=False,
        ),

        # ----------------------------------------------------
        # HISTORICAL STATE BEFORE CURRENT EVENT
        # ----------------------------------------------------

        StructField(
            "historical_count_before",
            LongType(),
            nullable=False,
        ),

        StructField(
            "historical_mean_before",
            DoubleType(),
            nullable=True,
        ),

        StructField(
            "historical_stddev_before",
            DoubleType(),
            nullable=True,
        ),

        # ----------------------------------------------------
        # ANOMALY FEATURES
        # ----------------------------------------------------

        StructField(
            "amount_to_historical_avg_ratio",
            DoubleType(),
            nullable=True,
        ),

        StructField(
            "amount_zscore",
            DoubleType(),
            nullable=True,
        ),

        StructField(
            "amount_gt_3x_historical_avg",
            BooleanType(),
            nullable=False,
        ),

        StructField(
            "historical_baseline_ready",
            BooleanType(),
            nullable=False,
        ),

        StructField(
            "historical_anomaly_flag",
            BooleanType(),
            nullable=False,
        ),

        # ----------------------------------------------------
        # TEMPORAL / DEVICE FEATURES
        # ----------------------------------------------------

        StructField(
            "seconds_since_previous_transaction",
            DoubleType(),
            nullable=True,
        ),

        StructField(
            "out_of_order_event",
            BooleanType(),
            nullable=False,
        ),

        StructField(
            "device_changed_from_last",
            BooleanType(),
            nullable=False,
        ),

        StructField(
            "historical_device_change_count",
            LongType(),
            nullable=False,
        ),

        # ----------------------------------------------------
        # STATE AFTER CURRENT EVENT
        # ----------------------------------------------------

        StructField(
            "historical_count_after",
            LongType(),
            nullable=False,
        ),

        StructField(
            "historical_mean_after",
            DoubleType(),
            nullable=False,
        ),

        StructField(
            "historical_stddev_after",
            DoubleType(),
            nullable=False,
        ),

        StructField(
            "state_timeout_epoch_ms",
            LongType(),
            nullable=False,
        ),

        StructField(
            "stateful_processed_at",
            TimestampType(),
            nullable=False,
        ),
    ]
)


# ============================================================
# CONFIGURATION HELPERS
# ============================================================

def get_amount_multiplier() -> float:
    """
    Exact multiplier from the project specification.
    """

    return float(
        os.getenv(
            "HISTORICAL_AMOUNT_MULTIPLIER",
            "3.0",
        )
    )


def get_minimum_history() -> int:
    """
    Minimum prior observations before our stronger production
    anomaly flag is considered mature.
    """

    return int(
        os.getenv(
            "MIN_HISTORICAL_TRANSACTIONS",
            "3",
        )
    )


def get_state_timeout_milliseconds() -> int:
    """
    Convert the configured inactivity timeout into milliseconds.
    """

    timeout_minutes = int(
        os.getenv(
            "USER_STATE_TIMEOUT_MINUTES",
            "30",
        )
    )

    return (
        timeout_minutes
        * 60
        * 1000
    )


# ============================================================
# STATISTICAL HELPERS
# ============================================================

def calculate_sample_stddev(
    *,
    count: int,
    m2: float,
) -> float:
    """
    Return sample standard deviation from Welford state.

    For fewer than two observations, standard deviation is zero.
    """

    if count < 2:
        return 0.0

    variance = (
        m2
        / (count - 1)
    )

    # Floating-point arithmetic can very occasionally produce
    # a tiny negative number around zero.
    variance = max(
        variance,
        0.0,
    )

    return math.sqrt(
        variance
    )


def update_welford(
    *,
    count: int,
    mean: float,
    m2: float,
    amount: float,
) -> tuple[int, float, float]:
    """
    Incrementally update mean and variance.

    Welford's algorithm avoids storing historical transaction
    values and is numerically more stable than maintaining:

        sum(x)
        sum(x^2)

    over long-running streams.
    """

    new_count = (
        count + 1
    )

    delta = (
        amount - mean
    )

    new_mean = (
        mean
        + delta / new_count
    )

    delta_after_mean_update = (
        amount - new_mean
    )

    new_m2 = (
        m2
        + (
            delta
            * delta_after_mean_update
        )
    )

    return (
        new_count,
        new_mean,
        new_m2,
    )


def timestamp_to_epoch_ms(
    value,
) -> int:
    """
    Convert pandas/Spark timestamp values to Unix milliseconds.
    """

    timestamp = pd.Timestamp(
        value
    )

    return int(
        timestamp.value
        // 1_000_000
    )


# ============================================================
# STATEFUL FUNCTION
# ============================================================

def process_user_state(
    key: tuple,
    pdf_iterator: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """
    Stateful processing function for one user_id.

    Parameters
    ----------
    key:
        Tuple containing the grouping key.
        For this project:
            key[0] == user_id

    pdf_iterator:
        One or more pandas DataFrames containing transactions
        belonging to the current user in the current trigger.

    state:
        Persistent Spark GroupState for this user.

    Returns
    -------
    Iterator[pandas.DataFrame]
        One output row per processed transaction.
    """

    user_id = str(
        key[0]
    )

    # ========================================================
    # STATE TIMEOUT
    # ========================================================
    #
    # When an event-time timeout fires, Spark invokes this
    # function with no new transactions and hasTimedOut=True.
    #
    # We remove the state and emit nothing.
    #
    # On future activity, the user receives a fresh baseline.
    # ========================================================

    if state.hasTimedOut:

        state.remove()

        return

    # ========================================================
    # READ EXISTING STATE
    # ========================================================

    if state.exists:

        (
            transaction_count,
            mean_amount,
            m2_amount,
            last_event_epoch_ms,
            last_device_id,
            device_change_count,
        ) = state.get

    else:

        transaction_count = 0
        mean_amount = 0.0
        m2_amount = 0.0

        last_event_epoch_ms = 0

        last_device_id = ""

        device_change_count = 0

    # ========================================================
    # CONSUME THE COMPLETE ITERATOR
    # ========================================================
    #
    # Spark does NOT guarantee input ordering for arbitrary
    # stateful grouped operations.
    #
    # We therefore consume all pandas chunks for this user in
    # the current micro-batch and impose deterministic ordering
    # ourselves.
    #
    # This uses memory proportional only to the current user's
    # records in the current micro-batch, not their entire
    # historical transaction history.
    #
    # A pathological "hot user" can still make this group large.
    # We address such skew later through workload controls,
    # partition analysis, and benchmarking.
    # ========================================================

    pandas_batches = [
        pdf
        for pdf in pdf_iterator
        if not pdf.empty
    ]

    if not pandas_batches:
        return

    user_events = pd.concat(
        pandas_batches,
        ignore_index=True,
    )

    # --------------------------------------------------------
    # DETERMINISTIC EVENT ORDER
    # --------------------------------------------------------
    #
    # event_timestamp is the primary ordering field.
    #
    # Kafka partition + offset provide deterministic tie-breaking
    # when timestamps are identical.
    # --------------------------------------------------------

    user_events = (
        user_events
        .sort_values(
            by=[
                "event_timestamp",
                "kafka_partition",
                "kafka_offset",
            ],
            kind="mergesort",
        )
        .reset_index(
            drop=True
        )
    )

    amount_multiplier = (
        get_amount_multiplier()
    )

    minimum_history = (
        get_minimum_history()
    )

    timeout_ms = (
        get_state_timeout_milliseconds()
    )

    output_rows: list[
        dict
    ] = []

    processing_timestamp = (
        pd.Timestamp.now(
            tz="UTC"
        )
        .tz_localize(
            None
        )
    )

    # ========================================================
    # PROCESS EACH TRANSACTION
    # ========================================================

    for row in user_events.itertuples(
        index=False
    ):

        amount = float(
            row.amount
        )

        event_epoch_ms = (
            timestamp_to_epoch_ms(
                row.event_timestamp
            )
        )

        current_device = str(
            row.device_id
        )

        # ----------------------------------------------------
        # HISTORICAL STATE BEFORE CURRENT TRANSACTION
        # ----------------------------------------------------

        count_before = int(
            transaction_count
        )

        if count_before > 0:

            mean_before = float(
                mean_amount
            )

        else:

            mean_before = None

        if count_before >= 2:

            stddev_before = (
                calculate_sample_stddev(
                    count=count_before,
                    m2=m2_amount,
                )
            )

        else:

            stddev_before = None

        # ----------------------------------------------------
        # EXACT 3X HISTORICAL-AVERAGE RULE
        # ----------------------------------------------------

        if (
            mean_before is not None
            and mean_before > 0
        ):

            amount_ratio = (
                amount
                / mean_before
            )

            amount_gt_3x = (
                amount
                >
                (
                    amount_multiplier
                    * mean_before
                )
            )

        else:

            amount_ratio = None

            amount_gt_3x = False

        # ----------------------------------------------------
        # Z-SCORE
        # ----------------------------------------------------

        if (
            stddev_before is not None
            and stddev_before > 0
            and mean_before is not None
        ):

            amount_zscore = (
                amount
                - mean_before
            ) / stddev_before

        else:

            amount_zscore = None

        # ----------------------------------------------------
        # BASELINE MATURITY
        # ----------------------------------------------------
        #
        # We preserve the exact 3x rule separately.
        #
        # historical_anomaly_flag adds a minimum-history
        # requirement so one previous tiny transaction does not
        # immediately create a high-confidence fraud alert.
        # ----------------------------------------------------

        baseline_ready = (
            count_before
            >= minimum_history
        )

        historical_anomaly_flag = (
            amount_gt_3x
            and baseline_ready
        )

        # ----------------------------------------------------
        # EVENT ORDER / TRANSACTION VELOCITY
        # ----------------------------------------------------

        if last_event_epoch_ms > 0:

            raw_gap_ms = (
                event_epoch_ms
                - last_event_epoch_ms
            )

            out_of_order_event = (
                raw_gap_ms < 0
            )

            if raw_gap_ms >= 0:

                seconds_since_previous = (
                    raw_gap_ms
                    / 1000.0
                )

            else:

                seconds_since_previous = None

        else:

            out_of_order_event = False

            seconds_since_previous = None

        # ----------------------------------------------------
        # DEVICE BEHAVIOR
        # ----------------------------------------------------

        device_changed = (
            bool(
                last_device_id
            )
            and current_device
            != last_device_id
        )

        if device_changed:

            device_change_count += 1

        # ----------------------------------------------------
        # UPDATE WELFORD STATE
        # ----------------------------------------------------

        (
            transaction_count,
            mean_amount,
            m2_amount,
        ) = update_welford(
            count=transaction_count,
            mean=mean_amount,
            m2=m2_amount,
            amount=amount,
        )

        stddev_after = (
            calculate_sample_stddev(
                count=transaction_count,
                m2=m2_amount,
            )
        )

        # ----------------------------------------------------
        # DO NOT REGRESS "LATEST EVENT" STATE FOR LATE EVENTS
        # ----------------------------------------------------

        if (
            event_epoch_ms
            >= last_event_epoch_ms
        ):

            last_event_epoch_ms = (
                event_epoch_ms
            )

            last_device_id = (
                current_device
            )

        # ----------------------------------------------------
        # EVENT-TIME STATE TIMEOUT
        # ----------------------------------------------------
        #
        # PySpark 3.5's Python GroupState accepts an absolute
        # timeout timestamp in milliseconds.
        #
        # We calculate:
        #
        # latest user event time
        #        +
        # configured inactivity period
        #
        # Once Spark's watermark moves beyond this timestamp,
        # the state becomes eligible for removal.
        # ----------------------------------------------------

        state_timeout_epoch_ms = (
            last_event_epoch_ms
            + timeout_ms
        )

        # ----------------------------------------------------
        # OUTPUT CURRENT TRANSACTION + FEATURES
        # ----------------------------------------------------

        output_rows.append(
            {
                "transaction_id": str(
                    row.transaction_id
                ),

                "user_id": user_id,

                "event_timestamp": (
                    pd.Timestamp(
                        row.event_timestamp
                    )
                ),

                "amount": amount,

                "merchant_category": str(
                    row.merchant_category
                ),

                "device_id": (
                    current_device
                ),

                "kafka_partition": int(
                    row.kafka_partition
                ),

                "kafka_offset": int(
                    row.kafka_offset
                ),

                "historical_count_before": (
                    count_before
                ),

                "historical_mean_before": (
                    mean_before
                ),

                "historical_stddev_before": (
                    stddev_before
                ),

                "amount_to_historical_avg_ratio": (
                    amount_ratio
                ),

                "amount_zscore": (
                    amount_zscore
                ),

                "amount_gt_3x_historical_avg": (
                    bool(
                        amount_gt_3x
                    )
                ),

                "historical_baseline_ready": (
                    bool(
                        baseline_ready
                    )
                ),

                "historical_anomaly_flag": (
                    bool(
                        historical_anomaly_flag
                    )
                ),

                "seconds_since_previous_transaction": (
                    seconds_since_previous
                ),

                "out_of_order_event": (
                    bool(
                        out_of_order_event
                    )
                ),

                "device_changed_from_last": (
                    bool(
                        device_changed
                    )
                ),

                "historical_device_change_count": int(
                    device_change_count
                ),

                "historical_count_after": int(
                    transaction_count
                ),

                "historical_mean_after": float(
                    mean_amount
                ),

                "historical_stddev_after": float(
                    stddev_after
                ),

                "state_timeout_epoch_ms": int(
                    state_timeout_epoch_ms
                ),

                "stateful_processed_at": (
                    processing_timestamp
                ),
            }
        )

    # ========================================================
    # PERSIST UPDATED STATE
    # ========================================================

    state.update(
        (
            int(
                transaction_count
            ),

            float(
                mean_amount
            ),

            float(
                m2_amount
            ),

            int(
                last_event_epoch_ms
            ),

            str(
                last_device_id
            ),

            int(
                device_change_count
            ),
        )
    )

    # --------------------------------------------------------
    # PYSPARK 3.5 EVENT-TIME TIMEOUT
    # --------------------------------------------------------
    #
    # The Python implementation expects the absolute timeout
    # timestamp directly.
    # --------------------------------------------------------

    final_timeout_epoch_ms = (
        last_event_epoch_ms
        + timeout_ms
    )

    state.setTimeoutTimestamp(
        int(
            final_timeout_epoch_ms
        )
    )

    # ========================================================
    # EMIT RESULTS
    # ========================================================

    if output_rows:

        yield pd.DataFrame(
            output_rows
        )


# ============================================================
# APPLY STATEFUL PROCESSING
# ============================================================

def build_stateful_user_behavior(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Apply arbitrary persistent per-user state.

    A watermark is required because this implementation uses
    EventTimeTimeout.
    """

    watermark = os.getenv(
        "USER_STATE_WATERMARK",
        "5 minutes",
    )

    watermarked_df = (
        silver_df
        .withWatermark(
            "event_timestamp",
            watermark,
        )
    )

    return (
        watermarked_df

        .groupBy(
            "user_id"
        )

        .applyInPandasWithState(
            func=(
                process_user_state
            ),

            outputStructType=(
                USER_BEHAVIOR_OUTPUT_SCHEMA
            ),

            stateStructType=(
                USER_BEHAVIOR_STATE_SCHEMA
            ),

            # Each processed input transaction represents a
            # new immutable anomaly-feature record.
            outputMode="Append",

            timeoutConf=(
                GroupStateTimeout
                .EventTimeTimeout
            ),
        )
    )