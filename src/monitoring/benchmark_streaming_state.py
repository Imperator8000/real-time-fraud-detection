from __future__ import annotations

import argparse
import json
import math
import shutil
import statistics
import sys

from datetime import (
    datetime,
    timezone,
)

from pathlib import Path

import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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


# ============================================================
# PATHS
# ============================================================

BENCHMARK_ROOT = (
    PROJECT_ROOT
    / "data"
    / "benchmarks"
    / "streaming_state"
)

BENCHMARK_CHECKPOINT_ROOT = (
    PROJECT_ROOT
    / "checkpoints"
    / "benchmarks"
    / "streaming_state"
)


# ============================================================
# EXPERIMENT SETTINGS
# ============================================================

SHUFFLE_PARTITION_VALUES = [
    4,
    8,
    16,
    32,
]


WORKLOAD_PROFILES = [
    "balanced",
    "skewed",
]


# ============================================================
# PERCENTILE
# ============================================================

def percentile(
    values: list[float],
    probability: float,
) -> float:

    if not values:
        return 0.0

    ordered = sorted(
        values
    )

    position = (
        math.ceil(
            probability
            * len(
                ordered
            )
        )
        - 1
    )

    position = max(
        0,
        min(
            position,
            len(
                ordered
            )
            - 1,
        ),
    )

    return float(
        ordered[
            position
        ]
    )


# ============================================================
# SYNTHETIC STREAM
# ============================================================

def build_benchmark_stream(
    spark,
    *,
    profile: str,
    rows_per_second: int,
    source_partitions: int,
    user_count: int,
    skew_percentage: int,
) -> DataFrame:
    """
    Build a deterministic Structured Streaming workload using
    Spark's rate source.

    balanced:
        values are spread evenly across users.

    skewed:
        skew_percentage of all transactions belong to the same
        user_id: user_hot
    """

    rate_df = (
        spark.readStream
        .format(
            "rate"
        )
        .option(
            "rowsPerSecond",
            rows_per_second,
        )
        .option(
            "numPartitions",
            source_partitions,
        )
        .load()
    )

    # --------------------------------------------------------
    # USER DISTRIBUTION
    # --------------------------------------------------------

    normal_user_index = (
        F.pmod(
            F.col(
                "value"
            ),
            F.lit(
                user_count
            ),
        )
    )

    normal_user_id = (
        F.concat(
            F.lit(
                "user_"
            ),

            F.lpad(
                normal_user_index
                .cast(
                    "string"
                ),
                6,
                "0",
            ),
        )
    )

    if profile == "balanced":

        user_id_expression = (
            normal_user_id
        )

    elif profile == "skewed":

        hot_user_condition = (
            F.pmod(
                F.col(
                    "value"
                ),
                F.lit(
                    100
                ),
            )
            <
            F.lit(
                skew_percentage
            )
        )

        user_id_expression = (
            F.when(
                hot_user_condition,
                F.lit(
                    "user_hot"
                ),
            )
            .otherwise(
                normal_user_id
            )
        )

    else:

        raise ValueError(
            f"Unknown profile: {profile}"
        )

    # --------------------------------------------------------
    # DETERMINISTIC AMOUNTS
    # --------------------------------------------------------

    amount_expression = (
        (
            F.pmod(
                F.col(
                    "value"
                )
                *
                F.lit(
                    37
                ),

                F.lit(
                    50_000
                ),
            )
            .cast(
                "double"
            )
            /
            F.lit(
                100.0
            )
        )
        +
        F.lit(
            1.0
        )
    )

    benchmark_events = (
        rate_df

        .withColumn(
            "user_id",
            user_id_expression,
        )

        .withColumn(
            "event_timestamp",
            F.col(
                "timestamp"
            ),
        )

        .withColumn(
            "amount",
            amount_expression,
        )

        .withColumn(
            "device_id",

            F.concat(
                F.col(
                    "user_id"
                ),

                F.lit(
                    "_device_"
                ),

                F.pmod(
                    F.col(
                        "value"
                    ),
                    F.lit(
                        3
                    ),
                ).cast(
                    "string"
                ),
            ),
        )

        .select(
            "user_id",
            "event_timestamp",
            "amount",
            "device_id",
        )
    )

    # --------------------------------------------------------
    # SAME STATEFUL WINDOW SHAPE AS OUR FRAUD PROJECT
    # --------------------------------------------------------

    return (
        benchmark_events

        .withWatermark(
            "event_timestamp",
            "5 minutes",
        )

        .groupBy(
            F.col(
                "user_id"
            ),

            F.window(
                F.col(
                    "event_timestamp"
                ),

                windowDuration=(
                    "10 minutes"
                ),

                slideDuration=(
                    "2 minutes"
                ),
            ).alias(
                "event_window"
            ),
        )

        .agg(
            F.count(
                "*"
            ).alias(
                "transaction_count"
            ),

            F.sum(
                "amount"
            ).alias(
                "total_spending"
            ),

            F.avg(
                "amount"
            ).alias(
                "average_amount"
            ),

            F.approx_count_distinct(
                "device_id"
            ).alias(
                "unique_devices"
            ),
        )
    )


# ============================================================
# BENCHMARK SINK
# ============================================================

def consume_micro_batch(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """
    Force execution without persisting benchmark result rows.

    We care about Spark's stateful processing performance,
    not the synthetic output dataset itself.
    """

    _ = (
        batch_df.count()
    )


# ============================================================
# PROGRESS SUMMARY
# ============================================================

def summarize_progress(
    progress_updates: list[dict],
) -> dict:
    """
    Reduce several trigger-progress records into one benchmark
    summary.
    """

    non_empty_progress = [
        progress
        for progress
        in progress_updates
        if int(
            progress.get(
                "numInputRows",
                0,
            )
            or 0
        )
        > 0
    ]

    # --------------------------------------------------------
    # REMOVE INITIAL WARM-UP TRIGGERS
    # --------------------------------------------------------

    if len(
        non_empty_progress
    ) > 3:

        measured_progress = (
            non_empty_progress[
                2:
            ]
        )

    else:

        measured_progress = (
            non_empty_progress
        )

    if not measured_progress:

        return {
            "measured_batches": 0,
            "total_input_rows": 0,
            "avg_input_rows_per_second": 0.0,
            "avg_processed_rows_per_second": 0.0,
            "processing_to_input_ratio": 0.0,
            "avg_trigger_execution_ms": 0.0,
            "p95_trigger_execution_ms": 0.0,
            "max_trigger_execution_ms": 0.0,
            "max_state_rows": 0,
            "max_state_memory_mb": 0.0,
            "max_state_store_instances": 0,
            "max_state_shuffle_partitions": 0,
        }

    input_rates = []
    processed_rates = []
    trigger_durations = []

    state_rows = []
    state_memory_bytes = []
    state_store_instances = []
    state_shuffle_partitions = []

    total_input_rows = 0

    for progress in measured_progress:

        total_input_rows += int(
            progress.get(
                "numInputRows",
                0,
            )
            or 0
        )

        input_rates.append(
            float(
                progress.get(
                    "inputRowsPerSecond",
                    0.0,
                )
                or 0.0
            )
        )

        processed_rates.append(
            float(
                progress.get(
                    "processedRowsPerSecond",
                    0.0,
                )
                or 0.0
            )
        )

        duration_ms = (
            progress.get(
                "durationMs",
                {},
            )
            or {}
        )

        trigger_durations.append(
            float(
                duration_ms.get(
                    "triggerExecution",
                    0.0,
                )
                or 0.0
            )
        )

        operators = (
            progress.get(
                "stateOperators",
                [],
            )
            or []
        )

        state_rows.append(
            sum(
                int(
                    operator.get(
                        "numRowsTotal",
                        0,
                    )
                    or 0
                )
                for operator
                in operators
            )
        )

        state_memory_bytes.append(
            sum(
                int(
                    operator.get(
                        "memoryUsedBytes",
                        0,
                    )
                    or 0
                )
                for operator
                in operators
            )
        )

        state_store_instances.append(
            sum(
                int(
                    operator.get(
                        "numStateStoreInstances",
                        0,
                    )
                    or 0
                )
                for operator
                in operators
            )
        )

        partition_counts = [
            int(
                operator.get(
                    "numShufflePartitions",
                    0,
                )
                or 0
            )
            for operator
            in operators
        ]

        state_shuffle_partitions.append(
            max(
                partition_counts
            )
            if partition_counts
            else 0
        )

    avg_input_rate = (
        statistics.mean(
            input_rates
        )
        if input_rates
        else 0.0
    )

    avg_processed_rate = (
        statistics.mean(
            processed_rates
        )
        if processed_rates
        else 0.0
    )

    processing_to_input_ratio = (
        avg_processed_rate
        /
        avg_input_rate

        if avg_input_rate > 0
        else 0.0
    )

    return {
        "measured_batches": (
            len(
                measured_progress
            )
        ),

        "total_input_rows": (
            total_input_rows
        ),

        "avg_input_rows_per_second": (
            avg_input_rate
        ),

        "avg_processed_rows_per_second": (
            avg_processed_rate
        ),

        "processing_to_input_ratio": (
            processing_to_input_ratio
        ),

        "avg_trigger_execution_ms": (
            statistics.mean(
                trigger_durations
            )
        ),

        "p95_trigger_execution_ms": (
            percentile(
                trigger_durations,
                0.95,
            )
        ),

        "max_trigger_execution_ms": (
            max(
                trigger_durations
            )
        ),

        "max_state_rows": (
            max(
                state_rows
            )
            if state_rows
            else 0
        ),

        "max_state_memory_mb": (
            (
                max(
                    state_memory_bytes
                )
                /
                1024.0
                /
                1024.0
            )
            if state_memory_bytes
            else 0.0
        ),

        "max_state_store_instances": (
            max(
                state_store_instances
            )
            if state_store_instances
            else 0
        ),

        "max_state_shuffle_partitions": (
            max(
                state_shuffle_partitions
            )
            if state_shuffle_partitions
            else 0
        ),
    }


# ============================================================
# ONE EXPERIMENT
# ============================================================

def run_one_experiment(
    spark,
    *,
    profile: str,
    shuffle_partitions: int,
    duration_seconds: int,
    rows_per_second: int,
    source_partitions: int,
    user_count: int,
    skew_percentage: int,
) -> dict:

    print()
    print("=" * 78)

    print(
        f"PROFILE={profile.upper()} | "
        f"SHUFFLE_PARTITIONS="
        f"{shuffle_partitions}"
    )

    print("=" * 78)

    # --------------------------------------------------------
    # ISOLATE THE VARIABLE UNDER TEST
    # --------------------------------------------------------

    spark.conf.set(
        "spark.sql.shuffle.partitions",
        str(
            shuffle_partitions
        ),
    )

    checkpoint_path = (
        BENCHMARK_CHECKPOINT_ROOT
        /
        (
            f"{profile}_"
            f"p{shuffle_partitions}"
        )
    )

    # Benchmark runs intentionally start with fresh state.
    shutil.rmtree(
        checkpoint_path,
        ignore_errors=True,
    )

    benchmark_df = (
        build_benchmark_stream(
            spark,
            profile=profile,
            rows_per_second=(
                rows_per_second
            ),
            source_partitions=(
                source_partitions
            ),
            user_count=(
                user_count
            ),
            skew_percentage=(
                skew_percentage
            ),
        )
    )

    query_name = (
        f"benchmark_{profile}_"
        f"shuffle_{shuffle_partitions}"
    )

    query = (
        benchmark_df

        .writeStream

        .outputMode(
            "update"
        )

        .foreachBatch(
            consume_micro_batch
        )

        .option(
            "checkpointLocation",
            str(
                checkpoint_path
            ),
        )

        .trigger(
            processingTime=(
                "5 seconds"
            )
        )

        .queryName(
            query_name
        )

        .start()
    )

    # --------------------------------------------------------
    # ALLOW QUERY TO RUN FOR FIXED DURATION
    # --------------------------------------------------------

    query.awaitTermination(
        duration_seconds
    )

    progress_updates = list(
        query.recentProgress
    )

    if query.isActive:
        query.stop()

    summary = (
        summarize_progress(
            progress_updates
        )
    )

    result = {
        "profile": profile,

        "shuffle_partitions": (
            shuffle_partitions
        ),

        "duration_seconds": (
            duration_seconds
        ),

        "rows_per_second": (
            rows_per_second
        ),

        "source_partitions": (
            source_partitions
        ),

        "user_count": (
            user_count
        ),

        "skew_percentage": (
            skew_percentage
            if profile
            == "skewed"
            else 0
        ),

        "executed_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),

        **summary,
    }

    print(
        f"Measured batches:       "
        f"{result['measured_batches']}"
    )

    print(
        f"Processed rows/sec:     "
        f"{result['avg_processed_rows_per_second']:.2f}"
    )

    print(
        f"Avg trigger ms:         "
        f"{result['avg_trigger_execution_ms']:.2f}"
    )

    print(
        f"P95 trigger ms:         "
        f"{result['p95_trigger_execution_ms']:.2f}"
    )

    print(
        f"Max state rows:         "
        f"{result['max_state_rows']:,}"
    )

    print(
        f"Max state memory MB:    "
        f"{result['max_state_memory_mb']:.2f}"
    )

    return result


# ============================================================
# CLI
# ============================================================

def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Benchmark Structured Streaming state performance "
            "across shuffle partition counts and skew profiles."
        )
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help=(
            "Seconds per benchmark configuration."
        ),
    )

    parser.add_argument(
        "--rows-per-second",
        type=int,
        default=1000,
    )

    parser.add_argument(
        "--users",
        type=int,
        default=2000,
    )

    parser.add_argument(
        "--source-partitions",
        type=int,
        default=4,
    )

    parser.add_argument(
        "--skew-percent",
        type=int,
        default=90,
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    arguments = (
        parse_arguments()
    )

    BENCHMARK_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    BENCHMARK_CHECKPOINT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "StreamingStateBenchmark"
            )
        )
    )

    results: list[
        dict
    ] = []

    try:

        print("=" * 78)

        print(
            "PYSPARK STRUCTURED STREAMING"
        )

        print(
            "STATE + SHUFFLE PARTITION BENCHMARK"
        )

        print("=" * 78)

        print(
            f"Rows/sec:          "
            f"{arguments.rows_per_second:,}"
        )

        print(
            f"Users:             "
            f"{arguments.users:,}"
        )

        print(
            f"Duration/run:      "
            f"{arguments.duration}s"
        )

        print(
            f"Source partitions: "
            f"{arguments.source_partitions}"
        )

        print(
            f"Hot-user skew:     "
            f"{arguments.skew_percent}%"
        )

        # ----------------------------------------------------
        # 2 PROFILES × 4 SHUFFLE SETTINGS
        # ----------------------------------------------------

        for profile in (
            WORKLOAD_PROFILES
        ):

            for shuffle_partitions in (
                SHUFFLE_PARTITION_VALUES
            ):

                result = (
                    run_one_experiment(
                        spark,

                        profile=(
                            profile
                        ),

                        shuffle_partitions=(
                            shuffle_partitions
                        ),

                        duration_seconds=(
                            arguments.duration
                        ),

                        rows_per_second=(
                            arguments.rows_per_second
                        ),

                        source_partitions=(
                            arguments.source_partitions
                        ),

                        user_count=(
                            arguments.users
                        ),

                        skew_percentage=(
                            arguments.skew_percent
                        ),
                    )
                )

                results.append(
                    result
                )

        # ----------------------------------------------------
        # SAVE RESULTS
        # ----------------------------------------------------

        results_df = (
            pd.DataFrame(
                results
            )
        )

        results_df = (
            results_df
            .sort_values(
                [
                    "profile",
                    "shuffle_partitions",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        csv_path = (
            BENCHMARK_ROOT
            / "benchmark_results.csv"
        )

        json_path = (
            BENCHMARK_ROOT
            / "benchmark_results.json"
        )

        results_df.to_csv(
            csv_path,
            index=False,
        )

        with json_path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            json.dump(
                results,
                handle,
                indent=2,
            )

        # ----------------------------------------------------
        # CONSOLE SUMMARY
        # ----------------------------------------------------

        display_columns = [
            "profile",
            "shuffle_partitions",
            "avg_processed_rows_per_second",
            "avg_trigger_execution_ms",
            "p95_trigger_execution_ms",
            "max_state_rows",
            "max_state_memory_mb",
        ]

        print()
        print("=" * 78)

        print(
            "BENCHMARK RESULTS"
        )

        print("=" * 78)

        print(
            results_df[
                display_columns
            ].to_string(
                index=False
            )
        )

        print()
        print(
            f"CSV:  {csv_path}"
        )

        print(
            f"JSON: {json_path}"
        )

        print()
        print("=" * 78)

        print(
            "BENCHMARK COMPLETED"
        )

        print("=" * 78)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()