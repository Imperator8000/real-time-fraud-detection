from __future__ import annotations

import json
import sys

from pathlib import Path

import pandas as pd


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)


# ============================================================
# PATHS
# ============================================================

BENCHMARK_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "benchmarks"
    / "streaming_state"
)

BENCHMARK_RESULTS_PATH = (
    BENCHMARK_DIRECTORY
    / "benchmark_results.csv"
)

RANKED_RESULTS_PATH = (
    BENCHMARK_DIRECTORY
    / "benchmark_ranked_results.csv"
)

RECOMMENDATION_PATH = (
    BENCHMARK_DIRECTORY
    / "recommended_shuffle_config.json"
)

ENV_RECOMMENDATION_PATH = (
    BENCHMARK_DIRECTORY
    / "recommended_shuffle_config.env"
)


# ============================================================
# BENCHMARK EXPECTATIONS
# ============================================================

TRIGGER_INTERVAL_MS = 5_000.0

BASELINE_SHUFFLE_PARTITIONS = 8


REQUIRED_COLUMNS = [
    "profile",
    "shuffle_partitions",
    "rows_per_second",
    "avg_input_rows_per_second",
    "avg_processed_rows_per_second",
    "processing_to_input_ratio",
    "avg_trigger_execution_ms",
    "p95_trigger_execution_ms",
    "max_trigger_execution_ms",
    "max_state_rows",
    "max_state_memory_mb",
]


# ============================================================
# VALIDATION
# ============================================================

def load_results() -> pd.DataFrame:

    if not BENCHMARK_RESULTS_PATH.exists():

        raise FileNotFoundError(
            "Benchmark results were not found at:\n"
            f"{BENCHMARK_RESULTS_PATH}\n\n"
            "Run benchmark_streaming_state.py first."
        )

    df = pd.read_csv(
        BENCHMARK_RESULTS_PATH
    )

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:

        raise RuntimeError(
            "Benchmark CSV is missing required columns: "
            f"{missing_columns}"
        )

    if df.empty:

        raise RuntimeError(
            "Benchmark results file contains no rows."
        )

    return df


# ============================================================
# RANK ONE WORKLOAD PROFILE
# ============================================================

def rank_profile(
    profile_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Rank one workload profile.

    Selection priorities:

    1. Query should keep up with incoming data.
    2. P95 trigger should remain inside the 5-second trigger.
    3. Lower P95 latency.
    4. Higher processing throughput.
    5. Lower state-memory consumption.

    Lower selection_score is better.
    """

    result = (
        profile_df
        .copy()
    )

    # --------------------------------------------------------
    # CAPACITY / LATENCY BUDGETS
    # --------------------------------------------------------

    result[
        "throughput_headroom_ratio"
    ] = (
        result[
            "avg_processed_rows_per_second"
        ]
        /
        result[
            "rows_per_second"
        ]
    )

    result[
        "p95_trigger_budget_ratio"
    ] = (
        result[
            "p95_trigger_execution_ms"
        ]
        /
        TRIGGER_INTERVAL_MS
    )

    result[
        "throughput_budget_met"
    ] = (
        result[
            "throughput_headroom_ratio"
        ]
        >= 1.0
    )

    result[
        "latency_budget_met"
    ] = (
        result[
            "p95_trigger_execution_ms"
        ]
        <= TRIGGER_INTERVAL_MS
    )

    result[
        "budget_failure"
    ] = (
        ~(
            result[
                "throughput_budget_met"
            ]
            &
            result[
                "latency_budget_met"
            ]
        )
    )

    # --------------------------------------------------------
    # PROFILE-SPECIFIC RANKS
    # --------------------------------------------------------

    result[
        "latency_rank"
    ] = (
        result[
            "p95_trigger_execution_ms"
        ]
        .rank(
            method="min",
            ascending=True,
        )
    )

    result[
        "throughput_rank"
    ] = (
        result[
            "avg_processed_rows_per_second"
        ]
        .rank(
            method="min",
            ascending=False,
        )
    )

    result[
        "memory_rank"
    ] = (
        result[
            "max_state_memory_mb"
        ]
        .rank(
            method="min",
            ascending=True,
        )
    )

    # --------------------------------------------------------
    # TRANSPARENT WEIGHTED SCORE
    # --------------------------------------------------------
    #
    # Latency gets the greatest weight because this is fraud
    # detection rather than a high-latency batch workload.
    # --------------------------------------------------------

    result[
        "performance_rank_score"
    ] = (
        (
            result[
                "latency_rank"
            ]
            * 0.50
        )
        +
        (
            result[
                "throughput_rank"
            ]
            * 0.35
        )
        +
        (
            result[
                "memory_rank"
            ]
            * 0.15
        )
    )

    # A configuration that cannot keep pace or breaches the
    # trigger interval receives a strong penalty.

    result[
        "selection_score"
    ] = (
        result[
            "performance_rank_score"
        ]
        +
        (
            result[
                "budget_failure"
            ].astype(
                int
            )
            * 100.0
        )
    )

    return result


# ============================================================
# OVERALL SELECTION
# ============================================================

def select_configuration(
    ranked_df: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.Series,
]:

    summary = (
        ranked_df

        .groupby(
            "shuffle_partitions",
            as_index=False,
        )

        .agg(
            workload_profiles=(
                "profile",
                "nunique",
            ),

            budget_failures=(
                "budget_failure",
                "sum",
            ),

            mean_selection_score=(
                "selection_score",
                "mean",
            ),

            worst_p95_trigger_ms=(
                "p95_trigger_execution_ms",
                "max",
            ),

            mean_p95_trigger_ms=(
                "p95_trigger_execution_ms",
                "mean",
            ),

            minimum_throughput_headroom=(
                "throughput_headroom_ratio",
                "min",
            ),

            mean_processed_rows_per_second=(
                "avg_processed_rows_per_second",
                "mean",
            ),

            maximum_state_memory_mb=(
                "max_state_memory_mb",
                "max",
            ),
        )
    )

    # --------------------------------------------------------
    # PRIORITY ORDER
    # --------------------------------------------------------
    #
    # 1. Fewest performance-budget failures
    # 2. Best combined profile ranking
    # 3. Lowest worst-case P95
    # 4. Lowest partition count if effectively tied
    #
    # The final tie-break avoids unnecessary partitioning
    # overhead.
    # --------------------------------------------------------

    summary = (
        summary

        .sort_values(
            by=[
                "budget_failures",
                "mean_selection_score",
                "worst_p95_trigger_ms",
                "shuffle_partitions",
            ],

            ascending=[
                True,
                True,
                True,
                True,
            ],
        )

        .reset_index(
            drop=True
        )
    )

    winner = (
        summary.iloc[
            0
        ]
    )

    return (
        summary,
        winner,
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print("=" * 78)
    print(
        "PYSPARK BENCHMARK ANALYSIS"
    )
    print("=" * 78)

    raw_df = (
        load_results()
    )

    ranked_profiles = []

    for profile_name, profile_df in (
        raw_df.groupby(
            "profile"
        )
    ):

        ranked_profile = (
            rank_profile(
                profile_df
            )
        )

        ranked_profiles.append(
            ranked_profile
        )

    ranked_df = (
        pd.concat(
            ranked_profiles,
            ignore_index=True,
        )
    )

    ranked_df = (
        ranked_df
        .sort_values(
            [
                "profile",
                "selection_score",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    (
        summary_df,
        winner,
    ) = select_configuration(
        ranked_df
    )

    recommended_partitions = int(
        winner[
            "shuffle_partitions"
        ]
    )

    # ========================================================
    # SAVE RANKED RESULTS
    # ========================================================

    ranked_df.to_csv(
        RANKED_RESULTS_PATH,
        index=False,
    )

    # ========================================================
    # BASELINE COMPARISON
    # ========================================================

    baseline_df = (
        ranked_df[
            ranked_df[
                "shuffle_partitions"
            ]
            ==
            BASELINE_SHUFFLE_PARTITIONS
        ]
    )

    recommended_df = (
        ranked_df[
            ranked_df[
                "shuffle_partitions"
            ]
            ==
            recommended_partitions
        ]
    )

    recommendation = {
        "baseline_shuffle_partitions": (
            BASELINE_SHUFFLE_PARTITIONS
        ),

        "recommended_shuffle_partitions": (
            recommended_partitions
        ),

        "changed_from_baseline": (
            recommended_partitions
            !=
            BASELINE_SHUFFLE_PARTITIONS
        ),

        "budget_failures": int(
            winner[
                "budget_failures"
            ]
        ),

        "worst_p95_trigger_ms": float(
            winner[
                "worst_p95_trigger_ms"
            ]
        ),

        "minimum_throughput_headroom": float(
            winner[
                "minimum_throughput_headroom"
            ]
        ),

        "mean_processed_rows_per_second": float(
            winner[
                "mean_processed_rows_per_second"
            ]
        ),

        "maximum_state_memory_mb": float(
            winner[
                "maximum_state_memory_mb"
            ]
        ),

        "selection_method": (
            "Budget compliance first; then "
            "50% P95 latency rank, "
            "35% throughput rank, "
            "15% state-memory rank "
            "across balanced and skewed workloads."
        ),
    }

    with RECOMMENDATION_PATH.open(
        "w",
        encoding="utf-8",
    ) as handle:

        json.dump(
            recommendation,
            handle,
            indent=2,
        )

    with ENV_RECOMMENDATION_PATH.open(
        "w",
        encoding="utf-8",
    ) as handle:

        handle.write(
            "SPARK_SQL_SHUFFLE_PARTITIONS="
            f"{recommended_partitions}\n"
        )

    # ========================================================
    # CONSOLE RESULTS
    # ========================================================

    print()
    print(
        "PER-PROFILE RESULTS"
    )
    print("-" * 78)

    display_columns = [
        "profile",
        "shuffle_partitions",
        "avg_processed_rows_per_second",
        "throughput_headroom_ratio",
        "p95_trigger_execution_ms",
        "max_state_memory_mb",
        "budget_failure",
        "selection_score",
    ]

    print(
        ranked_df[
            display_columns
        ].to_string(
            index=False
        )
    )

    print()
    print(
        "CROSS-PROFILE RANKING"
    )
    print("-" * 78)

    print(
        summary_df.to_string(
            index=False
        )
    )

    print()
    print("=" * 78)

    print(
        "RECOMMENDED CONFIGURATION"
    )

    print("=" * 78)

    print(
        "spark.sql.shuffle.partitions = "
        f"{recommended_partitions}"
    )

    if (
        recommended_partitions
        ==
        BASELINE_SHUFFLE_PARTITIONS
    ):

        print()
        print(
            "The benchmark validated the existing "
            "8-partition configuration."
        )

    else:

        print()
        print(
            "Measured recommendation changes the "
            "production setting from "
            f"{BASELINE_SHUFFLE_PARTITIONS} "
            f"to {recommended_partitions}."
        )

    print()
    print(
        f"Recommendation JSON: "
        f"{RECOMMENDATION_PATH}"
    )

    print(
        f"Environment setting: "
        f"{ENV_RECOMMENDATION_PATH}"
    )

    print()
    print("=" * 78)
    print(
        "BENCHMARK ANALYSIS COMPLETED"
    )
    print("=" * 78)


if __name__ == "__main__":
    main()