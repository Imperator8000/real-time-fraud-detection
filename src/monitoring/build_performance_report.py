from __future__ import annotations

import json
import sys

from pathlib import Path

import matplotlib.pyplot as plt
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
# INPUT PATHS
# ============================================================

BENCHMARK_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "benchmarks"
    / "streaming_state"
)

RESULTS_PATH = (
    BENCHMARK_DIRECTORY
    / "benchmark_ranked_results.csv"
)

RECOMMENDATION_PATH = (
    BENCHMARK_DIRECTORY
    / "recommended_shuffle_config.json"
)


# ============================================================
# OUTPUT PATHS
# ============================================================

OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "docs"
    / "performance"
)

REPORT_PATH = (
    OUTPUT_DIRECTORY
    / "benchmark_summary.md"
)

THROUGHPUT_CHART = (
    OUTPUT_DIRECTORY
    / "benchmark_throughput.png"
)

LATENCY_CHART = (
    OUTPUT_DIRECTORY
    / "benchmark_p95_latency.png"
)

MEMORY_CHART = (
    OUTPUT_DIRECTORY
    / "benchmark_state_memory.png"
)


# ============================================================
# HELPERS
# ============================================================

def percentage_change(
    old_value: float,
    new_value: float,
) -> float | None:

    if old_value == 0:
        return None

    return (
        (
            new_value
            - old_value
        )
        /
        old_value
        *
        100.0
    )


def improvement_percentage(
    old_value: float,
    new_value: float,
) -> float | None:
    """
    Used for metrics where LOWER is better, such as latency.
    """

    if old_value == 0:
        return None

    return (
        (
            old_value
            - new_value
        )
        /
        old_value
        *
        100.0
    )


def format_percentage(
    value: float | None,
) -> str:

    if value is None:
        return "N/A"

    return (
        f"{value:+.2f}%"
    )


# ============================================================
# CHARTS
# ============================================================

def create_throughput_chart(
    df: pd.DataFrame,
) -> None:

    fig, ax = plt.subplots(
        figsize=(
            9,
            5,
        )
    )

    for profile, group in (
        df.groupby(
            "profile"
        )
    ):

        ordered = (
            group.sort_values(
                "shuffle_partitions"
            )
        )

        ax.plot(
            ordered[
                "shuffle_partitions"
            ],

            ordered[
                "avg_processed_rows_per_second"
            ],

            marker="o",

            label=profile,
        )

    ax.set_title(
        "Structured Streaming Throughput"
    )

    ax.set_xlabel(
        "spark.sql.shuffle.partitions"
    )

    ax.set_ylabel(
        "Average processed rows / second"
    )

    ax.legend()

    ax.grid(
        True,
        alpha=0.25,
    )

    fig.tight_layout()

    fig.savefig(
        THROUGHPUT_CHART,
        dpi=180,
    )

    plt.close(
        fig
    )


def create_latency_chart(
    df: pd.DataFrame,
) -> None:

    fig, ax = plt.subplots(
        figsize=(
            9,
            5,
        )
    )

    for profile, group in (
        df.groupby(
            "profile"
        )
    ):

        ordered = (
            group.sort_values(
                "shuffle_partitions"
            )
        )

        ax.plot(
            ordered[
                "shuffle_partitions"
            ],

            ordered[
                "p95_trigger_execution_ms"
            ],

            marker="o",

            label=profile,
        )

    # 5-second trigger interval
    ax.axhline(
        5000,
        linestyle="--",
        label=(
            "5-second trigger budget"
        ),
    )

    ax.set_title(
        "P95 Structured Streaming Trigger Latency"
    )

    ax.set_xlabel(
        "spark.sql.shuffle.partitions"
    )

    ax.set_ylabel(
        "P95 trigger execution time (ms)"
    )

    ax.legend()

    ax.grid(
        True,
        alpha=0.25,
    )

    fig.tight_layout()

    fig.savefig(
        LATENCY_CHART,
        dpi=180,
    )

    plt.close(
        fig
    )


def create_memory_chart(
    df: pd.DataFrame,
) -> None:

    fig, ax = plt.subplots(
        figsize=(
            9,
            5,
        )
    )

    for profile, group in (
        df.groupby(
            "profile"
        )
    ):

        ordered = (
            group.sort_values(
                "shuffle_partitions"
            )
        )

        ax.plot(
            ordered[
                "shuffle_partitions"
            ],

            ordered[
                "max_state_memory_mb"
            ],

            marker="o",

            label=profile,
        )

    ax.set_title(
        "Streaming State-Store Memory"
    )

    ax.set_xlabel(
        "spark.sql.shuffle.partitions"
    )

    ax.set_ylabel(
        "Maximum state memory (MB)"
    )

    ax.legend()

    ax.grid(
        True,
        alpha=0.25,
    )

    fig.tight_layout()

    fig.savefig(
        MEMORY_CHART,
        dpi=180,
    )

    plt.close(
        fig
    )


# ============================================================
# MARKDOWN TABLE
# ============================================================

def build_markdown_table(
    df: pd.DataFrame,
) -> str:

    lines = [
        (
            "| Workload | Shuffle partitions | "
            "Processed rows/s | P95 trigger ms | "
            "State memory MB | Budget met |"
        ),

        (
            "|---|---:|---:|---:|---:|:---:|"
        ),
    ]

    ordered = (
        df.sort_values(
            [
                "profile",
                "shuffle_partitions",
            ]
        )
    )

    for _, row in (
        ordered.iterrows()
    ):

        budget_met = (
            "Yes"
            if not bool(
                row[
                    "budget_failure"
                ]
            )
            else "No"
        )

        lines.append(
            "| "
            f"{row['profile']} | "
            f"{int(row['shuffle_partitions'])} | "
            f"{row['avg_processed_rows_per_second']:.2f} | "
            f"{row['p95_trigger_execution_ms']:.2f} | "
            f"{row['max_state_memory_mb']:.2f} | "
            f"{budget_met} |"
        )

    return "\n".join(
        lines
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    if not RESULTS_PATH.exists():

        raise FileNotFoundError(
            "Ranked benchmark results do not exist. "
            "Run analyze_benchmark_results first."
        )

    if not RECOMMENDATION_PATH.exists():

        raise FileNotFoundError(
            "Benchmark recommendation does not exist."
        )

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    df = pd.read_csv(
        RESULTS_PATH
    )

    with RECOMMENDATION_PATH.open(
        "r",
        encoding="utf-8",
    ) as handle:

        recommendation = (
            json.load(
                handle
            )
        )

    baseline = int(
        recommendation[
            "baseline_shuffle_partitions"
        ]
    )

    recommended = int(
        recommendation[
            "recommended_shuffle_partitions"
        ]
    )

    # ========================================================
    # CREATE CHARTS
    # ========================================================

    create_throughput_chart(
        df
    )

    create_latency_chart(
        df
    )

    create_memory_chart(
        df
    )

    # ========================================================
    # BASELINE → RECOMMENDED COMPARISON
    # ========================================================

    comparison_sections = []

    for profile in sorted(
        df[
            "profile"
        ].unique()
    ):

        profile_df = (
            df[
                df[
                    "profile"
                ]
                ==
                profile
            ]
        )

        baseline_rows = (
            profile_df[
                profile_df[
                    "shuffle_partitions"
                ]
                ==
                baseline
            ]
        )

        recommended_rows = (
            profile_df[
                profile_df[
                    "shuffle_partitions"
                ]
                ==
                recommended
            ]
        )

        if (
            baseline_rows.empty
            or recommended_rows.empty
        ):
            continue

        baseline_row = (
            baseline_rows.iloc[
                0
            ]
        )

        recommended_row = (
            recommended_rows.iloc[
                0
            ]
        )

        throughput_change = (
            percentage_change(
                float(
                    baseline_row[
                        "avg_processed_rows_per_second"
                    ]
                ),

                float(
                    recommended_row[
                        "avg_processed_rows_per_second"
                    ]
                ),
            )
        )

        latency_improvement = (
            improvement_percentage(
                float(
                    baseline_row[
                        "p95_trigger_execution_ms"
                    ]
                ),

                float(
                    recommended_row[
                        "p95_trigger_execution_ms"
                    ]
                ),
            )
        )

        memory_change = (
            percentage_change(
                float(
                    baseline_row[
                        "max_state_memory_mb"
                    ]
                ),

                float(
                    recommended_row[
                        "max_state_memory_mb"
                    ]
                ),
            )
        )

        comparison_sections.append(
            (
                f"### {profile.title()} workload\n\n"
                f"- Baseline partitions: **{baseline}**\n"
                f"- Recommended partitions: **{recommended}**\n"
                f"- Throughput change: "
                f"**{format_percentage(throughput_change)}**\n"
                f"- P95 latency improvement: "
                f"**{format_percentage(latency_improvement)}**\n"
                f"- State-memory change: "
                f"**{format_percentage(memory_change)}**"
            )
        )

    # ========================================================
    # INTERPRETATION
    # ========================================================

    if recommended == baseline:

        selection_statement = (
            f"The controlled benchmark validated the existing "
            f"**{baseline}-partition** configuration as the "
            "best overall local throughput/latency trade-off."
        )

    else:

        selection_statement = (
            f"The controlled benchmark resulted in changing "
            f"`spark.sql.shuffle.partitions` from "
            f"**{baseline}** to **{recommended}**."
        )

    recruiter_statement = (
        f"Benchmarked Spark Structured Streaming with "
        f"4, 8, 16 and 32 shuffle partitions under both "
        f"balanced traffic and deliberate 90% hot-key skew; "
        f"selected **{recommended} partitions** using measured "
        f"P95 trigger latency, processing throughput and "
        f"state-store memory rather than static tuning rules."
    )

    # ========================================================
    # REPORT
    # ========================================================

    report = f"""# PySpark Structured Streaming Performance Benchmark

## Objective

Measure the effect of `spark.sql.shuffle.partitions` on the
stateful fraud-detection workload rather than selecting a value
by intuition.

The benchmark tested:

- 4 shuffle partitions
- 8 shuffle partitions
- 16 shuffle partitions
- 32 shuffle partitions

under two workloads:

- balanced user traffic
- deliberate hot-key traffic where 90% of records are assigned
  to one user

The benchmark used the same 10-minute sliding window,
2-minute slide, 5-minute watermark and 5-second processing
trigger used by the fraud pipeline.

## Selected Configuration

**spark.sql.shuffle.partitions = {recommended}**

{selection_statement}

Selection prioritised:

1. Keeping up with the configured input rate.
2. Keeping P95 trigger execution within the 5-second trigger
   budget.
3. Lower P95 trigger latency.
4. Higher processing throughput.
5. Lower state-store memory consumption.

## Benchmark Results

{build_markdown_table(df)}

## Baseline vs Recommended Configuration

{chr(10).join(comparison_sections)}

## Hot-Key Finding

Increasing shuffle partitions improves parallelism only when
work can actually be distributed across multiple keys.

A single user with extremely high transaction volume remains a
single logical state key for `applyInPandasWithState`.

The project deliberately does **not** salt `user_id` for the
arbitrary stateful fraud profile because doing so would create
multiple independent historical states for the same user and
break fraud-detection semantics.

Instead, the pipeline combines:

- measured shuffle-partition tuning
- `user_id` Kafka partition keys
- bounded Delta ingestion per stateful trigger
- event-time state expiration
- hot-key monitoring

## Portfolio Statement

> {recruiter_statement}

## Charts

### Processing throughput

![Benchmark throughput](benchmark_throughput.png)

### P95 trigger latency

![Benchmark P95 latency](benchmark_p95_latency.png)

### State-store memory

![Benchmark state memory](benchmark_state_memory.png)

## Reproducibility

Raw benchmark results are generated by:

`src/monitoring/benchmark_streaming_state.py`

Configuration selection is performed by:

`src/monitoring/analyze_benchmark_results.py`

This report is generated from those measured results by:

`src/monitoring/build_performance_report.py`
"""

    REPORT_PATH.write_text(
        report,
        encoding="utf-8",
    )

    print("=" * 78)
    print(
        "PERFORMANCE REPORT CREATED"
    )
    print("=" * 78)

    print(
        f"Recommended partitions: "
        f"{recommended}"
    )

    print()
    print(
        f"Report:"
    )
    print(
        REPORT_PATH
    )

    print()
    print(
        "Charts:"
    )

    print(
        THROUGHPUT_CHART
    )

    print(
        LATENCY_CHART
    )

    print(
        MEMORY_CHART
    )


if __name__ == "__main__":
    main()