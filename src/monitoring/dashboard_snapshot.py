from __future__ import annotations

import json

from datetime import (
    datetime,
    timezone,
)

from pathlib import Path

import pandas as pd

from deltalake import DeltaTable


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)


# ============================================================
# SOURCE PATHS
# ============================================================

SCORED_TRANSACTION_PATH = (
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


# ============================================================
# OUTPUT PATH
# ============================================================

DASHBOARD_DATA_PATH = (
    PROJECT_ROOT
    / "data"
    / "monitoring"
    / "dashboard"
)


# ============================================================
# COLUMNS
# ============================================================

SCORED_COLUMNS = [
    "transaction_id",
    "user_id",
    "event_timestamp",
    "amount",

    "historical_avg_amount",
    "amount_deviation_ratio",

    "transactions_last_10m",
    "spend_last_10m",

    "new_device_flag",
    "rule_anomaly_flag",
    "fraud_rule_score",

    "ml_prediction",
    "ml_fraud_probability",

    "final_fraud_flag",
    "final_fraud_severity",
    "final_decision_basis",

    "window_feature_match_found",

    "processing_timestamp",
]


ALERT_COLUMNS = [
    "transaction_id",
    "user_id",
    "event_timestamp",
    "amount",

    "historical_avg_amount",
    "amount_deviation_ratio",

    "transactions_last_10m",

    "new_device_flag",

    "fraud_rule_score",
    "ml_fraud_probability",

    "final_fraud_severity",
    "final_decision_basis",
    "fraud_reasons",

    "processing_timestamp",
]


# ============================================================
# DELTA HELPERS
# ============================================================

def is_delta_table(
    path: Path,
) -> bool:

    return (
        path.exists()
        and (
            path
            / "_delta_log"
        ).exists()
    )


def read_delta_table(
    path: Path,
    columns: list[str],
) -> pd.DataFrame:
    """
    Read a transactionally consistent Delta snapshot without
    starting another Spark JVM.
    """

    table = (
        DeltaTable(
            str(
                path
            )
        )
    )

    arrow_table = (
        table.to_pyarrow_table(
            columns=columns
        )
    )

    return (
        arrow_table.to_pandas()
    )


# ============================================================
# DATA PREPARATION
# ============================================================

def prepare_scored_transactions(
    df: pd.DataFrame,
) -> pd.DataFrame:

    result = (
        df.copy()
    )

    result[
        "event_timestamp"
    ] = pd.to_datetime(
        result[
            "event_timestamp"
        ],
        utc=True,
        errors="coerce",
    )

    result[
        "processing_timestamp"
    ] = pd.to_datetime(
        result[
            "processing_timestamp"
        ],
        utc=True,
        errors="coerce",
    )

    numeric_columns = [
        "amount",
        "historical_avg_amount",
        "amount_deviation_ratio",
        "transactions_last_10m",
        "spend_last_10m",
        "fraud_rule_score",
        "ml_fraud_probability",
    ]

    for column in numeric_columns:

        result[
            column
        ] = pd.to_numeric(
            result[
                column
            ],
            errors="coerce",
        )

    boolean_columns = [
        "new_device_flag",
        "rule_anomaly_flag",
        "final_fraud_flag",
        "window_feature_match_found",
    ]

    for column in boolean_columns:

        result[
            column
        ] = (
            result[
                column
            ]
            .fillna(
                False
            )
            .astype(
                bool
            )
        )

    # --------------------------------------------------------
    # EVENT → FINAL-SCORE LATENCY
    # --------------------------------------------------------
    #
    # This measures elapsed time between the transaction's event
    # timestamp and the final fraud decision timestamp.
    #
    # It includes:
    #   - intentional simulated lateness
    #   - queue/backlog time
    #   - streaming processing time
    #
    # Therefore we call it event-to-score latency rather than
    # pretending it represents CPU execution latency alone.
    # --------------------------------------------------------

    result[
        "event_to_score_latency_ms"
    ] = (
        (
            result[
                "processing_timestamp"
            ]
            -
            result[
                "event_timestamp"
            ]
        )
        .dt.total_seconds()
        *
        1000.0
    )

    # Ignore impossible negative values caused by intentionally
    # out-of-order/future-style synthetic events.

    result.loc[
        result[
            "event_to_score_latency_ms"
        ]
        < 0,
        "event_to_score_latency_ms",
    ] = pd.NA

    return result


def prepare_alerts(
    df: pd.DataFrame,
) -> pd.DataFrame:

    result = (
        df.copy()
    )

    result[
        "event_timestamp"
    ] = pd.to_datetime(
        result[
            "event_timestamp"
        ],
        utc=True,
        errors="coerce",
    )

    result[
        "processing_timestamp"
    ] = pd.to_datetime(
        result[
            "processing_timestamp"
        ],
        utc=True,
        errors="coerce",
    )

    return result


# ============================================================
# BUILD SNAPSHOT
# ============================================================

def build_dashboard_snapshot(
    *,
    persist: bool = True,
) -> dict:

    if not is_delta_table(
        SCORED_TRANSACTION_PATH
    ):

        raise FileNotFoundError(
            "The scored_transactions Delta table "
            "does not exist yet."
        )

    scored_df = (
        read_delta_table(
            SCORED_TRANSACTION_PATH,
            SCORED_COLUMNS,
        )
    )

    scored_df = (
        prepare_scored_transactions(
            scored_df
        )
    )

    if scored_df.empty:

        raise RuntimeError(
            "The scored_transactions table exists "
            "but currently contains no rows."
        )

    # ========================================================
    # ALERT SOURCE
    # ========================================================

    if is_delta_table(
        FRAUD_ALERT_PATH
    ):

        alerts_df = (
            read_delta_table(
                FRAUD_ALERT_PATH,
                ALERT_COLUMNS,
            )
        )

        alerts_df = (
            prepare_alerts(
                alerts_df
            )
        )

    else:

        # Fall back to the final flag from scored transactions
        # if no alert Delta table has been created yet.

        alerts_df = (
            scored_df[
                scored_df[
                    "final_fraud_flag"
                ]
            ]
            .copy()
        )


    # ========================================================
    # TOP-LEVEL KPIs
    # ========================================================

    transaction_count = int(
        len(
            scored_df
        )
    )

    fraud_alert_count = int(
        scored_df[
            "final_fraud_flag"
        ].sum()
    )

    fraud_rate = (
        fraud_alert_count
        /
        transaction_count
        if transaction_count > 0
        else 0.0
    )

    critical_alert_count = int(
        (
            scored_df[
                "final_fraud_severity"
            ]
            ==
            "CRITICAL"
        ).sum()
    )

    high_alert_count = int(
        (
            scored_df[
                "final_fraud_severity"
            ]
            ==
            "HIGH"
        ).sum()
    )

    unique_users = int(
        scored_df[
            "user_id"
        ].nunique()
    )

    average_ml_probability = float(
        scored_df[
            "ml_fraud_probability"
        ]
        .fillna(
            0.0
        )
        .mean()
    )

    average_transaction_value = float(
        scored_df[
            "amount"
        ]
        .fillna(
            0.0
        )
        .mean()
    )

    p95_transaction_amount = float(
        scored_df[
            "amount"
        ]
        .dropna()
        .quantile(
            0.95
        )
    )

    window_feature_coverage = float(
        scored_df[
            "window_feature_match_found"
        ]
        .mean()
    )

    valid_latency = (
        scored_df[
            "event_to_score_latency_ms"
        ]
        .dropna()
    )

    average_latency_ms = (
        float(
            valid_latency.mean()
        )
        if not valid_latency.empty
        else 0.0
    )

    p95_latency_ms = (
        float(
            valid_latency.quantile(
                0.95
            )
        )
        if not valid_latency.empty
        else 0.0
    )

    rule_anomaly_count = int(
        scored_df[
            "rule_anomaly_flag"
        ].sum()
    )

    ml_high_confidence_count = int(
        (
            scored_df[
                "final_decision_basis"
            ]
            ==
            "ML_HIGH_CONFIDENCE"
        ).sum()
    )

    hybrid_confirmation_count = int(
        (
            scored_df[
                "final_decision_basis"
            ]
            ==
            "RULE_ML_CONFIRMATION"
        ).sum()
    )

    critical_rule_count = int(
        (
            scored_df[
                "final_decision_basis"
            ]
            ==
            "CRITICAL_RULE_SCORE"
        ).sum()
    )

    # ========================================================
    # SEVERITY DISTRIBUTION
    # ========================================================

    severity_counts = (
        scored_df

        .groupby(
            "final_fraud_severity",
            dropna=False,
        )

        .size()

        .reset_index(
            name="transaction_count"
        )

        .rename(
            columns={
                "final_fraud_severity": (
                    "severity"
                )
            }
        )

        .sort_values(
            "transaction_count",
            ascending=False,
        )
    )


    # ========================================================
    # FINAL DECISION-BASIS DISTRIBUTION
    # ========================================================

    alert_scored_df = (
        scored_df[
            scored_df[
                "final_fraud_flag"
            ]
        ]
    )

    decision_basis_counts = (
        alert_scored_df

        .groupby(
            "final_decision_basis",
            dropna=False,
        )

        .size()

        .reset_index(
            name="alert_count"
        )

        .rename(
            columns={
                "final_decision_basis": (
                    "decision_basis"
                )
            }
        )

        .sort_values(
            "alert_count",
            ascending=False,
        )
    )


    # ========================================================
    # HOURLY TREND
    # ========================================================

    hourly_source = (
        scored_df.dropna(
            subset=[
                "event_timestamp"
            ]
        )
        .copy()
    )

    hourly_source[
        "hour"
    ] = (
        hourly_source[
            "event_timestamp"
        ]
        .dt.floor(
            "h"
        )
    )

    hourly_metrics = (
        hourly_source

        .groupby(
            "hour",
            as_index=False,
        )

        .agg(
            transactions=(
                "transaction_id",
                "count",
            ),

            alerts=(
                "final_fraud_flag",
                "sum",
            ),

            average_ml_probability=(
                "ml_fraud_probability",
                "mean",
            ),

            average_amount=(
                "amount",
                "mean",
            ),
        )
    )

    hourly_metrics[
        "alert_rate"
    ] = (
        hourly_metrics[
            "alerts"
        ]
        /
        hourly_metrics[
            "transactions"
        ]
    )


    # ========================================================
    # TOP USERS BY ALERT COUNT
    # ========================================================

    top_alert_users = (
        alert_scored_df

        .groupby(
            "user_id",
            as_index=False,
        )

        .agg(
            alert_count=(
                "transaction_id",
                "count",
            ),

            maximum_ml_probability=(
                "ml_fraud_probability",
                "max",
            ),

            maximum_rule_score=(
                "fraud_rule_score",
                "max",
            ),

            total_alert_amount=(
                "amount",
                "sum",
            ),
        )

        .sort_values(
            [
                "alert_count",
                "maximum_ml_probability",
            ],
            ascending=[
                False,
                False,
            ],
        )

        .head(
            20
        )
    )


    # ========================================================
    # RECENT ALERTS
    # ========================================================

    recent_alerts = (
        alerts_df

        .sort_values(
            "processing_timestamp",
            ascending=False,
        )

        .head(
            50
        )
        .copy()
    )


    # ========================================================
    # SUMMARY JSON
    # ========================================================

    latest_event_timestamp = (
        scored_df[
            "event_timestamp"
        ].max()
    )

    latest_processing_timestamp = (
        scored_df[
            "processing_timestamp"
        ].max()
    )

    summary = {
        "snapshot_created_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),

        "transactions_processed": (
            transaction_count
        ),

        "unique_users": (
            unique_users
        ),

        "fraud_alerts": (
            fraud_alert_count
        ),

        "fraud_rate": (
            fraud_rate
        ),

        "critical_alerts": (
            critical_alert_count
        ),

        "high_alerts": (
            high_alert_count
        ),

        "average_ml_fraud_probability": (
            average_ml_probability
        ),

        "average_transaction_value": (
            average_transaction_value
        ),

        "p95_transaction_amount": (
            p95_transaction_amount
        ),

        "window_feature_coverage": (
            window_feature_coverage
        ),

        "average_event_to_score_latency_ms": (
            average_latency_ms
        ),

        "p95_event_to_score_latency_ms": (
            p95_latency_ms
        ),

        "rule_anomaly_count": (
            rule_anomaly_count
        ),

        "ml_high_confidence_alerts": (
            ml_high_confidence_count
        ),

        "hybrid_confirmation_alerts": (
            hybrid_confirmation_count
        ),

        "critical_rule_alerts": (
            critical_rule_count
        ),

        "latest_event_timestamp": (
            latest_event_timestamp.isoformat()
            if pd.notna(
                latest_event_timestamp
            )
            else None
        ),

        "latest_processing_timestamp": (
            latest_processing_timestamp.isoformat()
            if pd.notna(
                latest_processing_timestamp
            )
            else None
        ),
    }


    # ========================================================
    # PERSIST COMPACT MONITORING DATA
    # ========================================================

    if persist:

        DASHBOARD_DATA_PATH.mkdir(
            parents=True,
            exist_ok=True,
        )

        (
            DASHBOARD_DATA_PATH
            / "summary.json"
        ).write_text(
            json.dumps(
                summary,
                indent=2,
            ),
            encoding="utf-8",
        )

        severity_counts.to_parquet(
            DASHBOARD_DATA_PATH
            / "severity_counts.parquet",
            index=False,
        )

        decision_basis_counts.to_parquet(
            DASHBOARD_DATA_PATH
            / "decision_basis_counts.parquet",
            index=False,
        )

        hourly_metrics.to_parquet(
            DASHBOARD_DATA_PATH
            / "hourly_metrics.parquet",
            index=False,
        )

        top_alert_users.to_parquet(
            DASHBOARD_DATA_PATH
            / "top_alert_users.parquet",
            index=False,
        )

        recent_alerts.to_parquet(
            DASHBOARD_DATA_PATH
            / "recent_alerts.parquet",
            index=False,
        )

    return {
        "summary": summary,
        "severity_counts": (
            severity_counts
        ),
        "decision_basis_counts": (
            decision_basis_counts
        ),
        "hourly_metrics": (
            hourly_metrics
        ),
        "top_alert_users": (
            top_alert_users
        ),
        "recent_alerts": (
            recent_alerts
        ),
    }


# ============================================================
# CLI
# ============================================================

def main() -> None:

    snapshot = (
        build_dashboard_snapshot(
            persist=True
        )
    )

    summary = (
        snapshot[
            "summary"
        ]
    )

    print("=" * 78)
    print(
        "FRAUD MONITORING SNAPSHOT"
    )
    print("=" * 78)

    print(
        f"Transactions processed: "
        f"{summary['transactions_processed']:,}"
    )

    print(
        f"Unique users:           "
        f"{summary['unique_users']:,}"
    )

    print(
        f"Fraud alerts:           "
        f"{summary['fraud_alerts']:,}"
    )

    print(
        f"Fraud rate:             "
        f"{summary['fraud_rate']:.2%}"
    )

    print(
        f"Critical alerts:        "
        f"{summary['critical_alerts']:,}"
    )

    print(
        f"Average ML probability: "
        f"{summary['average_ml_fraud_probability']:.4f}"
    )

    print(
        f"Average transaction:    "
        f"${summary['average_transaction_value']:,.2f}"
    )

    print(
        f"P95 transaction amount: "
        f"${summary['p95_transaction_amount']:,.2f}"
    )

    print(
        f"Window coverage:        "
        f"{summary['window_feature_coverage']:.2%}"
    )

    print(
        f"Avg event→score latency:"
        f" "
        f"{summary['average_event_to_score_latency_ms']:,.0f} ms"
    )

    print(
        f"P95 event→score latency:"
        f" "
        f"{summary['p95_event_to_score_latency_ms']:,.0f} ms"
    )

    print()
    print(
        f"Monitoring data saved to:"
    )

    print(
        DASHBOARD_DATA_PATH
    )

    print()
    print("=" * 78)
    print(
        "MONITORING SNAPSHOT CREATED"
    )
    print("=" * 78)


if __name__ == "__main__":
    main()