from __future__ import annotations

import random

from datetime import (
    datetime,
    timedelta,
    timezone,
)

import pandas as pd


# ============================================================
# DETERMINISTIC PUBLIC PORTFOLIO DEMO
# ============================================================

DEMO_RANDOM_SEED = 2026


def build_demo_snapshot() -> dict:
    """
    Build deterministic synthetic serving data for the public
    portfolio dashboard.

    This does NOT replace the real Kafka/PySpark pipeline.

    It allows recruiters to explore the dashboard without
    provisioning Spark, Kafka and Delta infrastructure.
    """

    rng = random.Random(
        DEMO_RANDOM_SEED
    )

    now = pd.Timestamp.now(
        tz="UTC"
    ).floor(
        "h"
    )


    # ========================================================
    # HOURLY TRANSACTION TREND
    # ========================================================

    hourly_rows = []

    for index in range(
        48
    ):

        hour = (
            now
            -
            pd.Timedelta(
                hours=47 - index
            )
        )

        base_transactions = (
            330
            +
            (
                index % 8
            )
            * 14
        )

        transactions = max(
            250,
            base_transactions
            +
            rng.randint(
                -28,
                28,
            ),
        )

        alert_rate = (
            0.016
            +
            (
                index % 6
            )
            * 0.0024
        )

        if (
            index
            in {
                13,
                14,
                31,
                32,
            }
        ):

            alert_rate += (
                0.018
            )

        alerts = max(
            1,
            int(
                transactions
                *
                alert_rate
            ),
        )

        average_ml_probability = (
            0.055
            +
            alert_rate
            * 0.75
            +
            rng.uniform(
                -0.008,
                0.008,
            )
        )

        average_amount = (
            78.0
            +
            rng.uniform(
                -12.0,
                18.0,
            )
        )

        hourly_rows.append(
            {
                "hour": hour,

                "transactions": (
                    transactions
                ),

                "alerts": (
                    alerts
                ),

                "average_ml_probability": (
                    max(
                        0.0,
                        min(
                            1.0,
                            average_ml_probability,
                        ),
                    )
                ),

                "average_amount": (
                    average_amount
                ),

                "alert_rate": (
                    alerts
                    /
                    transactions
                ),
            }
        )

    hourly_metrics = pd.DataFrame(
        hourly_rows
    )


    # ========================================================
    # SUMMARY COUNTS
    # ========================================================

    transaction_count = int(
        hourly_metrics[
            "transactions"
        ].sum()
    )

    fraud_alert_count = int(
        hourly_metrics[
            "alerts"
        ].sum()
    )

    critical_alert_count = max(
        1,
        int(
            fraud_alert_count
            *
            0.14
        ),
    )

    high_alert_count = (
        fraud_alert_count
        -
        critical_alert_count
    )

    medium_count = int(
        transaction_count
        *
        0.038
    )

    low_count = max(
        0,
        transaction_count
        -
        fraud_alert_count
        -
        medium_count
    )


    # ========================================================
    # SEVERITY DISTRIBUTION
    # ========================================================

    severity_counts = pd.DataFrame(
        [
            {
                "severity": "LOW",
                "transaction_count": low_count,
            },
            {
                "severity": "MEDIUM",
                "transaction_count": medium_count,
            },
            {
                "severity": "HIGH",
                "transaction_count": high_alert_count,
            },
            {
                "severity": "CRITICAL",
                "transaction_count": critical_alert_count,
            },
        ]
    )


    # ========================================================
    # FINAL DECISION BASIS
    # ========================================================

    ml_high_confidence = int(
        fraud_alert_count
        *
        0.43
    )

    hybrid_confirmation = int(
        fraud_alert_count
        *
        0.40
    )

    critical_rule = (
        fraud_alert_count
        -
        ml_high_confidence
        -
        hybrid_confirmation
    )

    decision_basis_counts = pd.DataFrame(
        [
            {
                "decision_basis": (
                    "ML_HIGH_CONFIDENCE"
                ),
                "alert_count": (
                    ml_high_confidence
                ),
            },
            {
                "decision_basis": (
                    "RULE_ML_CONFIRMATION"
                ),
                "alert_count": (
                    hybrid_confirmation
                ),
            },
            {
                "decision_basis": (
                    "CRITICAL_RULE_SCORE"
                ),
                "alert_count": (
                    critical_rule
                ),
            },
        ]
    )


    # ========================================================
    # TOP ALERT USERS
    # ========================================================

    user_rows = []

    for index in range(
        15
    ):

        alert_count = max(
            2,
            15
            -
            index
            +
            rng.randint(
                0,
                4,
            ),
        )

        user_rows.append(
            {
                "user_id": (
                    f"user_{1000 + index:04d}"
                ),

                "alert_count": (
                    alert_count
                ),

                "maximum_ml_probability": round(
                    min(
                        0.999,
                        0.83
                        +
                        rng.random()
                        * 0.16,
                    ),
                    4,
                ),

                "maximum_rule_score": (
                    rng.randint(
                        5,
                        11,
                    )
                ),

                "total_alert_amount": round(
                    rng.uniform(
                        1200.0,
                        9800.0,
                    ),
                    2,
                ),
            }
        )

    top_alert_users = (
        pd.DataFrame(
            user_rows
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

        .reset_index(
            drop=True
        )
    )


    # ========================================================
    # RECENT ALERTS
    # ========================================================

    reason_options = [
        [
            "HISTORICAL_AMOUNT_ANOMALY",
            "HIGH_ML_PROBABILITY",
        ],

        [
            "HIGH_TRANSACTION_VELOCITY",
            "MULTI_DEVICE_ACTIVITY",
            "RULE_ML_CONFIRMATION",
        ],

        [
            "DEVICE_CHANGE_AMOUNT_ANOMALY",
            "HIGH_ML_PROBABILITY",
        ],

        [
            "AMOUNT_ZSCORE_ANOMALY",
            "CRITICAL_RULE_SCORE",
        ],

        [
            "RAPID_REPEAT_ACTIVITY",
            "HIGH_TRANSACTION_VELOCITY",
        ],

        [
            "MERCHANT_DIVERSITY_ANOMALY",
            "RULE_ML_CONFIRMATION",
        ],
    ]

    decision_options = [
        "ML_HIGH_CONFIDENCE",
        "RULE_ML_CONFIRMATION",
        "CRITICAL_RULE_SCORE",
    ]

    recent_rows = []

    for index in range(
        50
    ):

        event_timestamp = (
            now
            -
            pd.Timedelta(
                minutes=(
                    index
                    *
                    7
                    +
                    rng.randint(
                        0,
                        5,
                    )
                )
            )
        )

        amount = round(
            rng.uniform(
                150.0,
                4200.0,
            ),
            2,
        )

        probability = round(
            rng.uniform(
                0.42,
                0.995,
            ),
            4,
        )

        rule_score = (
            rng.randint(
                3,
                11,
            )
        )

        severity = (
            "CRITICAL"
            if (
                probability
                >= 0.94
                or rule_score
                >= 9
            )
            else "HIGH"
        )

        processing_timestamp = (
            event_timestamp
            +
            pd.Timedelta(
                seconds=rng.uniform(
                    5.0,
                    18.0,
                )
            )
        )

        recent_rows.append(
            {
                "processing_timestamp": (
                    processing_timestamp
                ),

                "event_timestamp": (
                    event_timestamp
                ),

                "transaction_id": (
                    f"demo_txn_{50000 + index}"
                ),

                "user_id": (
                    f"user_{rng.randint(1000, 1199):04d}"
                ),

                "amount": (
                    amount
                ),

                "historical_avg_amount": round(
                    rng.uniform(
                        55.0,
                        420.0,
                    ),
                    2,
                ),

                "amount_deviation_ratio": round(
                    rng.uniform(
                        1.7,
                        7.8,
                    ),
                    2,
                ),

                "transactions_last_10m": (
                    rng.randint(
                        2,
                        12,
                    )
                ),

                "new_device_flag": (
                    rng.random()
                    <
                    0.34
                ),

                "fraud_rule_score": (
                    rule_score
                ),

                "ml_fraud_probability": (
                    probability
                ),

                "final_fraud_severity": (
                    severity
                ),

                "final_decision_basis": (
                    rng.choice(
                        decision_options
                    )
                ),

                "fraud_reasons": (
                    rng.choice(
                        reason_options
                    )
                ),
            }
        )

    recent_alerts = (
        pd.DataFrame(
            recent_rows
        )

        .sort_values(
            "processing_timestamp",
            ascending=False,
        )

        .reset_index(
            drop=True
        )
    )


    # ========================================================
    # SUMMARY
    # ========================================================

    summary = {
        "snapshot_created_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),

        "demo_mode": True,

        "data_source": (
            "synthetic_portfolio_replay"
        ),

        "transactions_processed": (
            transaction_count
        ),

        "unique_users": 2000,

        "fraud_alerts": (
            fraud_alert_count
        ),

        "fraud_rate": (
            fraud_alert_count
            /
            transaction_count
        ),

        "critical_alerts": (
            critical_alert_count
        ),

        "high_alerts": (
            high_alert_count
        ),

        "average_ml_fraud_probability": float(
            hourly_metrics[
                "average_ml_probability"
            ].mean()
        ),

        "average_transaction_value": float(
            hourly_metrics[
                "average_amount"
            ].mean()
        ),

        "p95_transaction_amount": 318.42,

        "window_feature_coverage": 0.984,

        "average_event_to_score_latency_ms": 8120.0,

        "p95_event_to_score_latency_ms": 14780.0,

        "rule_anomaly_count": int(
            fraud_alert_count
            *
            1.31
        ),

        "ml_high_confidence_alerts": (
            ml_high_confidence
        ),

        "hybrid_confirmation_alerts": (
            hybrid_confirmation
        ),

        "critical_rule_alerts": (
            critical_rule
        ),

        "latest_event_timestamp": (
            now.isoformat()
        ),

        "latest_processing_timestamp": (
            (
                now
                +
                pd.Timedelta(
                    seconds=9
                )
            ).isoformat()
        ),
    }


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