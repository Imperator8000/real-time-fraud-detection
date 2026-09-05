from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

def get_ml_fraud_threshold() -> float:

    return float(
        os.getenv(
            "ML_FRAUD_PROBABILITY_THRESHOLD",
            "0.80",
        )
    )


def get_hybrid_confirmation_threshold() -> float:

    return float(
        os.getenv(
            "ML_HYBRID_CONFIRMATION_THRESHOLD",
            "0.40",
        )
    )


def get_medium_risk_threshold() -> float:

    return float(
        os.getenv(
            "ML_MEDIUM_RISK_THRESHOLD",
            "0.35",
        )
    )


def get_critical_probability_threshold() -> float:

    return float(
        os.getenv(
            "ML_CRITICAL_PROBABILITY_THRESHOLD",
            "0.95",
        )
    )


def get_critical_rule_score() -> int:

    return int(
        os.getenv(
            "RULE_ONLY_CRITICAL_SCORE_THRESHOLD",
            "8",
        )
    )


# ============================================================
# FINAL DECISION
# ============================================================

def add_final_fraud_decision(
    df: DataFrame,
) -> DataFrame:

    ml_threshold = (
        get_ml_fraud_threshold()
    )

    hybrid_threshold = (
        get_hybrid_confirmation_threshold()
    )

    medium_threshold = (
        get_medium_risk_threshold()
    )

    critical_ml_threshold = (
        get_critical_probability_threshold()
    )

    critical_rule_score = (
        get_critical_rule_score()
    )

    # --------------------------------------------------------
    # THREE PATHS TO A FINAL FRAUD ALERT
    # --------------------------------------------------------
    #
    # 1. ML independently has high confidence.
    #
    # 2. Rule engine is suspicious and ML gives supporting
    #    probability.
    #
    # 3. Rules are so extreme that they can escalate even
    #    without strong ML agreement.
    # --------------------------------------------------------

    ml_high_confidence = (
        F.col(
            "ml_fraud_probability"
        )
        >=
        F.lit(
            ml_threshold
        )
    )

    rules_with_ml_confirmation = (
        F.col(
            "rule_based_fraud_flag"
        )
        &
        (
            F.col(
                "ml_fraud_probability"
            )
            >=
            F.lit(
                hybrid_threshold
            )
        )
    )

    critical_rules = (
        F.col(
            "fraud_rule_score"
        )
        >=
        F.lit(
            critical_rule_score
        )
    )

    final_fraud_flag = (
        ml_high_confidence
        |
        rules_with_ml_confirmation
        |
        critical_rules
    )

    # --------------------------------------------------------
    # EXPLAINABLE REASON ARRAY
    # --------------------------------------------------------

    fraud_reasons = (
        F.array_compact(
            F.array(

                F.when(
                    F.col(
                        "rule_historical_amount_anomaly"
                    ),
                    F.lit(
                        "HISTORICAL_AMOUNT_ANOMALY"
                    ),
                ),

                F.when(
                    F.col(
                        "rule_extreme_zscore"
                    ),
                    F.lit(
                        "EXTREME_ZSCORE"
                    ),
                ),

                F.when(
                    F.col(
                        "rule_high_amount_ratio"
                    ),
                    F.lit(
                        "HIGH_AMOUNT_RATIO"
                    ),
                ),

                F.when(
                    F.col(
                        "rule_high_velocity"
                    ),
                    F.lit(
                        "HIGH_TRANSACTION_VELOCITY"
                    ),
                ),

                F.when(
                    F.col(
                        "rule_multi_device_activity"
                    ),
                    F.lit(
                        "MULTI_DEVICE_ACTIVITY"
                    ),
                ),

                F.when(
                    F.col(
                        "rule_device_change_high_amount"
                    ),
                    F.lit(
                        "NEW_DEVICE_HIGH_AMOUNT"
                    ),
                ),

                F.when(
                    F.col(
                        "rule_high_merchant_diversity"
                    ),
                    F.lit(
                        "HIGH_MERCHANT_DIVERSITY"
                    ),
                ),

                F.when(
                    F.col(
                        "rule_rapid_repeat"
                    ),
                    F.lit(
                        "RAPID_REPEAT_ACTIVITY"
                    ),
                ),

                F.when(
                    ml_high_confidence,
                    F.lit(
                        "ML_HIGH_FRAUD_PROBABILITY"
                    ),
                ),

                F.when(
                    rules_with_ml_confirmation,
                    F.lit(
                        "RULE_ML_CONFIRMATION"
                    ),
                ),

                F.when(
                    critical_rules,
                    F.lit(
                        "CRITICAL_RULE_SCORE"
                    ),
                ),
            )
        )
    )

    return (
        df

        .withColumn(
            "final_fraud_flag",
            final_fraud_flag,
        )

        .withColumn(
            "final_fraud_severity",

            F.when(
                (
                    F.col(
                        "ml_fraud_probability"
                    )
                    >=
                    F.lit(
                        critical_ml_threshold
                    )
                )
                &
                (
                    F.col(
                        "fraud_rule_score"
                    )
                    >=
                    F.lit(
                        4
                    )
                ),

                F.lit(
                    "CRITICAL"
                ),
            )

            .when(
                (
                    F.col(
                        "fraud_rule_score"
                    )
                    >=
                    F.lit(
                        critical_rule_score
                    )
                )
                &
                (
                    F.col(
                        "ml_fraud_probability"
                    )
                    >=
                    F.lit(
                        0.70
                    )
                ),

                F.lit(
                    "CRITICAL"
                ),
            )

            .when(
                final_fraud_flag,

                F.lit(
                    "HIGH"
                ),
            )

            .when(
                (
                    F.col(
                        "ml_fraud_probability"
                    )
                    >=
                    F.lit(
                        medium_threshold
                    )
                )
                |
                (
                    F.col(
                        "fraud_rule_score"
                    )
                    >=
                    F.lit(
                        3
                    )
                ),

                F.lit(
                    "MEDIUM"
                ),
            )

            .otherwise(
                F.lit(
                    "LOW"
                )
            ),
        )

        .withColumn(
            "fraud_reasons",
            fraud_reasons,
        )

        .withColumn(
            "final_decision_basis",

            F.when(
                ml_high_confidence,

                F.lit(
                    "ML_HIGH_CONFIDENCE"
                ),
            )

            .when(
                rules_with_ml_confirmation,

                F.lit(
                    "RULE_ML_CONFIRMATION"
                ),
            )

            .when(
                critical_rules,

                F.lit(
                    "CRITICAL_RULE_SCORE"
                ),
            )

            .otherwise(
                F.lit(
                    "NO_FRAUD_DECISION"
                )
            ),
        )

        .withColumn(
            "final_scored_at",
            F.current_timestamp(),
        )
    )