from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.features.final_fraud_decision import (
    add_final_fraud_decision,
)


DECISION_SCHEMA = StructType(
    [
        StructField(
            "case_id",
            StringType(),
            False,
        ),

        StructField(
            "ml_fraud_probability",
            DoubleType(),
            False,
        ),

        StructField(
            "fraud_rule_score",
            IntegerType(),
            False,
        ),

        StructField(
            "rule_based_fraud_flag",
            BooleanType(),
            False,
        ),

        StructField(
            "rule_historical_amount_anomaly",
            BooleanType(),
            False,
        ),

        StructField(
            "rule_extreme_zscore",
            BooleanType(),
            False,
        ),

        StructField(
            "rule_high_amount_ratio",
            BooleanType(),
            False,
        ),

        StructField(
            "rule_high_velocity",
            BooleanType(),
            False,
        ),

        StructField(
            "rule_multi_device_activity",
            BooleanType(),
            False,
        ),

        StructField(
            "rule_device_change_high_amount",
            BooleanType(),
            False,
        ),

        StructField(
            "rule_high_merchant_diversity",
            BooleanType(),
            False,
        ),

        StructField(
            "rule_rapid_repeat",
            BooleanType(),
            False,
        ),
    ]
)


def test_final_fraud_decision_paths(
    spark,
):

    df = (
        spark.createDataFrame(
            [
                # --------------------------------------------
                # ML independently flags fraud
                # --------------------------------------------
                (
                    "ml_high",
                    0.95,
                    1,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                ),

                # --------------------------------------------
                # Rules + moderate ML confirmation
                # --------------------------------------------
                (
                    "hybrid",
                    0.55,
                    5,
                    True,
                    True,
                    False,
                    True,
                    False,
                    False,
                    False,
                    False,
                    False,
                ),

                # --------------------------------------------
                # Normal transaction
                # --------------------------------------------
                (
                    "normal",
                    0.10,
                    0,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                ),
            ],

            schema=(
                DECISION_SCHEMA
            ),
        )
    )

    results = {
        row.case_id: row

        for row in (
            add_final_fraud_decision(
                df
            )
            .collect()
        )
    }

    assert (
        results[
            "ml_high"
        ].final_fraud_flag
        is True
    )

    assert (
        results[
            "hybrid"
        ].final_fraud_flag
        is True
    )

    assert (
        results[
            "normal"
        ].final_fraud_flag
        is False
    )

    assert (
        results[
            "normal"
        ].final_fraud_severity
        == "LOW"
    )