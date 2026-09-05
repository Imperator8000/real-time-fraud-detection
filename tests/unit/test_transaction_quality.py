from datetime import datetime

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.quality.transaction_quality import (
    add_quality_columns,
)


TEST_SCHEMA = StructType(
    [
        StructField(
            "transaction_id",
            StringType(),
            True,
        ),

        StructField(
            "user_id",
            StringType(),
            True,
        ),

        StructField(
            "event_timestamp",
            TimestampType(),
            True,
        ),

        StructField(
            "amount",
            DoubleType(),
            True,
        ),

        StructField(
            "merchant_category",
            StringType(),
            True,
        ),

        StructField(
            "device_id",
            StringType(),
            True,
        ),

        StructField(
            "json_parse_success",
            BooleanType(),
            True,
        ),
    ]
)


def test_valid_transaction_passes_quality_rules(
    spark,
):

    df = (
        spark.createDataFrame(
            [
                (
                    "txn_001",
                    "user_001",
                    datetime(
                        2026,
                        8,
                        1,
                        12,
                        0,
                    ),
                    75.50,
                    "grocery",
                    "device_001",
                    True,
                )
            ],
            schema=TEST_SCHEMA,
        )
    )

    result = (
        add_quality_columns(
            df
        )
        .first()
    )

    assert (
        result.is_valid_transaction
        is True
    )

    assert (
        result.quality_rule_failure_count
        == 0
    )

    assert result.rejection_reasons == []


def test_invalid_transaction_gets_multiple_reasons(
    spark,
):

    df = (
        spark.createDataFrame(
            [
                (
                    None,
                    "",
                    None,
                    -25.00,
                    "",
                    None,
                    True,
                )
            ],
            schema=TEST_SCHEMA,
        )
    )

    result = (
        add_quality_columns(
            df
        )
        .first()
    )

    reasons = set(
        result.rejection_reasons
    )

    assert (
        result.is_valid_transaction
        is False
    )

    assert (
        "MISSING_TRANSACTION_ID"
        in reasons
    )

    assert (
        "MISSING_USER_ID"
        in reasons
    )

    assert (
        "MISSING_EVENT_TIMESTAMP"
        in reasons
    )

    assert (
        "NON_POSITIVE_AMOUNT"
        in reasons
    )

    assert (
        "MISSING_MERCHANT_CATEGORY"
        in reasons
    )

    assert (
        "MISSING_DEVICE_ID"
        in reasons
    )