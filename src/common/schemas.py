from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================================
# TRANSACTION EVENT CONTRACT
# ============================================================

TRANSACTION_SCHEMA = StructType(
    [
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
            "timestamp",
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
    ]
)


TRANSACTION_COLUMNS = (
    "transaction_id",
    "user_id",
    "timestamp",
    "amount",
    "merchant_category",
    "device_id",
)


REQUIRED_TRANSACTION_COLUMNS = frozenset(
    TRANSACTION_COLUMNS
)


# ============================================================
# JSON PAYLOAD FIELD NAMES
# ============================================================

TRANSACTION_JSON_FIELDS = {
    "transaction_id": "transaction_id",
    "user_id": "user_id",
    "timestamp": "timestamp",
    "amount": "amount",
    "merchant_category": "merchant_category",
    "device_id": "device_id",
}