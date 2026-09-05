from __future__ import annotations

import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F


# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


from src.common.spark_session import get_spark_session


# ============================================================
# DELTA TEST LOCATION
# ============================================================

DELTA_TEST_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "_spark_delta_smoke_test"
)


def main() -> None:
    """
    Verify that the local Spark and Delta Lake stack works.
    """

    print("=" * 72)
    print("REAL-TIME FRAUD DETECTION")
    print("SPARK + DELTA LAKE VERIFICATION")
    print("=" * 72)

    # ========================================================
    # CREATE SPARK SESSION
    # ========================================================

    spark = get_spark_session(
        app_name="FraudDetection-SparkDeltaVerification"
    )

    try:

        # ====================================================
        # READ IMPORTANT SPARK CONFIGURATION
        # ====================================================

        spark_version = spark.version

        spark_master = (
            spark.sparkContext.master
        )

        arrow_enabled = spark.conf.get(
            "spark.sql.execution.arrow.pyspark.enabled"
        )

        session_timezone = spark.conf.get(
            "spark.sql.session.timeZone"
        )

        print()
        print(
            f"Spark version: {spark_version}"
        )

        print(
            f"Spark master: {spark_master}"
        )

        print(
            f"Arrow enabled: {arrow_enabled}"
        )

        print(
            f"Session timezone: {session_timezone}"
        )

        # ====================================================
        # CREATE SAMPLE TRANSACTION DATA
        # ====================================================

        transactions = [
            (
                "txn_001",
                "user_001",
                42.50,
                "grocery",
                "device_A",
            ),
            (
                "txn_002",
                "user_002",
                125.75,
                "electronics",
                "device_B",
            ),
            (
                "txn_003",
                "user_001",
                18.99,
                "restaurant",
                "device_A",
            ),
        ]

        schema = """
            transaction_id STRING,
            user_id STRING,
            amount DOUBLE,
            merchant_category STRING,
            device_id STRING
        """

        transaction_df = (
            spark.createDataFrame(
                transactions,
                schema=schema,
            )
            .withColumn(
                "ingested_at",
                F.current_timestamp(),
            )
        )

        print()
        print(
            "Input Spark DataFrame:"
        )

        transaction_df.show(
            truncate=False
        )

        # ====================================================
        # WRITE DATA TO DELTA LAKE
        # ====================================================

        (
            transaction_df
            .write
            .format("delta")
            .mode("overwrite")
            .save(
                str(
                    DELTA_TEST_PATH
                )
            )
        )

        # ====================================================
        # CONFIRM THAT OUTPUT IS A REAL DELTA TABLE
        # ====================================================

        is_delta = (
            DeltaTable.isDeltaTable(
                spark,
                str(
                    DELTA_TEST_PATH
                ),
            )
        )

        if not is_delta:

            raise RuntimeError(
                "Spark wrote data, but the output "
                "was not recognized as a Delta table."
            )

        # ====================================================
        # READ DATA BACK FROM DELTA
        # ====================================================

        result_df = (
            spark.read
            .format("delta")
            .load(
                str(
                    DELTA_TEST_PATH
                )
            )
        )

        # ====================================================
        # VALIDATE ROW COUNT
        # ====================================================

        result_count = (
            result_df.count()
        )

        if result_count != 3:

            raise RuntimeError(
                "Unexpected Delta row count. "
                f"Expected 3, found {result_count}."
            )

        print()
        print(
            "Data read back from Delta:"
        )

        (
            result_df
            .orderBy(
                "transaction_id"
            )
            .show(
                truncate=False
            )
        )

        print()
        print(
            f"Delta location: {DELTA_TEST_PATH}"
        )

        print()
        print("=" * 72)
        print(
            "SPARK + DELTA LAKE VERIFICATION PASSED"
        )
        print("=" * 72)

    finally:

        # Always stop Spark cleanly, even if the test fails.
        spark.stop()


if __name__ == "__main__":
    main()