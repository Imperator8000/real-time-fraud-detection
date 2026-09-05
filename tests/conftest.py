from __future__ import annotations

import pytest

from src.common.spark_session import (
    get_spark_session,
)


@pytest.fixture(
    scope="session"
)
def spark():
    """
    Shared SparkSession for PySpark tests.

    Creating Spark is relatively expensive, so the test suite
    reuses one session rather than creating a JVM for every test.
    """

    session = (
        get_spark_session(
            app_name=(
                "FraudDetection-Tests"
            )
        )
    )

    yield session

    session.stop()