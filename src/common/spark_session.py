from __future__ import annotations

import os

from pathlib import Path
from urllib.parse import urlparse

from delta import (
    configure_spark_with_delta_pip,
)

from dotenv import (
    load_dotenv,
)

from pyspark.sql import (
    SparkSession,
)


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)


# ============================================================
# ENVIRONMENT
# ============================================================
#
# Load project-level environment variables from:
#
#     <project-root>/.env
#
# override=False means an environment variable supplied by
# Docker, the operating system, CI, etc. takes precedence over
# the value stored in .env.
#
# This is useful because Docker Compose can override settings
# such as:
#
#     SPARK_MASTER
#     SPARK_SQL_SHUFFLE_PARTITIONS
#     KAFKA_INTERNAL_BOOTSTRAP_SERVERS
#
# without modifying application code.
# ============================================================

load_dotenv(
    dotenv_path=(
        PROJECT_ROOT
        / ".env"
    ),
    override=False,
)


# ============================================================
# BOOLEAN ENVIRONMENT HELPER
# ============================================================

def get_boolean_env(
    name: str,
    default: bool,
) -> bool:
    """
    Read a boolean environment variable safely.

    Accepted true values:
        true
        1
        yes
        y
        on

    Accepted false values:
        false
        0
        no
        n
        off
    """

    raw_value = os.getenv(
        name
    )

    if raw_value is None:
        return default

    normalized = (
        raw_value
        .strip()
        .lower()
    )

    if normalized in {
        "true",
        "1",
        "yes",
        "y",
        "on",
    }:
        return True

    if normalized in {
        "false",
        "0",
        "no",
        "n",
        "off",
    }:
        return False

    raise ValueError(
        f"Environment variable {name} "
        f"must contain a boolean value. "
        f"Received: {raw_value!r}"
    )


# ============================================================
# FILE EVENT-LOG DIRECTORY
# ============================================================

def ensure_event_log_directory(
    event_log_uri: str,
) -> None:
    """
    Create the Spark event-log directory when the configured
    URI points to the local filesystem.

    Example:

        file:///workspace/logs/spark-events

    Spark expects the event-log directory to exist before the
    SparkContext starts.

    Non-file schemes such as:

        hdfs://...
        s3a://...
        abfss://...

    are deliberately left untouched.
    """

    parsed = urlparse(
        event_log_uri
    )

    # --------------------------------------------------------
    # EXPLICIT FILE URI
    # --------------------------------------------------------

    if parsed.scheme == "file":

        local_path = Path(
            parsed.path
        )

        local_path.mkdir(
            parents=True,
            exist_ok=True,
        )

        return

    # --------------------------------------------------------
    # PLAIN LOCAL FILESYSTEM PATH
    # --------------------------------------------------------

    if parsed.scheme == "":

        local_path = Path(
            event_log_uri
        )

        local_path.mkdir(
            parents=True,
            exist_ok=True,
        )


# ============================================================
# PACKAGE NORMALIZATION
# ============================================================

def normalize_extra_packages(
    extra_packages: list[str] | tuple[str, ...] | None,
) -> list[str]:
    """
    Remove empty package names and duplicate Maven coordinates
    while preserving the supplied order.

    Example Kafka package:

        org.apache.spark:
        spark-sql-kafka-0-10_2.12:
        3.5.9
    """

    if not extra_packages:
        return []

    normalized_packages: list[str] = []

    seen: set[str] = set()

    for package in extra_packages:

        package = (
            package.strip()
        )

        if not package:
            continue

        if package in seen:
            continue

        normalized_packages.append(
            package
        )

        seen.add(
            package
        )

    return normalized_packages


# ============================================================
# SPARK SESSION FACTORY
# ============================================================

def get_spark_session(
    app_name: str,
    master: str | None = None,
    extra_packages: list[str] | tuple[str, ...] | None = None,
) -> SparkSession:
    """
    Build and return the standard SparkSession used throughout
    the Real-Time Financial Fraud Detection project.

    The factory configures:

        - Apache Spark 3.5.x
        - Delta Lake
        - Kryo serialization
        - Apache Arrow
        - UTC timestamps
        - Adaptive Query Execution
        - configurable shuffle partitions
        - configurable default parallelism
        - Structured Streaming metrics
        - persistent Spark event logs
        - Spark UI
        - optional external Maven packages such as Kafka

    Parameters
    ----------
    app_name:
        Human-readable Spark application name.

    master:
        Optional explicit Spark master.

        If omitted, SPARK_MASTER is read from the environment.

        Default:
            local[*]

    extra_packages:
        Optional Maven coordinates that should be downloaded
        alongside Delta Lake dependencies.

        Example:

            [
                "org.apache.spark:"
                "spark-sql-kafka-0-10_2.12:"
                "3.5.9"
            ]

    Returns
    -------
    SparkSession
        Configured SparkSession.
    """

    # ========================================================
    # CORE CONFIGURATION
    # ========================================================

    resolved_master = (
        master
        or os.getenv(
            "SPARK_MASTER",
            "local[*]",
        )
    )

    shuffle_partitions = os.getenv(
        "SPARK_SQL_SHUFFLE_PARTITIONS",
        "8",
    )

    default_parallelism = os.getenv(
        "SPARK_DEFAULT_PARALLELISM",
        "8",
    )

    spark_log_level = os.getenv(
        "SPARK_LOG_LEVEL",
        "WARN",
    )

    # ========================================================
    # STREAMING OBSERVABILITY
    # ========================================================

    streaming_metrics_enabled = (
        get_boolean_env(
            "SPARK_STREAMING_METRICS_ENABLED",
            True,
        )
    )

    recent_progress_updates = os.getenv(
        "SPARK_STREAMING_RECENT_PROGRESS",
        "200",
    )

    # ========================================================
    # SPARK EVENT LOGGING
    # ========================================================

    event_log_enabled = (
        get_boolean_env(
            "SPARK_EVENT_LOG_ENABLED",
            True,
        )
    )

    event_log_directory = os.getenv(
        "SPARK_EVENT_LOG_DIR",
        "file:///workspace/logs/spark-events",
    )

    event_log_compression = (
        get_boolean_env(
            "SPARK_EVENT_LOG_COMPRESS",
            True,
        )
    )

    # Spark needs a local event-log directory to exist before
    # SparkContext initialization.

    if event_log_enabled:

        ensure_event_log_directory(
            event_log_directory
        )

    # ========================================================
    # OPTIONAL MAVEN PACKAGES
    # ========================================================

    packages = (
        normalize_extra_packages(
            extra_packages
        )
    )

    # ========================================================
    # SPARK BUILDER
    # ========================================================

    builder = (
        SparkSession.builder

        # ----------------------------------------------------
        # APPLICATION IDENTITY
        # ----------------------------------------------------

        .appName(
            app_name
        )

        .master(
            resolved_master
        )

        # ----------------------------------------------------
        # DELTA LAKE
        # ----------------------------------------------------
        #
        # These settings activate Delta's Spark SQL extension
        # and replace the default Spark catalog with Delta's
        # catalog implementation.
        # ----------------------------------------------------

        .config(
            "spark.sql.extensions",
            (
                "io.delta.sql."
                "DeltaSparkSessionExtension"
            ),
        )

        .config(
            "spark.sql.catalog.spark_catalog",
            (
                "org.apache.spark.sql.delta.catalog."
                "DeltaCatalog"
            ),
        )

        # ----------------------------------------------------
        # SHUFFLE / PARALLELISM
        # ----------------------------------------------------

        .config(
            "spark.sql.shuffle.partitions",
            shuffle_partitions,
        )

        .config(
            "spark.default.parallelism",
            default_parallelism,
        )

        # ----------------------------------------------------
        # SERIALIZATION
        # ----------------------------------------------------
        #
        # Kryo is generally more compact and efficient than
        # standard Java serialization for Spark workloads.
        # ----------------------------------------------------

        .config(
            "spark.serializer",
            (
                "org.apache.spark.serializer."
                "KryoSerializer"
            ),
        )

        # ----------------------------------------------------
        # APACHE ARROW
        # ----------------------------------------------------
        #
        # Arrow is especially important for our:
        #
        #     applyInPandasWithState
        #
        # workload because Spark exchanges columnar data with
        # Python/Pandas.
        # ----------------------------------------------------

        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "true",
        )

        .config(
            "spark.sql.execution.arrow.pyspark.fallback.enabled",
            "true",
        )

        # ----------------------------------------------------
        # TIMEZONE
        # ----------------------------------------------------
        #
        # Event-time calculations should remain consistent
        # regardless of the machine/container timezone.
        # ----------------------------------------------------

        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )

        # ----------------------------------------------------
        # ADAPTIVE QUERY EXECUTION
        # ----------------------------------------------------

        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )

        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )

        # ----------------------------------------------------
        # SPARK UI
        # ----------------------------------------------------

        .config(
            "spark.ui.enabled",
            "true",
        )

        # ----------------------------------------------------
        # STRUCTURED STREAMING METRICS
        # ----------------------------------------------------
        #
        # Enables Spark's native streaming metrics, including:
        #
        #     inputRowsPerSecond
        #     processedRowsPerSecond
        #     trigger durations
        #     state-store rows
        #     state-store memory
        #
        # Our StreamingQueryListener added in Step 14 persists
        # selected progress information to JSONL.
        # ----------------------------------------------------

        .config(
            "spark.sql.streaming.metricsEnabled",
            str(
                streaming_metrics_enabled
            ).lower(),
        )

        .config(
            "spark.sql.streaming.numRecentProgressUpdates",
            recent_progress_updates,
        )

        # ----------------------------------------------------
        # SPARK EVENT LOGGING
        # ----------------------------------------------------
        #
        # These files are consumed by Spark History Server.
        #
        # Unlike the live Spark UI on port 4040, event logs
        # remain available after an application terminates.
        # ----------------------------------------------------

        .config(
            "spark.eventLog.enabled",
            str(
                event_log_enabled
            ).lower(),
        )

        .config(
            "spark.eventLog.dir",
            event_log_directory,
        )

        .config(
            "spark.eventLog.compress",
            str(
                event_log_compression
            ).lower(),
        )
    )

    # ========================================================
    # DELTA PACKAGE CONFIGURATION
    # ========================================================
    #
    # configure_spark_with_delta_pip() adds the Delta Lake
    # Maven package required by the installed delta-spark
    # Python package.
    #
    # Any extra packages—such as Spark's Kafka connector—are
    # supplied at the same time so Spark/Ivy resolves all
    # dependencies during JVM startup.
    # ========================================================

    if packages:

        spark = (
            configure_spark_with_delta_pip(
                builder,
                extra_packages=(
                    packages
                ),
            )
            .getOrCreate()
        )

    else:

        spark = (
            configure_spark_with_delta_pip(
                builder
            )
            .getOrCreate()
        )

    # ========================================================
    # RUNTIME LOG LEVEL
    # ========================================================

    spark.sparkContext.setLogLevel(
        spark_log_level
    )

    return spark