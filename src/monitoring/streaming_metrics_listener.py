from __future__ import annotations

import json
import os
import re
import threading

from datetime import (
    datetime,
    timezone,
)

from pathlib import Path

from pyspark.sql.streaming import (
    StreamingQueryListener,
)


# ============================================================
# FILE HELPERS
# ============================================================

def safe_filename(
    value: str,
) -> str:
    """
    Convert query names into filesystem-safe names.
    """

    cleaned = re.sub(
        r"[^A-Za-z0-9_.-]+",
        "_",
        value,
    )

    return (
        cleaned.strip("_")
        or "unnamed_query"
    )


# ============================================================
# METRICS LISTENER
# ============================================================

class JsonlStreamingMetricsListener(
    StreamingQueryListener
):
    """
    Persist one JSON record for every completed Structured
    Streaming trigger.

    We deliberately write JSONL rather than opening another
    Spark/Delta job from inside the listener callback.

    The listener should remain lightweight and must not become
    part of the critical streaming workload itself.
    """

    def __init__(
        self,
        metrics_directory: Path,
    ) -> None:

        super().__init__()

        self.metrics_directory = (
            metrics_directory
        )

        self.metrics_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._write_lock = (
            threading.Lock()
        )

    # ========================================================
    # REQUIRED LISTENER METHODS
    # ========================================================

    def onQueryStarted(
        self,
        event,
    ) -> None:
        """
        No expensive work when a query starts.
        """

        pass

    def onQueryIdle(
        self,
        event,
    ) -> None:
        """
        Spark 3.5 requires this listener callback.

        We do not persist idle events because our benchmark and
        monitoring datasets focus on completed trigger progress.
        """

        pass

    def onQueryTerminated(
        self,
        event,
    ) -> None:
        """
        No expensive work when a query terminates.
        """

        pass

    # ========================================================
    # PROGRESS EVENT
    # ========================================================

    def onQueryProgress(
        self,
        event,
    ) -> None:
        """
        Flatten the most useful StreamingQueryProgress metrics
        while retaining the complete raw progress structure.
        """

        progress = json.loads(
            event.progress.json
        )

        duration_ms = (
            progress.get(
                "durationMs",
                {},
            )
            or {}
        )

        state_operators = (
            progress.get(
                "stateOperators",
                [],
            )
            or []
        )

        sources = (
            progress.get(
                "sources",
                [],
            )
            or []
        )

        # ----------------------------------------------------
        # AGGREGATED STATE METRICS
        # ----------------------------------------------------

        state_rows_total = sum(
            int(
                operator.get(
                    "numRowsTotal",
                    0,
                )
                or 0
            )
            for operator
            in state_operators
        )

        state_rows_updated = sum(
            int(
                operator.get(
                    "numRowsUpdated",
                    0,
                )
                or 0
            )
            for operator
            in state_operators
        )

        state_rows_removed = sum(
            int(
                operator.get(
                    "numRowsRemoved",
                    0,
                )
                or 0
            )
            for operator
            in state_operators
        )

        state_memory_bytes = sum(
            int(
                operator.get(
                    "memoryUsedBytes",
                    0,
                )
                or 0
            )
            for operator
            in state_operators
        )

        rows_dropped_by_watermark = sum(
            int(
                operator.get(
                    "numRowsDroppedByWatermark",
                    0,
                )
                or 0
            )
            for operator
            in state_operators
        )

        state_store_instances = sum(
            int(
                operator.get(
                    "numStateStoreInstances",
                    0,
                )
                or 0
            )
            for operator
            in state_operators
        )

        shuffle_partition_values = [
            int(
                operator.get(
                    "numShufflePartitions",
                    0,
                )
                or 0
            )
            for operator
            in state_operators
        ]

        state_shuffle_partitions = (
            max(
                shuffle_partition_values
            )
            if shuffle_partition_values
            else 0
        )

        # ----------------------------------------------------
        # SOURCE OFFSETS
        # ----------------------------------------------------

        source_start_offsets = [
            source.get(
                "startOffset"
            )
            for source
            in sources
        ]

        source_end_offsets = [
            source.get(
                "endOffset"
            )
            for source
            in sources
        ]

        # ----------------------------------------------------
        # FLATTENED RECORD
        # ----------------------------------------------------

        record = {
            "captured_at_utc": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),

            "query_id": (
                progress.get(
                    "id"
                )
            ),

            "run_id": (
                progress.get(
                    "runId"
                )
            ),

            "query_name": (
                progress.get(
                    "name"
                )
                or "unnamed_query"
            ),

            "batch_id": (
                progress.get(
                    "batchId"
                )
            ),

            "trigger_timestamp": (
                progress.get(
                    "timestamp"
                )
            ),

            # ------------------------------------------------
            # THROUGHPUT
            # ------------------------------------------------

            "num_input_rows": (
                progress.get(
                    "numInputRows",
                    0,
                )
            ),

            "input_rows_per_second": (
                progress.get(
                    "inputRowsPerSecond",
                    0.0,
                )
            ),

            "processed_rows_per_second": (
                progress.get(
                    "processedRowsPerSecond",
                    0.0,
                )
            ),

            # ------------------------------------------------
            # TRIGGER DURATION
            # ------------------------------------------------

            "batch_duration_ms": (
                progress.get(
                    "batchDuration",
                    0,
                )
            ),

            "trigger_execution_ms": (
                duration_ms.get(
                    "triggerExecution",
                    0,
                )
            ),

            "query_planning_ms": (
                duration_ms.get(
                    "queryPlanning",
                    0,
                )
            ),

            "get_batch_ms": (
                duration_ms.get(
                    "getBatch",
                    0,
                )
            ),

            "add_batch_ms": (
                duration_ms.get(
                    "addBatch",
                    0,
                )
            ),

            "wal_commit_ms": (
                duration_ms.get(
                    "walCommit",
                    0,
                )
            ),

            "commit_offsets_ms": (
                duration_ms.get(
                    "commitOffsets",
                    0,
                )
            ),

            # ------------------------------------------------
            # STATE
            # ------------------------------------------------

            "state_operator_count": (
                len(
                    state_operators
                )
            ),

            "state_rows_total": (
                state_rows_total
            ),

            "state_rows_updated": (
                state_rows_updated
            ),

            "state_rows_removed": (
                state_rows_removed
            ),

            "state_memory_bytes": (
                state_memory_bytes
            ),

            "state_memory_mb": (
                state_memory_bytes
                / 1024.0
                / 1024.0
            ),

            "rows_dropped_by_watermark": (
                rows_dropped_by_watermark
            ),

            "state_store_instances": (
                state_store_instances
            ),

            "state_shuffle_partitions": (
                state_shuffle_partitions
            ),

            # ------------------------------------------------
            # EVENT TIME / OFFSETS
            # ------------------------------------------------

            "event_time": (
                progress.get(
                    "eventTime",
                    {},
                )
            ),

            "source_start_offsets": (
                source_start_offsets
            ),

            "source_end_offsets": (
                source_end_offsets
            ),

            # ------------------------------------------------
            # KEEP COMPLETE SPARK PROGRESS
            # ------------------------------------------------

            "sources": (
                sources
            ),

            "state_operators": (
                state_operators
            ),

            "sink": (
                progress.get(
                    "sink",
                    {},
                )
            ),

            "raw_progress": (
                progress
            ),
        }

        # ----------------------------------------------------
        # ONE FILE PER QUERY RUN
        # ----------------------------------------------------

        query_name = safe_filename(
            str(
                record[
                    "query_name"
                ]
            )
        )

        run_id = safe_filename(
            str(
                record[
                    "run_id"
                ]
            )
        )

        output_file = (
            self.metrics_directory
            /
            f"{query_name}__{run_id}.jsonl"
        )

        serialized = (
            json.dumps(
                record,
                separators=(
                    ",",
                    ":",
                ),
                default=str,
            )
            + "\n"
        )

        # ----------------------------------------------------
        # THREAD-SAFE APPEND
        # ----------------------------------------------------

        with self._write_lock:

            with output_file.open(
                "a",
                encoding="utf-8",
            ) as handle:

                handle.write(
                    serialized
                )


# ============================================================
# REGISTRATION HELPER
# ============================================================

def attach_streaming_metrics_listener(
    spark,
    project_root: Path,
) -> JsonlStreamingMetricsListener:
    """
    Create and register the monitoring listener.
    """

    relative_directory = os.getenv(
        "STREAMING_METRICS_DIR",
        (
            "data/monitoring/"
            "streaming_progress"
        ),
    )

    metrics_directory = (
        project_root
        / relative_directory
    )

    listener = (
        JsonlStreamingMetricsListener(
            metrics_directory
        )
    )

    spark.streams.addListener(
        listener
    )

    return listener