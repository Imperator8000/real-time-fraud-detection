from __future__ import annotations

import json

from pathlib import Path

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SCHEMA = StructType(
    [
        StructField(
            "transaction_id",
            StringType(),
            False,
        ),

        StructField(
            "amount",
            IntegerType(),
            False,
        ),
    ]
)


def write_json_records(
    path: Path,
    filename: str,
    records: list[dict],
) -> None:

    path.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        path
        / filename
    )

    with output_file.open(
        "w",
        encoding="utf-8",
    ) as handle:

        for record in records:

            handle.write(
                json.dumps(
                    record
                )
                + "\n"
            )


def run_available_stream(
    *,
    spark,
    source_path: Path,
    output_path: Path,
    checkpoint_path: Path,
) -> None:

    stream_df = (
        spark.readStream
        .schema(
            SCHEMA
        )
        .json(
            str(
                source_path
            )
        )
    )

    query = (
        stream_df

        .writeStream

        .format(
            "delta"
        )

        .outputMode(
            "append"
        )

        .option(
            "checkpointLocation",
            str(
                checkpoint_path
            ),
        )

        .trigger(
            availableNow=True
        )

        .start(
            str(
                output_path
            )
        )
    )

    query.awaitTermination()


def test_stream_recovers_from_same_checkpoint(
    spark,
    tmp_path,
):

    source_path = (
        tmp_path
        / "source"
    )

    output_path = (
        tmp_path
        / "delta_output"
    )

    checkpoint_path = (
        tmp_path
        / "checkpoint"
    )

    # ========================================================
    # FIRST EXECUTION
    # ========================================================

    write_json_records(
        source_path,
        "batch_001.json",
        [
            {
                "transaction_id": (
                    "txn_001"
                ),
                "amount": 10,
            },
            {
                "transaction_id": (
                    "txn_002"
                ),
                "amount": 20,
            },
        ],
    )

    run_available_stream(
        spark=spark,
        source_path=source_path,
        output_path=output_path,
        checkpoint_path=checkpoint_path,
    )

    first_count = (
        spark.read
        .format(
            "delta"
        )
        .load(
            str(
                output_path
            )
        )
        .count()
    )

    assert first_count == 2

    # ========================================================
    # NEW DATA ARRIVES WHILE QUERY IS DOWN
    # ========================================================

    write_json_records(
        source_path,
        "batch_002.json",
        [
            {
                "transaction_id": (
                    "txn_003"
                ),
                "amount": 30,
            },
        ],
    )

    # ========================================================
    # RESTART USING SAME CHECKPOINT
    # ========================================================

    run_available_stream(
        spark=spark,
        source_path=source_path,
        output_path=output_path,
        checkpoint_path=checkpoint_path,
    )

    result_df = (
        spark.read
        .format(
            "delta"
        )
        .load(
            str(
                output_path
            )
        )
    )

    assert result_df.count() == 3

    assert (
        result_df
        .select(
            "transaction_id"
        )
        .distinct()
        .count()
        == 3
    )

    transaction_ids = {
        row.transaction_id

        for row in (
            result_df
            .select(
                "transaction_id"
            )
            .collect()
        )
    }

    assert transaction_ids == {
        "txn_001",
        "txn_002",
        "txn_003",
    }