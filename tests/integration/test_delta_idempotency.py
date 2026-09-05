from __future__ import annotations


def test_delta_transaction_identifier_prevents_retry_duplicate(
    spark,
    tmp_path,
):

    output_path = (
        tmp_path
        / "idempotent_delta"
    )

    df = (
        spark.createDataFrame(
            [
                (
                    "txn_001",
                    100.0,
                ),
                (
                    "txn_002",
                    200.0,
                ),
            ],
            [
                "transaction_id",
                "amount",
            ],
        )
    )

    app_id = (
        "pytest-idempotency"
    )

    batch_id = 42

    # ========================================================
    # FIRST ATTEMPT
    # ========================================================

    (
        df.write

        .format(
            "delta"
        )

        .mode(
            "append"
        )

        .option(
            "txnAppId",
            app_id,
        )

        .option(
            "txnVersion",
            batch_id,
        )

        .save(
            str(
                output_path
            )
        )
    )

    # ========================================================
    # SIMULATED RETRY OF EXACT SAME MICRO-BATCH
    # ========================================================

    (
        df.write

        .format(
            "delta"
        )

        .mode(
            "append"
        )

        .option(
            "txnAppId",
            app_id,
        )

        .option(
            "txnVersion",
            batch_id,
        )

        .save(
            str(
                output_path
            )
        )
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

    # If idempotency were absent we would have four rows.
    assert result_df.count() == 2

    assert (
        result_df
        .select(
            "transaction_id"
        )
        .distinct()
        .count()
        == 2
    )