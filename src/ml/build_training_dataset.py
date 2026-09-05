from __future__ import annotations

import sys

from pathlib import Path

from pyspark.sql import functions as F

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


from src.common.spark_session import (
    get_spark_session,
)


# ============================================================
# PATHS
# ============================================================

FRAUD_FEATURE_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "fraud_features"
)

GROUND_TRUTH_PATH = (
    PROJECT_ROOT
    / "data"
    / "training"
    / "ground_truth"
    / "transaction_labels.jsonl"
)

ML_TRAINING_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "ml_training_dataset"
)


# ============================================================
# LABEL SCHEMA
# ============================================================

GROUND_TRUTH_SCHEMA = StructType(
    [
        StructField(
            "transaction_id",
            StringType(),
            False,
        ),

        StructField(
            "user_id",
            StringType(),
            False,
        ),

        StructField(
            "event_timestamp",
            StringType(),
            False,
        ),

        StructField(
            "is_fraud_label",
            IntegerType(),
            False,
        ),

        StructField(
            "fraud_type",
            StringType(),
            True,
        ),

        StructField(
            "simulator_is_duplicate",
            BooleanType(),
            False,
        ),

        StructField(
            "simulator_lateness_seconds",
            IntegerType(),
            False,
        ),

        StructField(
            "label_written_at_utc",
            StringType(),
            False,
        ),
    ]
)


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "BuildMLTrainingDataset"
            )
        )
    )

    try:

        print("=" * 78)
        print(
            "BUILDING ML FRAUD TRAINING DATASET"
        )
        print("=" * 78)

        feature_df = (
            spark.read
            .format(
                "delta"
            )
            .load(
                str(
                    FRAUD_FEATURE_PATH
                )
            )
        )

        raw_labels_df = (
            spark.read
            .schema(
                GROUND_TRUTH_SCHEMA
            )
            .json(
                str(
                    GROUND_TRUTH_PATH
                )
            )
        )

        # ----------------------------------------------------
        # COLLAPSE SIMULATED DUPLICATES
        # ----------------------------------------------------
        #
        # The generator deliberately sends the same
        # transaction_id more than once in some scenarios.
        #
        # Silver removes those duplicates, so our training
        # labels also need one business label per transaction.
        # ----------------------------------------------------

        labels_df = (
            raw_labels_df

            .groupBy(
                "transaction_id"
            )

            .agg(
                F.max(
                    "is_fraud_label"
                ).cast(
                    "double"
                ).alias(
                    "label"
                ),

                F.first(
                    "fraud_type",
                    ignorenulls=True,
                ).alias(
                    "simulated_fraud_type"
                ),

                F.max(
                    F.col(
                        "simulator_is_duplicate"
                    ).cast(
                        "int"
                    )
                ).cast(
                    "boolean"
                ).alias(
                    "simulator_duplicate_seen"
                ),
            )
        )

        # ----------------------------------------------------
        # JOIN LABEL TO ENGINEERED FEATURES
        # ----------------------------------------------------
        #
        # Inner join is deliberate.
        #
        # Old transactions generated before this labeled run
        # have no simulator label and therefore cannot be used
        # for supervised training.
        # ----------------------------------------------------

        training_df = (
            feature_df

            .join(
                labels_df,
                on=(
                    "transaction_id"
                ),
                how="inner",
            )

            .withColumn(
                "training_dataset_created_at",
                F.current_timestamp(),
            )
        )

        training_count = (
            training_df.count()
        )

        positive_count = (
            training_df
            .filter(
                F.col("label")
                == 1.0
            )
            .count()
        )

        negative_count = (
            training_count
            - positive_count
        )

        if (
            positive_count == 0
            or negative_count == 0
        ):

            raise RuntimeError(
                "Training dataset must contain both "
                "fraud and non-fraud observations."
            )

        fraud_percentage = (
            positive_count
            / training_count
            * 100.0
        )

        print()
        print(
            f"Training rows:      "
            f"{training_count:,}"
        )

        print(
            f"Non-fraud rows:     "
            f"{negative_count:,}"
        )

        print(
            f"Fraud rows:         "
            f"{positive_count:,}"
        )

        print(
            f"Fraud percentage:   "
            f"{fraud_percentage:.2f}%"
        )

        print()
        print(
            "Window feature coverage:"
        )

        (
            training_df
            .groupBy(
                "window_feature_match_found"
            )
            .count()
            .show()
        )

        # ----------------------------------------------------
        # WRITE TRAINING TABLE
        # ----------------------------------------------------

        (
            training_df

            .write
            .format(
                "delta"
            )

            .mode(
                "overwrite"
            )

            .option(
                "overwriteSchema",
                "true",
            )

            .save(
                str(
                    ML_TRAINING_PATH
                )
            )
        )

        print()
        print(
            f"Training Delta table: "
            f"{ML_TRAINING_PATH}"
        )

        print()
        print("=" * 78)
        print(
            "ML TRAINING DATASET CREATED"
        )
        print("=" * 78)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()