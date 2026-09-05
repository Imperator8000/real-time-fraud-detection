from __future__ import annotations

import json
import math
import sys

from datetime import (
    datetime,
    timezone,
)

from pathlib import Path


from pyspark.ml import Pipeline

from pyspark.ml.classification import (
    RandomForestClassifier,
)

from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
)

from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    SQLTransformer,
    StringIndexer,
    VectorAssembler,
)

from pyspark.ml.functions import (
    vector_to_array,
)

from pyspark.sql import functions as F


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

TRAINING_DATA_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "ml_training_dataset"
)

MODEL_PATH = (
    PROJECT_ROOT
    / "models"
    / "fraud_random_forest"
)

METRICS_PATH = (
    PROJECT_ROOT
    / "models"
    / "fraud_random_forest_metrics.json"
)


# ============================================================
# RAW NUMERIC FEATURES
# ============================================================

NUMERIC_FEATURES = [
    "amount",
    "historical_count_before",
    "historical_mean_before",
    "historical_stddev_before",
    "amount_to_historical_avg_ratio",
    "amount_zscore",
    "seconds_since_previous_transaction",
    "historical_device_change_count",
    "transaction_count_10m",
    "total_spending_10m",
    "average_transaction_amount_10m",
    "maximum_transaction_amount_10m",
    "transaction_amount_stddev_10m",
    "unique_devices_10m",
    "unique_merchant_categories_10m",
    "transactions_per_minute",
    "spending_per_minute",
    "amount_coefficient_of_variation",
    "device_diversity_ratio",
    "merchant_diversity_ratio",
]


IMPUTED_FEATURES = [
    f"{column}_imputed"
    for column in NUMERIC_FEATURES
]


BOOLEAN_NUMERIC_FEATURES = [
    "historical_baseline_ready_num",
    "out_of_order_event_num",
    "device_changed_from_last_num",
    "high_velocity_flag_num",
    "multi_device_flag_num",
    "window_feature_match_found_num",
]


# ============================================================
# TIME-BASED TRAIN / TEST SPLIT
# ============================================================

def temporal_split(
    dataset,
):

    timestamped_df = (
        dataset
        .withColumn(
            "_event_epoch",
            F.col(
                "event_timestamp"
            ).cast(
                "long"
            ),
        )
    )

    cutoff_values = (
        timestamped_df
        .approxQuantile(
            "_event_epoch",
            [0.80],
            0.001,
        )
    )

    if not cutoff_values:

        raise RuntimeError(
            "Could not determine the temporal "
            "training/test cutoff."
        )

    cutoff_epoch = int(
        cutoff_values[0]
    )

    training_df = (
        timestamped_df
        .filter(
            F.col(
                "_event_epoch"
            )
            <= cutoff_epoch
        )
        .drop(
            "_event_epoch"
        )
    )

    testing_df = (
        timestamped_df
        .filter(
            F.col(
                "_event_epoch"
            )
            > cutoff_epoch
        )
        .drop(
            "_event_epoch"
        )
    )

    return (
        training_df,
        testing_df,
        cutoff_epoch,
    )


# ============================================================
# CLASS WEIGHTING
# ============================================================

def add_class_weights(
    training_df,
):

    total_count = (
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
        total_count
        - positive_count
    )

    if (
        positive_count == 0
        or negative_count == 0
    ):

        raise RuntimeError(
            "Training partition must contain "
            "both classes."
        )

    # --------------------------------------------------------
    # BALANCED CLASS WEIGHT
    # --------------------------------------------------------
    #
    # weight(class) =
    #
    #       total observations
    # ------------------------------
    # 2 × observations in that class
    #
    # This makes the minority fraud class matter more during
    # training without simply duplicating fraud rows.
    # --------------------------------------------------------

    positive_weight = (
        total_count
        / (
            2.0
            * positive_count
        )
    )

    negative_weight = (
        total_count
        / (
            2.0
            * negative_count
        )
    )

    weighted_df = (
        training_df

        .withColumn(
            "class_weight",

            F.when(
                F.col("label")
                == 1.0,

                F.lit(
                    positive_weight
                ),
            )
            .otherwise(
                F.lit(
                    negative_weight
                )
            ),
        )
    )

    return (
        weighted_df,
        positive_weight,
        negative_weight,
    )


# ============================================================
# ML PIPELINE
# ============================================================

def build_ml_pipeline() -> Pipeline:

    # --------------------------------------------------------
    # BOOLEAN → DOUBLE
    # --------------------------------------------------------
    #
    # Keeping this transformation inside the Pipeline means
    # exactly the same transformation will happen later during
    # streaming inference.
    # --------------------------------------------------------

    boolean_transformer = (
        SQLTransformer(
            statement="""
                SELECT
                    *,

                    CAST(
                        historical_baseline_ready
                        AS DOUBLE
                    )
                    AS historical_baseline_ready_num,

                    CAST(
                        out_of_order_event
                        AS DOUBLE
                    )
                    AS out_of_order_event_num,

                    CAST(
                        device_changed_from_last
                        AS DOUBLE
                    )
                    AS device_changed_from_last_num,

                    CAST(
                        high_velocity_flag
                        AS DOUBLE
                    )
                    AS high_velocity_flag_num,

                    CAST(
                        multi_device_flag
                        AS DOUBLE
                    )
                    AS multi_device_flag_num,

                    CAST(
                        window_feature_match_found
                        AS DOUBLE
                    )
                    AS window_feature_match_found_num

                FROM __THIS__
            """
        )
    )

    # --------------------------------------------------------
    # NUMERIC NULL HANDLING
    # --------------------------------------------------------
    #
    # Historical features are legitimately null during a user's
    # cold-start period.
    #
    # Median values are learned on the TRAINING data and stored
    # inside the fitted PipelineModel.
    # --------------------------------------------------------

    imputer = (
        Imputer(
            inputCols=(
                NUMERIC_FEATURES
            ),

            outputCols=(
                IMPUTED_FEATURES
            ),

            strategy="median",
        )
    )

    # --------------------------------------------------------
    # CATEGORICAL MERCHANT FEATURE
    # --------------------------------------------------------

    merchant_indexer = (
        StringIndexer(
            inputCol=(
                "merchant_category"
            ),

            outputCol=(
                "merchant_category_index"
            ),

            handleInvalid="keep",

            stringOrderType=(
                "frequencyDesc"
            ),
        )
    )

    merchant_encoder = (
        OneHotEncoder(
            inputCol=(
                "merchant_category_index"
            ),

            outputCol=(
                "merchant_category_ohe"
            ),

            handleInvalid="keep",

            dropLast=False,
        )
    )

    # --------------------------------------------------------
    # VECTOR ASSEMBLY
    # --------------------------------------------------------

    assembler = (
        VectorAssembler(
            inputCols=(
                IMPUTED_FEATURES
                + BOOLEAN_NUMERIC_FEATURES
                + [
                    "merchant_category_ohe"
                ]
            ),

            outputCol=(
                "features"
            ),

            handleInvalid="keep",
        )
    )

    # --------------------------------------------------------
    # RANDOM FOREST
    # --------------------------------------------------------

    classifier = (
        RandomForestClassifier(
            labelCol="label",
            featuresCol="features",

            predictionCol="prediction",

            probabilityCol=(
                "probability"
            ),

            rawPredictionCol=(
                "rawPrediction"
            ),

            weightCol=(
                "class_weight"
            ),

            numTrees=200,

            maxDepth=10,

            maxBins=64,

            featureSubsetStrategy=(
                "sqrt"
            ),

            subsamplingRate=0.80,

            impurity="gini",

            seed=42,
        )
    )

    return Pipeline(
        stages=[
            boolean_transformer,
            imputer,
            merchant_indexer,
            merchant_encoder,
            assembler,
            classifier,
        ]
    )


# ============================================================
# EVALUATION
# ============================================================

def evaluate_predictions(
    predictions,
) -> dict:

    roc_evaluator = (
        BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol=(
                "rawPrediction"
            ),
            metricName=(
                "areaUnderROC"
            ),
        )
    )

    pr_evaluator = (
        BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol=(
                "rawPrediction"
            ),
            metricName=(
                "areaUnderPR"
            ),
        )
    )

    roc_auc = float(
        roc_evaluator.evaluate(
            predictions
        )
    )

    pr_auc = float(
        pr_evaluator.evaluate(
            predictions
        )
    )

    confusion = (
        predictions

        .agg(
            F.sum(
                F.when(
                    (
                        F.col("label")
                        == 1.0
                    )
                    &
                    (
                        F.col("prediction")
                        == 1.0
                    ),
                    1,
                ).otherwise(
                    0
                )
            ).alias(
                "tp"
            ),

            F.sum(
                F.when(
                    (
                        F.col("label")
                        == 0.0
                    )
                    &
                    (
                        F.col("prediction")
                        == 1.0
                    ),
                    1,
                ).otherwise(
                    0
                )
            ).alias(
                "fp"
            ),

            F.sum(
                F.when(
                    (
                        F.col("label")
                        == 1.0
                    )
                    &
                    (
                        F.col("prediction")
                        == 0.0
                    ),
                    1,
                ).otherwise(
                    0
                )
            ).alias(
                "fn"
            ),

            F.sum(
                F.when(
                    (
                        F.col("label")
                        == 0.0
                    )
                    &
                    (
                        F.col("prediction")
                        == 0.0
                    ),
                    1,
                ).otherwise(
                    0
                )
            ).alias(
                "tn"
            ),
        )

        .first()
    )

    tp = int(
        confusion["tp"]
    )

    fp = int(
        confusion["fp"]
    )

    fn = int(
        confusion["fn"]
    )

    tn = int(
        confusion["tn"]
    )

    precision = (
        tp
        / (tp + fp)
        if (
            tp + fp
        ) > 0
        else 0.0
    )

    recall = (
        tp
        / (tp + fn)
        if (
            tp + fn
        ) > 0
        else 0.0
    )

    f1 = (
        2
        * precision
        * recall
        / (
            precision
            + recall
        )
        if (
            precision
            + recall
        ) > 0
        else 0.0
    )

    accuracy = (
        (
            tp + tn
        )
        /
        (
            tp
            + tn
            + fp
            + fn
        )
    )

    return {
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,

        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,

        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "true_negatives": tn,
    }


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "RandomForestTraining"
            )
        )
    )

    training_df = None
    testing_df = None

    try:

        print("=" * 78)
        print(
            "SPARK MLLIB RANDOM FOREST FRAUD MODEL"
        )
        print("=" * 78)

        dataset = (
            spark.read
            .format(
                "delta"
            )
            .load(
                str(
                    TRAINING_DATA_PATH
                )
            )
        )

        (
            training_df,
            testing_df,
            cutoff_epoch,
        ) = temporal_split(
            dataset
        )

        training_df = (
            training_df.cache()
        )

        testing_df = (
            testing_df.cache()
        )

        training_count = (
            training_df.count()
        )

        testing_count = (
            testing_df.count()
        )

        print()
        print(
            f"Training rows: "
            f"{training_count:,}"
        )

        print(
            f"Testing rows:  "
            f"{testing_count:,}"
        )

        if (
            training_count == 0
            or testing_count == 0
        ):

            raise RuntimeError(
                "Temporal split produced an empty "
                "training or testing partition."
            )

        # Confirm both classes exist in the test period too.
        testing_classes = {
            float(
                row["label"]
            )
            for row in (
                testing_df
                .select(
                    "label"
                )
                .distinct()
                .collect()
            )
        }

        if testing_classes != {
            0.0,
            1.0,
        }:

            raise RuntimeError(
                "Testing period does not contain "
                "both fraud classes. Generate more "
                "labeled transactions."
            )

        (
            weighted_training_df,
            positive_weight,
            negative_weight,
        ) = add_class_weights(
            training_df
        )

        print()
        print(
            "Class weighting:"
        )

        print(
            f"  Fraud weight:     "
            f"{positive_weight:.4f}"
        )

        print(
            f"  Non-fraud weight: "
            f"{negative_weight:.4f}"
        )

        pipeline = (
            build_ml_pipeline()
        )

        print()
        print(
            "Training Random Forest..."
        )

        model = (
            pipeline.fit(
                weighted_training_df
            )
        )

        print(
            "Model training completed."
        )

        predictions = (
            model.transform(
                testing_df
            )

            .withColumn(
                "fraud_probability",

                vector_to_array(
                    "probability"
                )[1],
            )
        )

        metrics = (
            evaluate_predictions(
                predictions
            )
        )

        cutoff_utc = (
            datetime.fromtimestamp(
                cutoff_epoch,
                tz=timezone.utc,
            ).isoformat()
        )

        metrics.update(
            {
                "training_rows": (
                    training_count
                ),

                "testing_rows": (
                    testing_count
                ),

                "temporal_split_cutoff_utc": (
                    cutoff_utc
                ),

                "positive_class_weight": (
                    positive_weight
                ),

                "negative_class_weight": (
                    negative_weight
                ),

                "num_trees": 200,

                "max_depth": 10,

                "feature_subset_strategy": (
                    "sqrt"
                ),
            }
        )

        print()
        print("=" * 78)
        print(
            "MODEL EVALUATION"
        )
        print("=" * 78)

        print(
            f"ROC-AUC:    "
            f"{metrics['roc_auc']:.4f}"
        )

        print(
            f"PR-AUC:     "
            f"{metrics['pr_auc']:.4f}"
        )

        print(
            f"Precision:  "
            f"{metrics['precision']:.4f}"
        )

        print(
            f"Recall:     "
            f"{metrics['recall']:.4f}"
        )

        print(
            f"F1:         "
            f"{metrics['f1']:.4f}"
        )

        print(
            f"Accuracy:   "
            f"{metrics['accuracy']:.4f}"
        )

        print()
        print(
            "Confusion Matrix"
        )

        print(
            "                Predicted"
        )

        print(
            "              Normal   Fraud"
        )

        print(
            "Actual Normal "
            f"{metrics['true_negatives']:>7,} "
            f"{metrics['false_positives']:>7,}"
        )

        print(
            "Actual Fraud  "
            f"{metrics['false_negatives']:>7,} "
            f"{metrics['true_positives']:>7,}"
        )

        # ----------------------------------------------------
        # SAVE COMPLETE PIPELINE MODEL
        # ----------------------------------------------------

        MODEL_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        (
            model
            .write()
            .overwrite()
            .save(
                str(
                    MODEL_PATH
                )
            )
        )

        with METRICS_PATH.open(
            "w",
            encoding="utf-8",
        ) as metrics_file:

            json.dump(
                metrics,
                metrics_file,
                indent=2,
            )

        print()
        print(
            f"Model saved:   "
            f"{MODEL_PATH}"
        )

        print(
            f"Metrics saved: "
            f"{METRICS_PATH}"
        )

        print()
        print(
            "Highest-probability test transactions:"
        )

        (
            predictions

            .select(
                "transaction_id",
                "user_id",
                "amount",
                "label",
                "prediction",
                "fraud_probability",
                "simulated_fraud_type",
            )

            .orderBy(
                F.desc(
                    "fraud_probability"
                )
            )

            .show(
                20,
                truncate=False,
            )
        )

        print()
        print("=" * 78)
        print(
            "RANDOM FOREST TRAINING COMPLETED"
        )
        print("=" * 78)

    finally:

        if training_df is not None:
            training_df.unpersist()

        if testing_df is not None:
            testing_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()