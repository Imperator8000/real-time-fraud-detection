from __future__ import annotations

from pathlib import Path

from pyspark.ml import (
    PipelineModel,
)

from pyspark.ml.functions import (
    vector_to_array,
)

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================
# MODEL LOADING
# ============================================================

def load_fraud_model(
    model_path: Path | str,
) -> PipelineModel:
    """
    Load the complete fitted Spark ML PipelineModel.

    This includes:
        - boolean transformation
        - imputation
        - merchant indexing
        - one-hot encoding
        - VectorAssembler
        - Random Forest
    """

    return (
        PipelineModel.load(
            str(
                model_path
            )
        )
    )


# ============================================================
# DISTRIBUTED INFERENCE
# ============================================================

def apply_live_ml_inference(
    feature_df: DataFrame,
    model: PipelineModel,
) -> DataFrame:
    """
    Apply the fitted ML pipeline to a DataFrame.

    No fitting or retraining occurs here.

    The model creates:
        rawPrediction
        probability
        prediction

    We additionally expose the fraud-class probability as a
    normal scalar column.
    """

    predicted_df = (
        model.transform(
            feature_df
        )
    )

    return (
        predicted_df

        .withColumn(
            "ml_prediction",

            F.col(
                "prediction"
            ).cast(
                "integer"
            ),
        )

        .withColumn(
            "ml_fraud_probability",

            vector_to_array(
                F.col(
                    "probability"
                )
            ).getItem(
                1
            ),
        )
    )