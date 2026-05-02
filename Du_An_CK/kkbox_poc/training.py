from __future__ import annotations

import pickle
import tempfile
import time

import mlflow
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier

from kkbox_poc.config import get_settings
from kkbox_poc.constants import (
    CATEGORICAL_FEATURE_COLUMNS,
    FEATURE_TABLE_NAME,
    MODEL_FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
)
from kkbox_poc.logging_utils import get_logger
from kkbox_poc.storage import configure_mlflow, read_sql_query

LOGGER = get_logger(__name__)


def _load_training_frame() -> pd.DataFrame:
    frame = read_sql_query(f"SELECT * FROM {FEATURE_TABLE_NAME}")
    if frame.empty:
        raise ValueError(f"Table {FEATURE_TABLE_NAME} is empty. Build features before training.")

    frame = frame.dropna(subset=["is_churn"]).copy()
    frame["is_churn"] = frame["is_churn"].astype(int)

    for column in NUMERIC_FEATURE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    for column in CATEGORICAL_FEATURE_COLUMNS:
        frame[column] = frame[column].fillna("unknown").astype(str)

    return frame


def _build_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))]),
                NUMERIC_FEATURE_COLUMNS,
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                CATEGORICAL_FEATURE_COLUMNS,
            ),
        ]
    )

    estimator = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        n_estimators=250,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.85,
        colsample_bytree=0.85,
        random_state=42,
        n_jobs=4,
    )

    return Pipeline(steps=[("preprocessor", preprocessor), ("model", estimator)])


def _wait_for_model_version(client: MlflowClient, model_name: str, version: str, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        version_info = client.get_model_version(model_name, version)
        if version_info.status == "READY":
            return
        time.sleep(2)
    raise TimeoutError(f"Model version {model_name}:{version} was not READY after {timeout_seconds} seconds.")


def train_and_register_model() -> dict[str, object]:
    settings = get_settings()
    configure_mlflow()

    frame = _load_training_frame()
    features = frame[MODEL_FEATURE_COLUMNS]
    target = frame["is_churn"]

    x_train, x_temp, y_train, y_temp = train_test_split(
        features,
        target,
        test_size=0.30,
        random_state=42,
        stratify=target,
    )
    x_valid, x_test, y_valid, y_test = train_test_split(
        x_temp,
        y_temp,
        test_size=0.50,
        random_state=42,
        stratify=y_temp,
    )

    pipeline = _build_pipeline()

    with mlflow.start_run(run_name="kkbox_xgboost_training") as run:
        pipeline.fit(x_train, y_train)

        valid_scores = pipeline.predict_proba(x_valid)[:, 1]
        test_scores = pipeline.predict_proba(x_test)[:, 1]

        metrics = {
            "valid_auc": float(roc_auc_score(y_valid, valid_scores)),
            "valid_logloss": float(log_loss(y_valid, valid_scores)),
            "test_auc": float(roc_auc_score(y_test, test_scores)),
            "test_logloss": float(log_loss(y_test, test_scores)),
        }
        params = pipeline.named_steps["model"].get_xgb_params()

        mlflow.log_params(params)
        mlflow.log_params(
            {
                "train_rows": len(x_train),
                "valid_rows": len(x_valid),
                "test_rows": len(x_test),
                "feature_count": len(MODEL_FEATURE_COLUMNS),
            }
        )
        mlflow.log_metrics(metrics)

        signature = infer_signature(x_valid, pipeline.predict_proba(x_valid))
        model_info = mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="model",
            signature=signature,
            input_example=x_valid.head(3),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = f"{temp_dir}/model.pkl"
            with open(model_path, "wb") as file_handle:
                pickle.dump(pipeline, file_handle)
            mlflow.log_artifact(model_path, artifact_path="pickle_artifact")

        registered_model = mlflow.register_model(model_uri=model_info.model_uri, name=settings.mlflow_model_name)
        client = MlflowClient()
        _wait_for_model_version(client, settings.mlflow_model_name, registered_model.version)
        client.transition_model_version_stage(
            name=settings.mlflow_model_name,
            version=registered_model.version,
            stage="Production",
            archive_existing_versions=True,
        )

    LOGGER.info(
        "Training completed. Model %s version %s promoted to Production.",
        settings.mlflow_model_name,
        registered_model.version,
    )
    return {
        "run_id": run.info.run_id,
        "model_name": settings.mlflow_model_name,
        "model_version": registered_model.version,
        **metrics,
    }


if __name__ == "__main__":
    train_and_register_model()
