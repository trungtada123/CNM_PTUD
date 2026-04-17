from __future__ import annotations

import os
from typing import Any

import bentoml
import mlflow
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from kkbox_poc.config import get_settings
from kkbox_poc.constants import CATEGORICAL_FEATURE_COLUMNS, MODEL_FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS
from kkbox_poc.storage import get_redis_client

svc = bentoml.Service("kkbox_churn_service")
app = FastAPI(title="KKBox Churn Bento Service")

MODEL: Any | None = None
MODEL_URI: str | None = None
REDIS_CLIENT = None


class PredictionRequest(BaseModel):
    msno: str


def _configure_mlflow() -> str:
    settings = get_settings()
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = settings.minio_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = settings.minio_root_user
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.minio_root_password
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    return f"models:/{settings.mlflow_model_name}/Production"


def _load_production_model() -> None:
    global MODEL, MODEL_URI
    MODEL_URI = _configure_mlflow()
    MODEL = mlflow.sklearn.load_model(MODEL_URI)


def _ensure_runtime_loaded() -> None:
    global REDIS_CLIENT
    if MODEL is None:
        _load_production_model()
    if REDIS_CLIENT is None:
        REDIS_CLIENT = get_redis_client()


def _parse_feature_mapping(raw_mapping: dict[str, str]) -> dict[str, object]:
    parsed: dict[str, object] = {}
    for column in NUMERIC_FEATURE_COLUMNS:
        parsed[column] = float(raw_mapping.get(column, "0") or 0)
    for column in CATEGORICAL_FEATURE_COLUMNS:
        parsed[column] = raw_mapping.get(column, "unknown") or "unknown"
    return parsed


def _fetch_features(msno: str) -> dict[str, object]:
    redis_key = f"kkbox:features:{msno}"
    raw_mapping = REDIS_CLIENT.hgetall(redis_key)
    if not raw_mapping:
        raise HTTPException(status_code=404, detail=f"Features not found in Redis for msno={msno}")
    return _parse_feature_mapping(raw_mapping)


@app.on_event("startup")
def startup_event() -> None:
    _ensure_runtime_loaded()


@app.get("/health")
def health() -> dict[str, object]:
    try:
        _ensure_runtime_loaded()
        return {
            "status": "ok",
            "model_uri": MODEL_URI,
        }
    except Exception as exc:
        return {
            "status": "error",
            "model_uri": MODEL_URI,
            "detail": str(exc),
        }


@app.post("/predict")
def predict(request: PredictionRequest) -> dict[str, object]:
    try:
        _ensure_runtime_loaded()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Production model is not loaded: {exc}") from exc

    features = _fetch_features(request.msno)
    frame = pd.DataFrame([{column: features[column] for column in MODEL_FEATURE_COLUMNS}])
    score = float(MODEL.predict_proba(frame)[:, 1][0])
    return {
        "msno": request.msno,
        "churn_probability": score,
        "churn_prediction": int(score >= 0.5),
        "model_uri": MODEL_URI,
        "features": features,
    }


svc.mount_asgi_app(app)
