from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "kkbox")
    postgres_user: str = os.getenv("POSTGRES_USER", "kkbox")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "kkbox")

    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))

    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow_experiment_name: str = os.getenv("MLFLOW_EXPERIMENT_NAME", "kkbox-churn-poc")
    mlflow_model_name: str = os.getenv("MLFLOW_MODEL_NAME", "kkbox_churn_xgboost")
    mlflow_artifact_bucket: str = os.getenv("MLFLOW_ARTIFACT_BUCKET", "mlflow")

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_root_user: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_root_password: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    train_csv_path: str = os.getenv(
        "KKBOX_TRAIN_CSV",
        "/data/train_v2.csv/data/churn_comp_refresh/train_v2.csv",
    )
    members_csv_path: str = os.getenv(
        "KKBOX_MEMBERS_CSV",
        "/data/members_v3.csv/members_v3.csv",
    )
    transactions_csv_path: str = os.getenv(
        "KKBOX_TRANSACTIONS_CSV",
        "/data/transactions_v2.csv/data/churn_comp_refresh/transactions_v2.csv",
    )
    user_logs_csv_path: str = os.getenv(
        "KKBOX_USER_LOGS_CSV",
        "/data/user_logs_v2.csv/data/churn_comp_refresh/user_logs_v2.csv",
    )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
