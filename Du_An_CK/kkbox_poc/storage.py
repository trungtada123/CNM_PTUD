from __future__ import annotations

import os
from functools import lru_cache
from typing import Iterable

import mlflow
import pandas as pd
import redis
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from kkbox_poc.config import get_settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    settings = get_settings()
    return create_engine(settings.sqlalchemy_url, future=True, pool_pre_ping=True)


def dataframe_to_postgres(
    dataframe: pd.DataFrame,
    table_name: str,
    if_exists: str = "replace",
) -> None:
    engine = get_engine()
    safe_chunksize = min(5_000, max(1, 60_000 // max(1, len(dataframe.columns))))
    dataframe.to_sql(
        table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=safe_chunksize,
    )


def execute_statements(statements: Iterable[str]) -> None:
    engine = get_engine()
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def read_sql_query(query: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql_query(query, con=engine)


def get_redis_client() -> redis.Redis:
    settings = get_settings()
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


def configure_mlflow() -> None:
    settings = get_settings()
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = settings.minio_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = settings.minio_root_user
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.minio_root_password
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(settings.mlflow_experiment_name)
