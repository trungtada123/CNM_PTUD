from __future__ import annotations

from datetime import date, datetime

from kkbox_poc.constants import FEATURE_TABLE_NAME, MODEL_FEATURE_COLUMNS
from kkbox_poc.logging_utils import get_logger
from kkbox_poc.storage import get_redis_client, read_sql_query

LOGGER = get_logger(__name__)


def _normalize_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def materialize_latest_features() -> dict[str, int]:
    feature_frame = read_sql_query(f"SELECT msno, snapshot_date, {', '.join(MODEL_FEATURE_COLUMNS)} FROM {FEATURE_TABLE_NAME}")
    if feature_frame.empty:
        raise ValueError(f"Table {FEATURE_TABLE_NAME} is empty. Build features before pushing to Redis.")

    redis_client = get_redis_client()
    rows_written = 0
    with redis_client.pipeline(transaction=False) as pipeline:
        for record in feature_frame.to_dict(orient="records"):
            msno = record["msno"]
            redis_key = f"kkbox:features:{msno}"
            mapping = {key: _normalize_value(value) for key, value in record.items() if key != "msno"}
            pipeline.hset(redis_key, mapping=mapping)
            rows_written += 1
        pipeline.execute()

    LOGGER.info("Materialized %s feature rows into Redis.", rows_written)
    return {"rows_written": rows_written}


if __name__ == "__main__":
    materialize_latest_features()
