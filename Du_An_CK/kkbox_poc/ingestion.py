from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from kkbox_poc.config import get_settings
from kkbox_poc.constants import RAW_TABLE_NAMES
from kkbox_poc.logging_utils import get_logger
from kkbox_poc.storage import execute_statements, get_engine

LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class CsvLoadSpec:
    csv_path: str
    table_name: str
    date_columns: tuple[str, ...]
    dtype_mapping: dict[str, str]
    chunk_size: int


def _parse_yyyymmdd(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype("string"), format="%Y%m%d", errors="coerce").dt.date


def _prepare_chunk(chunk: pd.DataFrame, date_columns: tuple[str, ...]) -> pd.DataFrame:
    prepared = chunk.copy()
    for column in date_columns:
        prepared[column] = _parse_yyyymmdd(prepared[column])
    return prepared


def _load_table(spec: CsvLoadSpec) -> int:
    csv_path = Path(spec.csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    rows_loaded = 0
    engine = get_engine()
    LOGGER.info("Loading %s into %s", csv_path, spec.table_name)

    for index, chunk in enumerate(
        pd.read_csv(
            csv_path,
            chunksize=spec.chunk_size,
            dtype=spec.dtype_mapping,
            low_memory=False,
        )
    ):
        prepared_chunk = _prepare_chunk(chunk, spec.date_columns)
        prepared_chunk.to_sql(
            spec.table_name,
            con=engine,
            if_exists="replace" if index == 0 else "append",
            index=False,
            method="multi",
            chunksize=5_000,
        )
        rows_loaded += len(prepared_chunk)
        LOGGER.info("Loaded %s rows into %s", rows_loaded, spec.table_name)

    return rows_loaded


def _create_indexes() -> None:
    statements = [
        f"CREATE INDEX IF NOT EXISTS idx_{RAW_TABLE_NAMES['train']}_msno ON {RAW_TABLE_NAMES['train']} (msno)",
        f"CREATE INDEX IF NOT EXISTS idx_{RAW_TABLE_NAMES['members']}_msno ON {RAW_TABLE_NAMES['members']} (msno)",
        (
            f"CREATE INDEX IF NOT EXISTS idx_{RAW_TABLE_NAMES['transactions']}_msno_transaction_date "
            f"ON {RAW_TABLE_NAMES['transactions']} (msno, transaction_date)"
        ),
        (
            f"CREATE INDEX IF NOT EXISTS idx_{RAW_TABLE_NAMES['user_logs']}_msno_date "
            f"ON {RAW_TABLE_NAMES['user_logs']} (msno, date)"
        ),
    ]
    execute_statements(statements)


def ingest_all() -> dict[str, int]:
    settings = get_settings()
    specs = [
        CsvLoadSpec(
            csv_path=settings.train_csv_path,
            table_name=RAW_TABLE_NAMES["train"],
            date_columns=(),
            dtype_mapping={"msno": "string", "is_churn": "int8"},
            chunk_size=50_000,
        ),
        CsvLoadSpec(
            csv_path=settings.members_csv_path,
            table_name=RAW_TABLE_NAMES["members"],
            date_columns=("registration_init_time",),
            dtype_mapping={
                "msno": "string",
                "city": "Int64",
                "bd": "Int64",
                "gender": "string",
                "registered_via": "Int64",
            },
            chunk_size=50_000,
        ),
        CsvLoadSpec(
            csv_path=settings.transactions_csv_path,
            table_name=RAW_TABLE_NAMES["transactions"],
            date_columns=("transaction_date", "membership_expire_date"),
            dtype_mapping={
                "msno": "string",
                "payment_method_id": "Int64",
                "payment_plan_days": "Int64",
                "plan_list_price": "float64",
                "actual_amount_paid": "float64",
                "is_auto_renew": "Int8",
                "is_cancel": "Int8",
            },
            chunk_size=100_000,
        ),
        CsvLoadSpec(
            csv_path=settings.user_logs_csv_path,
            table_name=RAW_TABLE_NAMES["user_logs"],
            date_columns=("date",),
            dtype_mapping={
                "msno": "string",
                "num_25": "float64",
                "num_50": "float64",
                "num_75": "float64",
                "num_985": "float64",
                "num_100": "float64",
                "num_unq": "float64",
                "total_secs": "float64",
            },
            chunk_size=200_000,
        ),
    ]

    results = {spec.table_name: _load_table(spec) for spec in specs}
    _create_indexes()
    LOGGER.info("Ingestion completed: %s", results)
    return results


if __name__ == "__main__":
    ingest_all()
