from __future__ import annotations

from datetime import date

import polars as pl

from kkbox_poc.constants import FEATURE_TABLE_NAME, NUMERIC_FEATURE_COLUMNS, RAW_TABLE_NAMES
from kkbox_poc.logging_utils import get_logger
from kkbox_poc.storage import dataframe_to_postgres, get_engine

LOGGER = get_logger(__name__)


def load_raw_tables() -> dict[str, pl.DataFrame]:
    engine = get_engine()
    with engine.connect() as connection:
        train = pl.read_database(f"SELECT msno, is_churn FROM {RAW_TABLE_NAMES['train']}", connection)
        members = pl.read_database(
            (
                "SELECT msno, city, bd, gender, registration_init_time "
                f"FROM {RAW_TABLE_NAMES['members']}"
            ),
            connection,
        )
        transactions = pl.read_database(
            (
                "SELECT msno, payment_method_id, is_auto_renew, transaction_date, "
                f"membership_expire_date, is_cancel FROM {RAW_TABLE_NAMES['transactions']}"
            ),
            connection,
        )
        user_logs = pl.read_database(
            (
                "SELECT msno, date, num_25, num_50, num_75, num_985, num_100, total_secs "
                f"FROM {RAW_TABLE_NAMES['user_logs']}"
            ),
            connection,
        )
    return {
        "train": train,
        "members": members,
        "transactions": transactions,
        "user_logs": user_logs,
    }


def resolve_reference_date(transactions: pl.DataFrame, user_logs: pl.DataFrame) -> date:
    max_transaction_date = transactions.select(pl.col("transaction_date").max()).item()
    max_expire_date = transactions.select(pl.col("membership_expire_date").max()).item()
    max_log_date = user_logs.select(pl.col("date").max()).item()
    candidates = [value for value in [max_transaction_date, max_expire_date, max_log_date] if value is not None]
    if not candidates:
        raise ValueError("Cannot resolve a reference date from transactions or user_logs.")
    return max(candidates)


def _build_member_features(members: pl.DataFrame) -> pl.DataFrame:
    return members.select(
        [
            "msno",
            pl.col("city").cast(pl.Int64, strict=False).cast(pl.Utf8).fill_null("unknown").alias("city"),
            (
                pl.when(pl.col("bd").is_between(10, 100))
                .then(pl.col("bd").cast(pl.Float64))
                .otherwise(None)
            ).alias("age"),
            (
                pl.when(pl.col("gender").fill_null("unknown").str.to_lowercase().is_in(["male", "female"]))
                .then(pl.col("gender").fill_null("unknown").str.to_lowercase())
                .otherwise(pl.lit("unknown"))
            ).alias("gender"),
        ]
    )


def _build_log_features(user_logs: pl.DataFrame, reference_date: date) -> pl.DataFrame:
    logs = user_logs.with_columns(
        [
            (pl.lit(reference_date) - pl.col("date")).dt.total_days().alias("days_ago"),
            (
                pl.col("num_25").fill_null(0)
                + pl.col("num_50").fill_null(0)
                + pl.col("num_75").fill_null(0)
                + pl.col("num_985").fill_null(0)
                + pl.col("num_100").fill_null(0)
            ).alias("total_tracks"),
        ]
    )

    return (
        logs.group_by("msno")
        .agg(
            [
                pl.col("date").filter(pl.col("days_ago").is_between(0, 6)).n_unique().alias("listen_days_7d"),
                pl.col("date").filter(pl.col("days_ago").is_between(0, 13)).n_unique().alias("listen_days_14d"),
                pl.col("date").filter(pl.col("days_ago").is_between(0, 29)).n_unique().alias("listen_days_30d"),
                pl.col("num_25").filter(pl.col("days_ago").is_between(0, 29)).sum().alias("num_25_30d"),
                pl.col("num_50").filter(pl.col("days_ago").is_between(0, 29)).sum().alias("num_50_30d"),
                pl.col("num_100").filter(pl.col("days_ago").is_between(0, 29)).sum().alias("num_100_30d"),
                pl.col("total_secs").filter(pl.col("days_ago").is_between(0, 29)).sum().alias("total_secs_30d"),
                pl.col("total_tracks").filter(pl.col("days_ago").is_between(0, 29)).sum().alias("total_tracks_30d"),
                pl.col("total_secs").filter(pl.col("days_ago").is_between(0, 6)).sum().alias("total_secs_last_7d"),
                pl.col("total_secs").filter(pl.col("days_ago").is_between(7, 13)).sum().alias("total_secs_prev_7d"),
                pl.col("date").max().alias("last_listen_date"),
            ]
        )
        .with_columns(
            [
                (
                    pl.when(pl.col("total_tracks_30d") > 0)
                    .then(pl.col("num_100_30d") / pl.col("total_tracks_30d"))
                    .otherwise(0.0)
                ).alias("completion_ratio_30d"),
                (
                    pl.when(pl.col("total_secs_prev_7d") > 0)
                    .then((pl.col("total_secs_prev_7d") - pl.col("total_secs_last_7d")) / pl.col("total_secs_prev_7d"))
                    .otherwise(0.0)
                ).alias("engagement_drop_ratio"),
                (pl.lit(reference_date) - pl.col("last_listen_date")).dt.total_days().alias("days_since_last_listen"),
            ]
        )
        .drop(["total_tracks_30d", "total_secs_last_7d", "total_secs_prev_7d", "last_listen_date"])
    )


def _build_transaction_features(transactions: pl.DataFrame, reference_date: date) -> pl.DataFrame:
    payment_mode = (
        transactions.group_by(["msno", "payment_method_id"])
        .len()
        .sort(["msno", "len", "payment_method_id"], descending=[False, True, False])
        .group_by("msno")
        .first()
        .select(
            [
                "msno",
                pl.col("payment_method_id").cast(pl.Utf8).fill_null("unknown").alias("dominant_payment_method"),
            ]
        )
    )

    latest_status = (
        transactions.sort(["msno", "transaction_date"], descending=[False, True])
        .group_by("msno")
        .first()
        .select(
            [
                "msno",
                pl.col("is_auto_renew").fill_null(0).cast(pl.Int64).alias("latest_is_auto_renew"),
            ]
        )
    )

    summary = (
        transactions.group_by("msno")
        .agg(
            [
                pl.len().alias("transaction_count"),
                pl.col("transaction_date").min().alias("first_transaction_date"),
                pl.col("membership_expire_date").max().alias("latest_membership_expire_date"),
                pl.col("is_cancel").fill_null(0).max().cast(pl.Int64).alias("had_cancel"),
            ]
        )
        .with_columns(
            [
                (
                    pl.when(pl.col("first_transaction_date").is_not_null())
                    .then(
                        (
                            pl.coalesce([pl.col("latest_membership_expire_date"), pl.lit(reference_date)])
                            - pl.col("first_transaction_date")
                        ).dt.total_days()
                        / 30.0
                    )
                    .otherwise(0.0)
                ).alias("service_months")
            ]
        )
        .select(["msno", "transaction_count", "service_months", "had_cancel"])
    )

    return summary.join(payment_mode, on="msno", how="left").join(latest_status, on="msno", how="left")


def build_feature_snapshot_from_frames(
    train: pl.DataFrame,
    members: pl.DataFrame,
    transactions: pl.DataFrame,
    user_logs: pl.DataFrame,
    reference_date: date | None = None,
) -> tuple[pl.DataFrame, date]:
    snapshot_date = reference_date or resolve_reference_date(transactions, user_logs)
    LOGGER.info("Building feature snapshot for reference date %s", snapshot_date)

    feature_frame = (
        train.join(_build_member_features(members), on="msno", how="left")
        .join(_build_transaction_features(transactions, snapshot_date), on="msno", how="left")
        .join(_build_log_features(user_logs, snapshot_date), on="msno", how="left")
        .with_columns([pl.lit(snapshot_date).alias("snapshot_date")])
        .with_columns(
            [
                pl.col("city").fill_null("unknown"),
                pl.col("gender").fill_null("unknown"),
                pl.col("dominant_payment_method").fill_null("unknown"),
            ]
        )
    )

    numeric_fill_columns = [column for column in NUMERIC_FEATURE_COLUMNS if column in feature_frame.columns]
    feature_frame = feature_frame.with_columns([pl.col(column).fill_null(0.0) for column in numeric_fill_columns])
    return feature_frame, snapshot_date


def persist_feature_snapshot(feature_frame: pl.DataFrame) -> int:
    pandas_frame = feature_frame.to_pandas()
    dataframe_to_postgres(pandas_frame, FEATURE_TABLE_NAME, if_exists="replace")
    LOGGER.info("Persisted %s feature rows into %s", len(pandas_frame), FEATURE_TABLE_NAME)
    return len(pandas_frame)


def build_feature_snapshot() -> dict[str, object]:
    raw_tables = load_raw_tables()
    feature_frame, snapshot_date = build_feature_snapshot_from_frames(**raw_tables)
    row_count = persist_feature_snapshot(feature_frame)
    return {"rows": row_count, "snapshot_date": str(snapshot_date)}


if __name__ == "__main__":
    build_feature_snapshot()
