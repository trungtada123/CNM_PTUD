from __future__ import annotations

import os
import time
from functools import lru_cache

import mlflow
import pandas as pd

from kkbox_poc.config import get_settings
from kkbox_poc.constants import CATEGORICAL_FEATURE_COLUMNS, FEATURE_TABLE_NAME, MODEL_FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS
from kkbox_poc.storage import read_sql_query

HIGH_RISK_THRESHOLD = 0.70
MEDIUM_RISK_THRESHOLD = 0.40

_CACHE: dict[str, object] = {
    "expires_at": 0.0,
    "payload": None,
}


def _translate_risk_band(value: str) -> str:
    mapping = {
        "high": "Nguy cơ cao",
        "medium": "Cần theo dõi",
        "low": "Ổn định",
    }
    return mapping.get(str(value).lower(), "Chưa phân loại")


def _translate_gender(value: str) -> str:
    mapping = {
        "male": "Nam",
        "female": "Nữ",
        "unknown": "Chưa rõ",
        "": "Chưa rõ",
    }
    return mapping.get(str(value).strip().lower(), str(value))


def _configure_mlflow() -> str:
    settings = get_settings()
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = settings.minio_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = settings.minio_root_user
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.minio_root_password
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    return f"models:/{settings.mlflow_model_name}/Production"


@lru_cache(maxsize=1)
def load_production_model():
    model_uri = _configure_mlflow()
    model = mlflow.sklearn.load_model(model_uri)
    return model_uri, model


def _load_snapshot_frame() -> pd.DataFrame:
    frame = read_sql_query(f"SELECT * FROM {FEATURE_TABLE_NAME}")
    if frame.empty:
        raise ValueError(f"Table {FEATURE_TABLE_NAME} is empty. Run feature engineering first.")

    for column in NUMERIC_FEATURE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

    for column in CATEGORICAL_FEATURE_COLUMNS:
        frame[column] = frame[column].fillna("unknown").astype(str)

    return frame


def _with_risk_scores(frame: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    model_uri, model = load_production_model()
    scored = frame.copy()
    scored["churn_probability"] = model.predict_proba(scored[MODEL_FEATURE_COLUMNS])[:, 1]
    scored["risk_band"] = pd.cut(
        scored["churn_probability"],
        bins=[-0.01, MEDIUM_RISK_THRESHOLD, HIGH_RISK_THRESHOLD, 1.0],
        labels=["low", "medium", "high"],
    ).astype(str)
    scored["risk_band_label"] = scored["risk_band"].map(_translate_risk_band)
    return model_uri, scored.sort_values("churn_probability", ascending=False)


def _ratio(value: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(value * 100.0 / total, 2)


def _distribution(series: pd.Series, limit: int = 6, label_map: dict[str, str] | None = None) -> list[dict[str, object]]:
    counts = series.fillna("unknown").astype(str).replace({"": "unknown"}).value_counts().head(limit)
    total = int(counts.sum())
    return [
        {
            "label": label_map.get(label, label) if label_map else label,
            "count": int(count),
            "pct": _ratio(int(count), total),
        }
        for label, count in counts.items()
    ]


def _build_watchlist(scored: pd.DataFrame, limit: int = 15) -> list[dict[str, object]]:
    columns = [
        "msno",
        "risk_band",
        "risk_band_label",
        "churn_probability",
        "city",
        "gender",
        "listen_days_30d",
        "engagement_drop_ratio",
        "transaction_count",
        "service_months",
        "had_cancel",
    ]
    watchlist = scored.loc[:, columns].head(limit).copy()
    watchlist["churn_probability"] = watchlist["churn_probability"].round(4)
    watchlist["engagement_drop_ratio"] = watchlist["engagement_drop_ratio"].round(4)
    watchlist["service_months"] = watchlist["service_months"].round(2)
    watchlist["gender"] = watchlist["gender"].map(_translate_gender)
    watchlist["city"] = watchlist["city"].replace({"unknown": "Chưa rõ", "": "Chưa rõ"})
    return watchlist.to_dict(orient="records")


def _build_payload() -> dict[str, object]:
    settings = get_settings()
    snapshot = _load_snapshot_frame()
    model_uri, scored = _with_risk_scores(snapshot)

    total_users = len(scored)
    high_risk = int((scored["risk_band"] == "high").sum())
    medium_risk = int((scored["risk_band"] == "medium").sum())
    low_risk = int((scored["risk_band"] == "low").sum())
    inactive_30d = int((scored["listen_days_30d"] <= 0).sum())
    strong_drop = int((scored["engagement_drop_ratio"] >= 0.50).sum())
    canceled_before = int((scored["had_cancel"] > 0).sum())
    auto_renew_users = int((scored["latest_is_auto_renew"] > 0).sum())
    avg_churn_probability = round(float(scored["churn_probability"].mean()), 4)
    avg_age = round(float(scored.loc[scored["age"] > 0, "age"].mean()), 2) if (scored["age"] > 0).any() else 0.0
    snapshot_date = str(pd.to_datetime(scored["snapshot_date"]).max().date())
    high_risk_pct = _ratio(high_risk, total_users)
    inactive_pct = _ratio(inactive_30d, total_users)
    auto_renew_pct = _ratio(auto_renew_users, total_users)

    operational_notes = [
        {
            "title": "Nhóm cần ưu tiên giữ chân",
            "value": f"{high_risk_pct}%",
            "description": "Tệp khách hàng đang có xác suất rời bỏ từ 0.70 trở lên theo mô hình Production.",
        },
        {
            "title": "Tự động gia hạn",
            "value": f"{auto_renew_pct}%",
            "description": "Khách hàng vẫn đang bật gia hạn tự động, phù hợp cho các chương trình chăm sóc chủ động.",
        },
        {
            "title": "Chất lượng tín hiệu hành vi",
            "value": f"{inactive_pct}%",
            "description": "Tỷ lệ người dùng không ghi nhận hoạt động nghe nhạc trong cửa sổ 30 ngày gần nhất.",
        },
    ]

    return {
        "generated_at": pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "snapshot_date": snapshot_date,
        "model_uri": model_uri,
        "model_name": settings.mlflow_model_name,
        "total_users": total_users,
        "avg_churn_probability": avg_churn_probability,
        "avg_age": avg_age,
        "operational_notes": operational_notes,
        "summary_cards": [
            {
                "title": "Tổng người dùng",
                "value": f"{total_users:,}",
                "subtitle": "Số khách hàng có feature snapshot mới nhất.",
            },
            {
                "title": "Nguy cơ cao",
                "value": f"{high_risk:,}",
                "subtitle": f"{high_risk_pct}% có điểm churn từ {HIGH_RISK_THRESHOLD:.2f} trở lên.",
            },
            {
                "title": "Sắp rời bỏ",
                "value": f"{inactive_30d:,}",
                "subtitle": "Không phát sinh nghe nhạc trong 30 ngày gần nhất.",
            },
            {
                "title": "Điểm churn trung bình",
                "value": f"{avg_churn_probability:.2%}",
                "subtitle": f"Tuổi hợp lệ trung bình {avg_age} | Tự động gia hạn {auto_renew_users:,}.",
            },
        ],
        "risk_overview": [
            {"label": "Nguy cơ cao", "count": high_risk, "pct": _ratio(high_risk, total_users), "css_class": "high"},
            {"label": "Cần theo dõi", "count": medium_risk, "pct": _ratio(medium_risk, total_users), "css_class": "medium"},
            {"label": "Ổn định", "count": low_risk, "pct": _ratio(low_risk, total_users), "css_class": "low"},
        ],
        "behavior_flags": [
            {"label": "Không nghe nhạc trong 30 ngày", "count": inactive_30d, "pct": _ratio(inactive_30d, total_users)},
            {"label": "Giảm mạnh tổng thời gian nghe", "count": strong_drop, "pct": _ratio(strong_drop, total_users)},
            {"label": "Đã từng hủy gói", "count": canceled_before, "pct": _ratio(canceled_before, total_users)},
        ],
        "segments": {
            "gender": _distribution(scored["gender"], limit=4, label_map={"male": "Nam", "female": "Nữ", "unknown": "Chưa rõ"}),
            "city": _distribution(scored["city"], limit=8, label_map={"unknown": "Chưa rõ", "": "Chưa rõ"}),
            "payment_method": _distribution(scored["dominant_payment_method"], limit=8, label_map={"unknown": "Chưa rõ", "": "Chưa rõ"}),
        },
        "watchlist": _build_watchlist(scored),
    }


def get_dashboard_payload(force_refresh: bool = False, ttl_seconds: int = 120) -> dict[str, object]:
    now = time.time()
    cached_payload = _CACHE.get("payload")
    if not force_refresh and cached_payload is not None and now < float(_CACHE["expires_at"]):
        return cached_payload  # type: ignore[return-value]

    payload = _build_payload()
    _CACHE["payload"] = payload
    _CACHE["expires_at"] = now + ttl_seconds
    return payload
