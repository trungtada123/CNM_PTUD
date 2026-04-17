RAW_TABLE_NAMES = {
    "train": "kkbox_train",
    "members": "kkbox_members",
    "transactions": "kkbox_transactions",
    "user_logs": "kkbox_user_logs",
}

FEATURE_TABLE_NAME = "kkbox_feature_snapshot"

NUMERIC_FEATURE_COLUMNS = [
    "age",
    "listen_days_7d",
    "listen_days_14d",
    "listen_days_30d",
    "num_25_30d",
    "num_50_30d",
    "num_100_30d",
    "total_secs_30d",
    "completion_ratio_30d",
    "engagement_drop_ratio",
    "days_since_last_listen",
    "transaction_count",
    "service_months",
    "latest_is_auto_renew",
    "had_cancel",
]

CATEGORICAL_FEATURE_COLUMNS = [
    "city",
    "gender",
    "dominant_payment_method",
]

MODEL_FEATURE_COLUMNS = NUMERIC_FEATURE_COLUMNS + CATEGORICAL_FEATURE_COLUMNS
