# Database Schema (Sprint 2)

Tai lieu nay mo ta schema PostgreSQL cho he thong du doan churn theo huong MLOps.

## Nhom bang

1. Raw data tables:
- `members`
- `transactions`
- `user_logs`
- `train_label`

2. Offline feature store:
- `feature_snapshots`

3. Prediction serving logs:
- `prediction_log`

4. Delayed labels:
- `label_feedback`

5. Monitoring:
- `monitoring_log`

## Chi tiet bang

### `members`
- `msno` (PK, VARCHAR(255))
- `city` (INTEGER)
- `bd` (INTEGER)
- `gender` (VARCHAR(20))
- `registered_via` (INTEGER)
- `registration_init_time` (INTEGER)

### `transactions`
- `msno` (VARCHAR(255))
- `payment_method_id` (INTEGER)
- `payment_plan_days` (INTEGER)
- `plan_list_price` (NUMERIC)
- `actual_amount_paid` (NUMERIC)
- `is_auto_renew` (INTEGER)
- `transaction_date` (INTEGER)
- `membership_expire_date` (INTEGER)
- `is_cancel` (INTEGER)

### `user_logs`
- `msno` (VARCHAR(255))
- `date` (INTEGER)
- `num_25` (INTEGER)
- `num_50` (INTEGER)
- `num_75` (INTEGER)
- `num_985` (INTEGER)
- `num_100` (INTEGER)
- `num_unq` (INTEGER)
- `total_secs` (NUMERIC)

### `train_label`
- `msno` (VARCHAR(255))
- `is_churn` (INTEGER)

### `feature_snapshots`
- `snapshot_id` (PK, BIGINT IDENTITY)
- `msno` (VARCHAR(255), NOT NULL)
- `snapshot_date` (DATE, NOT NULL)
- `features` (JSONB, NOT NULL, default `{}`)
- `source_pipeline` (VARCHAR(128))
- `created_at` (TIMESTAMPTZ, default `NOW()`)

Indexes:
- Unique index `uq_feature_snapshots_msno_snapshot_date` tren `(msno, snapshot_date)`.

### `prediction_log`
- `prediction_id` (PK, BIGINT IDENTITY)
- `msno` (VARCHAR(255), NOT NULL)
- `snapshot_date` (DATE, NOT NULL)
- `churn_probability` (DOUBLE PRECISION, NOT NULL)
- `churn_label` (SMALLINT, NOT NULL)
- `model_version` (VARCHAR(128), NOT NULL)
- `request_source` (VARCHAR(128))
- `created_at` (TIMESTAMPTZ, default `NOW()`)

Index:
- `idx_prediction_log_msno_created_at` tren `(msno, created_at DESC)`.

### `label_feedback`
- `feedback_id` (PK, BIGINT IDENTITY)
- `prediction_id` (BIGINT, FK -> `prediction_log.prediction_id`, ON DELETE SET NULL)
- `msno` (VARCHAR(255), NOT NULL)
- `snapshot_date` (DATE, NOT NULL)
- `actual_churn` (SMALLINT, NOT NULL)
- `feedback_date` (DATE, NOT NULL)
- `created_at` (TIMESTAMPTZ, default `NOW()`)

Index:
- `idx_label_feedback_msno_feedback_date` tren `(msno, feedback_date DESC)`.

### `monitoring_log`
- `monitor_id` (PK, BIGINT IDENTITY)
- `monitor_date` (DATE, NOT NULL)
- `metric_name` (VARCHAR(128), NOT NULL)
- `metric_value` (DOUBLE PRECISION)
- `threshold` (DOUBLE PRECISION)
- `drift_detected` (BOOLEAN, default `FALSE`)
- `notes` (TEXT)
- `created_at` (TIMESTAMPTZ, default `NOW()`)

Index:
- `idx_monitoring_log_monitor_date` tren `(monitor_date DESC)`.

## Init flow

- `docker compose up -d postgres` lan dau:
  - Postgres entrypoint se chay `data/init-db.sh`.
  - Script tao `mage_db`, `mlflow_db`.
  - Script apply schema tu `data/schema.sql`.
  - Neu `LOAD_RAW_DATA_ON_INIT=true`, script se nap CSV raw vao 4 bang raw.

## Lenh verify nhanh

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
```

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'feature_snapshots';
```

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'prediction_log'
ORDER BY ordinal_position;
```
