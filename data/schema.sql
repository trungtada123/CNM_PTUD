-- Sprint 2/3 - PostgreSQL schema for churn prediction MLOps flow
-- This file is executed by data/init-db.sh during first DB initialization.
-- Note:
--   Raw-table indexes (members/transactions/user_logs/train_label) are created
--   after bulk COPY in init-db.sh to keep initial loading fast.

-- 1) Raw source tables
CREATE TABLE IF NOT EXISTS members (
    msno VARCHAR(255) PRIMARY KEY,
    city INTEGER,
    bd INTEGER,
    gender VARCHAR(20),
    registered_via INTEGER,
    registration_init_time INTEGER
);

CREATE TABLE IF NOT EXISTS transactions (
    msno VARCHAR(255),
    payment_method_id INTEGER,
    payment_plan_days INTEGER,
    plan_list_price NUMERIC,
    actual_amount_paid NUMERIC,
    is_auto_renew INTEGER,
    transaction_date INTEGER,
    membership_expire_date INTEGER,
    is_cancel INTEGER
);

CREATE TABLE IF NOT EXISTS user_logs (
    msno VARCHAR(255),
    date INTEGER,
    num_25 INTEGER,
    num_50 INTEGER,
    num_75 INTEGER,
    num_985 INTEGER,
    num_100 INTEGER,
    num_unq INTEGER,
    total_secs NUMERIC
);

CREATE TABLE IF NOT EXISTS train_label (
    msno VARCHAR(255),
    is_churn INTEGER
);

-- 2) Offline feature store snapshots
CREATE TABLE IF NOT EXISTS feature_snapshots (
    snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    msno VARCHAR(255) NOT NULL,
    snapshot_date DATE NOT NULL,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_pipeline VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_feature_snapshots_msno_snapshot_date
    ON feature_snapshots (msno, snapshot_date);

-- 3) Online/offline prediction log
CREATE TABLE IF NOT EXISTS prediction_log (
    prediction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    msno VARCHAR(255) NOT NULL,
    snapshot_date DATE NOT NULL,
    churn_probability DOUBLE PRECISION NOT NULL,
    churn_label SMALLINT NOT NULL,
    model_version VARCHAR(128) NOT NULL,
    request_source VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prediction_log_msno_created_at
    ON prediction_log (msno, created_at DESC);

-- 4) Delayed ground-truth feedback
CREATE TABLE IF NOT EXISTS label_feedback (
    feedback_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    prediction_id BIGINT,
    msno VARCHAR(255) NOT NULL,
    snapshot_date DATE NOT NULL,
    actual_churn SMALLINT NOT NULL,
    feedback_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_label_feedback_prediction
        FOREIGN KEY (prediction_id) REFERENCES prediction_log (prediction_id)
        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_label_feedback_msno_feedback_date
    ON label_feedback (msno, feedback_date DESC);

-- 5) Monitoring metrics and alerts
CREATE TABLE IF NOT EXISTS monitoring_log (
    monitor_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    monitor_date DATE NOT NULL,
    metric_name VARCHAR(128) NOT NULL,
    metric_value DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    drift_detected BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_monitoring_log_monitor_date
    ON monitoring_log (monitor_date DESC);
