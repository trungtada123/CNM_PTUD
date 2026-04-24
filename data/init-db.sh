#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
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

    -- 2. Đổ dữ liệu từ các file CSV
    COPY members FROM '/var/lib/postgresql/raw_data/members_v3.csv' WITH (FORMAT csv, HEADER true);
    COPY train_label FROM '/var/lib/postgresql/raw_data/train_v2.csv' WITH (FORMAT csv, HEADER true);
    COPY transactions FROM '/var/lib/postgresql/raw_data/transactions_v2.csv' WITH (FORMAT csv, HEADER true);
    COPY user_logs FROM '/var/lib/postgresql/raw_data/user_logs_v2.csv' WITH (FORMAT csv, HEADER true);


    -- 3. Tạo cơ sở dữ liệu cho Mage và MLflow
    SELECT 'CREATE DATABASE $MAGE_DB_NAME' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$MAGE_DB_NAME') \gexec
    SELECT 'CREATE DATABASE $MLFLOW_DB_NAME' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$MLFLOW_DB_NAME') \gexec
EOSQL