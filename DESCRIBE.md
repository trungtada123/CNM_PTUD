# cấu trúc hệ thống hiện tại

Hệ thống dự đoán khách hàng rời bỏ nền tảng
Data: https://www.kaggle.com/competitions/kkbox-churn-prediction-challenge/data(v2)


## 1) Cây thư mục tổng quan

```text
.
├── .dockerignore
├── .env
├── .env.example
├── .gitignore
├── data/
│   ├── init-db.sh
│   ├── members_v3.csv
│   ├── sample_submission_v2.csv
│   ├── train_v2.csv
│   ├── transactions_v2.csv
│   └── user_logs_v2.csv
├── DESCRIBE.md
├── docker-compose.yaml
├── KIEN_TRUC.MD
├── frontend/                 # React UI application
├── backend/                  # Flask Gateway/API
├── churn_serving/            # BentoML Service
│   ├── bentofile.yaml
│   └── service.py
├── mage/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── mage_data/
│   │   └── mage-src/
│   └── mage-src/
│       ├── .file_versions/
│       ├── .gitignore
│       ├── .ssh_tunnel/
│       │   └── aws_emr.json
│       ├── __init__.py
│       ├── charts/__init__.py
│       ├── custom/__init__.py
│       ├── data_loaders/
│       │   ├── __init__.py
│       │   └── load_raw_data.py
│       ├── transformers/
│       │   ├── __init__.py
│       │   └── transform_raw_data.py
│       ├── data_exporters/
│       │   ├── __init__.py
│       │   ├── export_feature_to_minio.py
│       │   └── export_feature_to_redis.py
│       ├── dbt/profiles.yml
│       ├── extensions/__init__.py
│       ├── interactions/__init__.py
│       ├── io_config.yaml
│       ├── metadata.yaml
│       ├── pipelines/
│       │   ├── __init__.py
│       │   ├── example_pipeline/
│       │   │   ├── __init__.py
│       │   │   └── metadata.yaml
│       │   └── extract_feature/
│       │       ├── __init__.py
│       │       └── metadata.yaml
│       ├── requirements.txt
│       ├── scratchpads/__init__.py
│       └── utils/__init__.py
├── README.md
├── requirements.txt
└── test_scripts/
  └── EDA.ipynb
```


## 4) Thư mục `mage/` (service build + Mage project)
### `mage/mage-src/`

#### Nhóm data loader / transformer / exporter (logic chính)

| File | Chức năng | Đầu vào | Đầu ra |
|---|---|---|---|
| `mage/mage-src/data_loaders/load_raw_data.py` | Đọc dữ liệu train từ Postgres bằng câu SQL join `train_label`, `members`, `transactions` | Postgres connection từ `io_config.yaml`; bảng nguồn trong DB | `pandas.DataFrame` gồm cột: `msno`, `is_churn`, `city`, `gender`, `registered_via`, `actual_amount_paid`, `is_auto_renew` |
| `mage/mage-src/transformers/transform_raw_data.py` | Làm sạch dữ liệu: fill missing, cast kiểu, drop duplicate theo `msno` | DataFrame từ `load_raw_data.py` | DataFrame đã tiền xử lý đặc trưng cơ bản |
| `mage/mage-src/data_exporters/export_feature_to_redis.py` | Ghi feature theo user vào Redis (`user_features:{msno}`), value là JSON | DataFrame từ transformer; env `REDIS_HOST`, `REDIS_PORT` | Key-value trong Redis phục vụ online serving |
| `mage/mage-src/data_exporters/export_feature_to_minio.py` | Ghi DataFrame ra Parquet trên MinIO (S3-compatible), tạo bucket nếu chưa có | DataFrame từ transformer; env `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_ENDPOINT_URL`, `MINIO_FEATURES_BUCKET_NAME` | Object `s3://<bucket>/churn_users_features.parquet` trên MinIO |
