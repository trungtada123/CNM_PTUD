# KKBox Churn PoC

PoC local-first cho bài toán churn prediction với:

- `PostgreSQL 15` làm offline store và raw store
- `Mage.ai` orchestration
- `Polars` feature engineering
- `MLflow + MinIO` experiment tracking, model registry, artifact store
- `Redis` online feature store
- `BentoML` model serving

## Cấu trúc

```text
kkbox_poc/          Logic dùng chung cho ingestion, features, training, redis
mage_project/       Mage blocks và pipeline metadata
bentoml_service/    BentoML service
scripts/            CLI để chạy từng bước ngoài Mage
```

## Chuẩn bị

1. Copy `.env.example` thành `.env`
2. Bảo đảm bộ dữ liệu KKBox đã được mount vào `../data`
3. Khởi động stack:

```bash
docker compose up --build -d postgres redis minio createbuckets mlflow mage
```

## Chạy pipeline bằng script

```bash
docker compose run --rm runner python scripts/ingest_kkbox_to_postgres.py
docker compose run --rm runner python scripts/build_features.py
docker compose run --rm runner python scripts/train_and_register.py
docker compose run --rm runner python scripts/materialize_online_store.py
docker compose up --build -d bentoml
```

## Chạy qua Mage

- Mage UI: `http://localhost:6789`
- Pipeline: `kkbox_churn_poc`

Block flow:

1. `load_kkbox_raw`
2. `build_kkbox_features`
3. `train_xgboost_model`
4. `push_features_to_redis`

## API serving

BentoML sẽ nạp model đang ở stage `Production` từ MLflow khi service khởi động.

Ví dụ request:

```bash
curl -X POST http://localhost:3000/predict ^
  -H "Content-Type: application/json" ^
  -d "{\"msno\": \"xxxxxxxxxxxxxxxx\"}"
```

## Ghi chú triển khai

- Các cột ngày dạng `YYYYMMDD` được parse sang `DATE` trước khi nạp vào PostgreSQL
- Feature snapshot được lưu ở bảng `kkbox_feature_snapshot`
- Redis lưu mỗi người dùng tại key `kkbox:features:<msno>` dưới dạng `HASH`
- MLflow artifact root trỏ về MinIO bucket `s3://mlflow`
