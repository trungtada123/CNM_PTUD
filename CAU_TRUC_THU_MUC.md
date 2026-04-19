# Mô tả cấu trúc thư mục hiện tại

Tài liệu này mô tả snapshot cấu trúc thư mục hiện tại của dự án tại thời điểm **2026-04-17**.

## Cây thư mục (đã lược bớt thư mục hệ thống/cache)

```text
.
├── .dockerignore
├── .env
├── .env.example
├── .gitignore
├── data
│   ├── members_v3.csv
│   ├── sample_submission_v2.csv
│   ├── train_v2.csv
│   ├── transactions_v2.csv
│   └── user_logs_v2.csv
├── docker-compose.yaml
├── init.sql
├── KIEN_TRUC.MD
├── mage
│   ├── .gitignore
│   ├── .ssh_tunnel
│   │   └── aws_emr.json
│   ├── __init__.py
│   ├── charts
│   │   └── __init__.py
│   ├── custom
│   │   └── __init__.py
│   ├── data_exporters
│   │   ├── __init__.py
│   │   └── export_titanic_clean.py
│   ├── data_loaders
│   │   ├── __init__.py
│   │   └── load_titanic.py
│   ├── dbt
│   │   └── profiles.yml
│   ├── extensions
│   │   └── __init__.py
│   ├── interactions
│   │   └── __init__.py
│   ├── io_config.yaml
│   ├── metadata.yaml
│   ├── pipelines
│   │   ├── __init__.py
│   │   └── example_pipeline
│   │       ├── __init__.py
│   │       └── metadata.yaml
│   ├── requirements.txt
│   ├── scratchpads
│   │   └── __init__.py
│   ├── transformers
│   │   ├── __init__.py
│   │   └── fill_in_missing_values.py
│   └── utils
│       └── __init__.py
├── mage_data
│   └── mage
│       └── .cache
│           ├── block_action_objects_mapping
│           ├── blocks_to_pipeline_mapping
│           ├── pipeline_details_mapping
│           └── tags_to_object_mapping
├── README.md
├── requirements.txt
└── test_scripts
    └── EDA.ipynb
```

## Giải thích nhanh theo nhóm

- `data/`: dữ liệu đầu vào thô (CSV) cho quá trình EDA/training/feature engineering.
- `mage/`: mã nguồn chính của Mage project (loaders, transformers, exporters, pipeline metadata, cấu hình kết nối).
- `mage_data/`: dữ liệu runtime/cache do Mage tạo trong quá trình chạy.
- `test_scripts/`: notebook phục vụ khám phá dữ liệu (`EDA.ipynb`).
- `docker-compose.yaml`: khởi tạo các service hạ tầng (Postgres, pgAdmin, Mage, Redis, MinIO).
- `init.sql`: script SQL khởi tạo ban đầu cho Postgres.
- `KIEN_TRUC.MD`: tài liệu kiến trúc tổng quan hệ thống.
- `requirements.txt` (root): dependency Python cấp workspace.

## Ghi chú

- Snapshot trên đã **lược bớt** các thư mục không cần tài liệu hóa chi tiết như `.git`, `venv`, `__pycache__`.
- Nếu bạn muốn, có thể tách thêm tài liệu theo dạng:
  - `docs/structure-overview.md` (tổng quan)
  - `docs/mage-pipeline-map.md` (mapping block/pipeline chi tiết).
