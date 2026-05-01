# PHẦN 1 — REVIEW KIEN_TRUC CŨ

## Những điểm còn đúng (giữ nguyên)

- Tư tưởng chia 3 phân hệ: Data & Training / Serving / Monitoring — đúng và giữ.
- PostgreSQL làm offline store (raw tables + feature_snapshots) — đúng.
- Redis làm online feature store — đúng.
- Mage.ai làm orchestrator toàn bộ pipeline — đúng.
- BentoML làm model serving — đúng.
- NannyML với CBPE + drift detection — đúng.
- Prometheus + Grafana cho metrics và alerting — đúng.
- Docker Compose cho deploy local — đúng.
- GitHub Actions CI/CD — đúng.
- Schema PostgreSQL (customers, transactions, user_events, feature_snapshots, prediction_log, label_feedback, monitoring_log) — đúng.
- Định nghĩa feature engineering window 7d/30d/90d — đúng.
- Logic delayed labels 30 ngày — đúng.
- Các pipeline Mage.ai (ingest → feature → materialize → train → evaluate → register → batch_score → monitor → backfill → retrain) — đúng về tên và thứ tự.

## Những điểm bị lệch stack mới (phải sửa)

**Sai 1 — W&B → MLflow + MinIO:**
File cũ đề cập W&B ở khắp nơi: tracking, artifact, registry, promote model. Phải thay toàn bộ bằng MLflow (tracking + registry) + MinIO (lưu file artifact vật lý). Đây là thay đổi lớn nhất, ảnh hưởng đến pipeline train_model, register_model, và cách BentoML load model.

**Sai 2 — Streamlit → Flask + React:**
File cũ đề cập Streamlit làm UI. Phải xóa hoàn toàn. Thay bằng: React (frontend) + Flask (backend application / API bridge). Hai lớp này có vai trò tách biệt rõ ràng, không phải một.

**Sai 3 — Thiếu SDV hoàn toàn:**
File cũ có đề cập simulation dữ liệu nhưng dùng script Python thủ công. Phải thay bằng SDV (Synthetic Data Vault) — đây là công cụ chính thức được chốt để học phân phối từ KKBox rồi sinh data tháng 2, tháng 3.

**Sai 4 — Thiếu Polars:**
File cũ không nhắc đến Polars. Toàn bộ phần feature engineering vẫn ngầm dùng pandas. Phải cập nhật rõ: dùng Polars cho tất cả join/aggregate/transform trong pipeline build_feature_snapshot.

**Sai 5 — Cách BentoML load model:**
File cũ mô tả BentoML load từ W&B Registry. Phải sửa thành: BentoML hỏi MLflow Registry tìm model Production, sau đó tải file `.pkl` từ MinIO.

**Sai 6 — Luồng user interaction thiếu Flask:**
File cũ mô tả User → Streamlit → BentoML (trực tiếp). Luồng đúng phải là: User → React → Flask → BentoML → Redis/PostgreSQL.

**Sai 7 — Phần phân công nhóm còn dấu vết W&B và Streamlit:**
Thành viên 2 và 4 vẫn nhắc đến W&B workspace và Streamlit app — phải sửa.

**Sai 8 — Tài liệu tham khảo:**
Còn W&B docs, thiếu MLflow, MinIO, SDV, Polars, Flask docs.

## Mức độ chỉnh sửa cần thiết

| Mục | Mức độ |
|---|---|
| Sơ đồ kiến trúc tổng thể | Sửa lớn |
| Bảng ánh xạ công nghệ | Sửa lớn |
| Luồng end-to-end | Sửa trung bình (thêm SDV, Polars, Flask) |
| Phần giao diện ứng dụng | Viết lại hoàn toàn (Flask + React) |
| Vai trò từng công cụ | Sửa lớn |
| Phần dữ liệu (simulation) | Bổ sung SDV flow |
| Phân công nhóm | Sửa nhẹ |
| Tài liệu tham khảo | Cập nhật |

---

# PHẦN 2 — KIEN_TRUC ĐÃ TINH CHỈNH

## A. Kiến trúc tổng thể (đã cập nhật stack)

```
┌──────────────────────────────────────────────────────────────────────┐
│  PHÂN HỆ 1: DATA & TRAINING                                          │
│                                                                      │
│  KKBox Raw CSV (Tháng 1 — seed dataset)                              │
│       │                                                              │
│       ▼                                                              │
│  [Mage.ai: ingest_raw_data]                                          │
│       │  ── Insert vào PostgreSQL (customers / transactions /        │
│       │                            user_events)                      │
│       │                                                              │
│       ▼                                                              │
│  [Mage.ai: generate_synthetic_data] ← SDV học phân phối từ Tháng 1   │
│       │  ── Sinh data Tháng 2 (bình thường)                          │
│       │  ── Sinh data Tháng 3 (có drift cố ý)                        │
│       │  ── Insert vào PostgreSQL (cùng các bảng raw)                │
│       │                                                              │
│       ▼                                                              │
│  [Mage.ai: build_feature_snapshot]  ← Dùng Polars để transform       │
│       │  ── JOIN customers + transactions + user_events              │
│       │  ── Tính window 7d / 30d / 90d                               │
│       │  ── Ghi vào PostgreSQL (feature_snapshots)                   │
│       │     = Offline Feature Store                                  │
│       │                                                              │
│       ▼                                                              │
│  [Mage.ai: materialize_online_features]                              │
│       │  ── Đẩy feature mới nhất từ Postgres → Redis                 │
│       │     = Online Feature Store (key: customer:{id}:features)     │
│       │                                                              │
│       ▼                                                              │
│  [Mage.ai: train_model]                                              │
│       │  ── Kéo data từ feature_snapshots (Postgres)                 │
│       │  ── Train Logistic Regression (baseline) + XGBoost           │
│       │  ── Log params + metrics → MLflow Tracking Server            │
│       │  ── Lưu xgboost.pkl + scaler.pkl → MinIO (Object Storage)    │
│       │                                                              │
│       ▼                                                              │
│  [Mage.ai: evaluate_model + register_model]                          │
│       │  ── Tính AUC, F1, PR-AUC, Confusion Matrix                   │
│       │  ── Log metrics + plots → MLflow                             │
│       │  ── Nếu đủ ngưỡng: đánh dấu "Production" trong MLflow        │
│       │     Registry                                                 │
│       │                                                              │
│  MLflow Tracking Server   ←──────────────────────────────────────┐   │
│  └── Experiments, Runs, Metrics, Params                          │   │
│  MLflow Registry                                                 │   │
│  └── Model versions, aliases (staging / production)              │   │
│  MinIO Object Storage                                            │   │
│  └── xgboost.pkl, scaler.pkl, feature_names.json                 │   │
└──────────────────────────────────────────────────────────────────┘   │
                              │                                        │
                              ▼                                        │
┌──────────────────────────────────────────────────────────────────┐   │
│  PHÂN HỆ 2: APPLICATION & SERVING                                │   │
│                                                                  │   │
│  BentoML Service (khởi động)                                     │   │
│  ├── Hỏi MLflow Registry: model nào đang "Production"?           │   │
│  ├── Tải artifact tương ứng từ MinIO                             │   │
│  └── Dựng REST API:                                              │   │
│      ├── POST /predict          (feature dict → churn prob)      │   │
│      ├── POST /predict_batch    (list features → bulk results)   │   │
│      ├── GET  /predict_by_id    (customer_id → Redis lookup)     │   │
│      ├── GET  /health                                            │   │
│      └── GET  /metrics          (Prometheus scrape endpoint)     │   │
│                                                                  │   │
│  Flask Backend (Application Layer)                               │   │
│  ├── Nhận request từ React                                       │   │
│  ├── Auth / validate / business logic                            │   │
│  ├── Gọi BentoML API (internal network)                          │   │
│  ├── Gọi PostgreSQL / Redis trực tiếp (cho dashboard data)       │   │
│  └── Trả response về React                                       │   │
│                                                                  │   │
│  React Frontend (Web Application)                                │   │
│  ├── Dashboard tổng quan (KPI cards, recent predictions)         │   │
│  ├── Single Prediction (form nhập tay)                           │   │
│  ├── Batch Prediction (upload CSV)                               │   │
│  ├── Customer Lookup (tra cứu theo ID)                           │   │
│  ├── Monitoring Dashboard (AUC, drift charts)                    │   │
│  └── System / Model Info (registry, service status)              │   │
│                                                                  │   │
│  Luồng chính: User → React → Flask → BentoML → Redis/Postgres    │   │
│  Side effect:  BentoML → prediction_log (Postgres)               │   │
└──────────────────────────────────────────────────────────────────┘   │
                              │                                        │
                              ▼                                        │
┌──────────────────────────────────────────────────────────────────┐   │
│  PHÂN HỆ 3: MONITORING & RETRAINING                              │   │
│                                                                  │   │
│  [Mage.ai: backfill_labels] (chạy ngày 1 hàng tháng)             │   │
│  └── Gán label thật vào feature_snapshots / label_feedback       │   │
│                                                                  │   │
│  [Mage.ai: monitor_model] (chạy cuối tháng)                      │   │
│  ├── Đọc prediction_log + feature_snapshots từ Postgres          │   │
│  ├── Gọi NannyML:                                                │   │
│  │   ├── CBPE → estimated AUC/F1 (không cần label)               │   │
│  │   ├── Univariate Drift Detection (so sánh tháng vs ref)       │   │
│  │   └── Realized Performance (khi có label backfill)            │   │
│  └── Ghi kết quả → monitoring_log (Postgres)                     │   │
│      Đẩy metrics → Prometheus (custom exporter)                  │   │
│                                                                  │   │
│  Prometheus                                                      │   │
│  ├── Scrape BentoML /metrics (latency, request count)            │   │
│  ├── Scrape Flask /metrics (API stats)                           │   │
│  └── Scrape monitoring exporter (estimated AUC, drift score)     │   │
│                                                                  │   │
│  Grafana                                                         │   │
│  ├── Dashboard: API performance, model performance, drift        │   │
│  └── Alert Rule: estimated_AUC < 0.72 hoặc drift_score > 0.5     │   │
│      → Webhook → Mage.ai trigger retrain                         │   │
│                                                                  │   │
│  [Mage.ai: retrain_model_if_needed]                              │   │
│  └── Nếu flag triggered → quay lại train_model pipeline  ──────► ┘   │
└──────────────────────────────────────────────────────────────────┘

Infrastructure: Docker Compose (13 containers, 1 network)
CI/CD: GitHub Actions (test → lint → build → deploy)
```

---

## B. Ánh xạ công nghệ vào từng phân hệ (đã cập nhật)

| Phân hệ | Công nghệ | Vai trò cụ thể |
|---|---|---|
| **Data & Training** | PostgreSQL | Lưu raw tables + offline feature store (feature_snapshots) + logs |
| **Data & Training** | Polars | Xử lý / join / aggregate dữ liệu trong pipeline feature engineering |
| **Data & Training** | SDV | Học phân phối từ KKBox tháng 1, sinh synthetic data tháng 2 và tháng 3 |
| **Data & Training** | Mage.ai | Orchestrate toàn bộ 10 pipelines (schedule + dependency) |
| **Data & Training** | MLflow | Experiment tracking + Model Registry (quản lý version, alias production) |
| **Data & Training** | MinIO | Object storage lưu file model artifact (pkl, json) |
| **Data & Training** | Redis | Online feature store — cache feature phục vụ inference tốc độ cao |
| **Application & Serving** | React | Frontend web app đa trang, gọi Flask API |
| **Application & Serving** | Flask | Backend trung gian: auth, validate, routing đến BentoML, truy vấn DB |
| **Application & Serving** | BentoML | Model serving: load từ MLflow+MinIO, expose REST endpoints, log prediction |
| **Monitoring & Retraining** | NannyML | CBPE ước tính AUC không cần label + univariate drift detection |
| **Monitoring & Retraining** | Prometheus | Scrape metrics từ BentoML, Flask, monitoring exporter |
| **Monitoring & Retraining** | Grafana | Visualize metrics, cấu hình alert rule, gửi webhook trigger retrain |
| **Monitoring & Retraining** | Mage.ai | Trigger monitor_model và retrain_model_if_needed theo lịch / webhook |
| **Infrastructure** | Docker Compose | Khởi chạy 13 container, quản lý network và volume |
| **CI/CD** | GitHub Actions | Tự động test, lint, build image, deploy via Docker Compose |

---

## C. Vai trò từng công cụ (sắc bén, không chung chung)

**PostgreSQL** — Trái tim lưu trữ của toàn hệ thống. Chứa 7 bảng với 3 nhóm vai trò riêng: raw tables (customers, transactions, user_events), feature store offline (feature_snapshots), và operational logs (prediction_log, label_feedback, monitoring_log).

**Polars** — Thay thế pandas trong tất cả các bước transform dữ liệu. Lý do chọn: xử lý hàng triệu dòng KKBox nhanh gấp 5–10 lần pandas nhờ engine Rust, tiết kiệm bộ nhớ qua lazy evaluation và downcasting tự động.

**SDV (Synthetic Data Vault)** — Công cụ sinh dữ liệu giả lập có kiểm soát. Học phân phối thống kê từ KKBox tháng 1, sau đó sinh ra tháng 2 (bình thường) và tháng 3 (với drift cố ý ở các feature hành vi: giảm `total_secs`, tăng `last_listen_days_ago`). Đảm bảo data contract tương thích với schema thật.

**Mage.ai** — Orchestrator trung tâm. Quản lý 10 pipeline với lịch chạy riêng, dependency rõ ràng, và khả năng nhận trigger từ bên ngoài (Grafana webhook). UI pipeline view có thể dùng làm minh chứng trong báo cáo.

**MLflow** — Hai vai trò: (1) Tracking Server ghi lại mọi experiment run (params, metrics, artifacts path); (2) Registry quản lý version model với alias `staging` và `production`. Đây là nguồn sự thật duy nhất về model lifecycle.

**MinIO** — Object Storage tự host, tương thích S3 API. Lưu file artifact vật lý (xgboost.pkl, scaler.pkl, feature_names.json). MLflow trỏ đến MinIO, BentoML tải từ MinIO — hai dịch vụ không bao giờ trao đổi file trực tiếp.

**Redis** — Cache tầng serving. Sau khi Mage.ai materialize features hàng đêm, Redis chứa snapshot feature mới nhất của mỗi customer dưới dạng hash. BentoML đọc từ đây với latency < 1ms. Fallback sang PostgreSQL nếu miss.

**BentoML** — Model serving layer. Khi khởi động: query MLflow Registry → tải pkl từ MinIO → wrap thành REST API. Không chứa business logic, chỉ làm inference. Expose `/metrics` cho Prometheus.

**Flask** — Application backend. Đứng giữa React và BentoML. Xử lý authentication, validation, rate limiting, business rules (ví dụ: chỉ cho phép lookup customer_id đang active). Cũng truy vấn PostgreSQL trực tiếp để phục vụ các màn hình dashboard không cần predict.

**React** — Frontend web application nhiều trang với routing, state management, chart visualization (dùng Recharts hoặc Chart.js). Gọi Flask API qua REST. Không bao giờ gọi BentoML trực tiếp.

**NannyML** — Engine giám sát model. Giải quyết vấn đề delayed labels: ước tính AUC và F1 từ phân phối confidence score (CBPE) mà không cần chờ label thật 30 ngày. Đồng thời phát hiện drift từng feature bằng statistical tests.

**Prometheus** — Thu thập time-series metrics từ BentoML, Flask, và custom monitoring exporter (đẩy estimated_AUC, drift_score từ NannyML output).

**Grafana** — Hiển thị dashboard và cấu hình alert. Khi estimated_AUC < 0.72 hoặc drift_score > 0.5, Grafana gửi webhook POST đến Mage.ai API để trigger pipeline retrain.

**Docker Compose** — Infrastructure-as-code cho 13 container. Tất cả services trên cùng một Docker network, giao tiếp qua service name (ví dụ: `http://bentoml:3000`).

---

## D. Luồng hoạt động end-to-end (9 bước, đúng stack)

**Bước 0 — Infrastructure Setup:**
Chạy `docker-compose up`. Docker khởi động 13 container: `postgres`, `redis`, `minio`, `mlflow`, `mage`, `bentoml`, `flask`, `react`, `prometheus`, `grafana`, và các container phụ trợ. Tất cả join vào network `churn-net`.

**Bước 1 — Ingestion & Synthetic Generation:**
Mage.ai pipeline `ingest_raw_data` đọc KKBox CSV → insert vào PostgreSQL (customers, transactions, user_events). Tiếp theo pipeline `generate_synthetic_data` dùng SDV: fit multivariate distributions từ tháng 1 → generate tháng 2 (phân phối bình thường) và tháng 3 (inject drift: `total_secs_7d` giảm 45%, `last_listen_days_ago` tăng 2.5x, `num_cancel_30d` tăng gấp đôi). Dữ liệu synthetic cũng insert vào cùng bảng raw với cột `data_source` = 'synthetic'.

**Bước 2 — Feature Engineering (Polars):**
Hàng ngày lúc 1:00 SA, Mage.ai pipeline `build_feature_snapshot` chạy. Dùng Polars scan PostgreSQL (lazy mode), join 3 bảng raw, tính window aggregations 7d/30d/90d, compute derived features (engagement_drop_ratio, completion_rate, spend_trend), downcasting dtypes để tiết kiệm storage. Ghi snapshot vào bảng `feature_snapshots` (Offline Feature Store).

**Bước 3 — Materialization to Redis:**
Lúc 2:00 SA, Mage.ai pipeline `materialize_online_features` lấy rows mới nhất từ `feature_snapshots` → đẩy lên Redis dưới dạng hash `customer:{id}:features` với TTL 48 giờ. Model serving từ lúc này có thể phục vụ prediction latency < 10ms.

**Bước 4 — Training, Tracking, Registry:**
Pipeline `train_model` kéo training data từ `feature_snapshots` (chỉ rows có `is_churn IS NOT NULL`). Train XGBoost với early stopping. Log toàn bộ params + metrics vào MLflow Tracking Server. Upload `xgboost.pkl` và `scaler.pkl` lên MinIO bucket `churn-models`. Pipeline `register_model` so sánh candidate AUC với champion, nếu tốt hơn 0.5%: register version mới lên MLflow Registry với alias `production`.

**Bước 5 — Model Serving:**
BentoML service khởi động: query MLflow Registry tìm model alias `production` → lấy artifact path → tải `xgboost.pkl` + `scaler.pkl` từ MinIO → wrap thành REST API tại port 3000. Expose `/metrics` endpoint cho Prometheus.

**Bước 6 — User Interaction:**
User mở React app → nhập customer_id → React POST `/api/predict_by_id` đến Flask → Flask validate → Flask POST `/predict_by_id` đến BentoML → BentoML HGETALL `customer:{id}:features` từ Redis (< 1ms) → XGBoost predict → trả score về Flask → Flask format response → React hiển thị risk score + risk level + explanation. BentoML đồng thời INSERT vào `prediction_log` (async, không chặn response).

**Bước 7 — Monitoring:**
Cuối mỗi tháng, pipeline `monitor_model` đọc `prediction_log` (chunk tháng hiện tại) và `feature_snapshots` (reference = tháng train). Gọi NannyML CBPE → estimated AUC/F1 không cần label. Gọi NannyML UnivariateDriftCalculator → drift score từng feature. Ghi vào `monitoring_log`. Monitoring exporter đẩy metrics lên Prometheus endpoint.

**Bước 8 — Dashboarding & Alert:**
Prometheus scrape mỗi 15 giây. Grafana đọc từ Prometheus, hiển thị dashboard: API latency (BentoML + Flask), request count, estimated AUC theo chunk, drift score heat map. Alert Rule: `estimated_auc < 0.72` hoặc `drift_score_overall > 0.50` → status FIRING → Grafana gửi HTTP POST webhook đến Mage.ai API endpoint `/trigger/retrain_model_if_needed`.

**Bước 9 — Automated Retraining:**
Mage.ai nhận webhook → kích hoạt pipeline `retrain_model_if_needed` → trigger lại `train_model` với training window mở rộng (bao gồm data mới nhất đã có label). Model mới được log vào MLflow, artifact lưu MinIO, Registry cập nhật alias `production`. BentoML tự reload model mới (hoặc restart container qua Docker API). Chu kỳ khép kín.

---

## E. Giao diện ứng dụng React — 6 màn hình chính

### Màn hình 1 — Dashboard Tổng quan (`/`)
- KPI cards: tổng khách hàng, % churn dự đoán tuần này, số khách risk cao (score > 0.7), model version đang chạy
- Estimated AUC hiện tại (badge màu: xanh ≥ 0.80, vàng 0.72–0.79, đỏ < 0.72)
- Drift status banner (LOW / MEDIUM / HIGH)
- Recent predictions table (10 prediction mới nhất)
- Quick stats: request count 24h, avg latency

### Màn hình 2 — Single Prediction (`/predict`)
- Form nhập tay: 12 fields (age, gender, tenure_days, listen_days_7d, listen_days_30d, total_secs_7d, completion_rate_7d, last_listen_days_ago, is_auto_renew, payment_plan_days, days_to_expire, num_cancel_30d)
- Nút "Predict"
- Kết quả: Gauge chart xác suất (0–100%), risk badge (LOW/MEDIUM/HIGH), explanation card (top 3 contributing factors dạng text)

### Màn hình 3 — Batch Prediction (`/batch`)
- Upload zone cho CSV file
- Preview bảng 5 dòng đầu sau khi upload
- Nút "Run Batch Prediction"
- Progress bar khi đang xử lý
- Bảng kết quả đầy đủ với filter/sort
- Summary stats: total rows, % predicted churn, % HIGH risk
- Download button (output CSV)

### Màn hình 4 — Customer Lookup (`/customer`)
- Input: msno / customer_id (text box + search button)
- Card thông tin khách hàng: feature values hiện tại, data source badge (Redis / PostgreSQL / Not Found)
- Prediction result: score, label, model version, prediction timestamp
- Lịch sử dự đoán 3 tháng gần nhất (mini chart)
- Data quality indicator: số features có giá trị hợp lệ

### Màn hình 5 — Monitoring Dashboard (`/monitoring`)
- Line chart: Estimated AUC theo tháng (từ NannyML CBPE), Realized AUC khi có label (cùng trục)
- Bar chart: Drift score từng feature (sorted descending), highlight màu đỏ khi drifted
- Drift status timeline: mỗi chunk tháng là 1 row (status LOW/MED/HIGH)
- Alert log: timestamp + loại alert + action đã trigger
- Retrain history: ngày retrain, model version cũ → mới, AUC change

### Màn hình 6 — System Info (`/system`)
- Service status table: tên service, status (UP/DOWN), last check
- Current model card: version, train date, test AUC, alias, artifact path (MinIO)
- Training history: bảng các lần train (date, AUC, F1, status: Production/Archived)
- MLflow Registry summary (call Flask → call MLflow API)
- Docker container stats (nếu expose)

---

## F. Các Mage.ai Pipelines (đã cập nhật)

| Pipeline | Trigger | Input | Output | Tool chính |
|---|---|---|---|---|
| `ingest_raw_data` | Manual / once | KKBox CSV | PostgreSQL raw tables | Polars |
| `generate_synthetic_data` | Sau ingest | PostgreSQL T1 | PostgreSQL T2, T3 (synthetic) | SDV |
| `build_feature_snapshot` | Daily 1:00 SA | PostgreSQL raw | PostgreSQL feature_snapshots | Polars |
| `materialize_online_features` | Daily 2:00 SA | PostgreSQL snapshots | Redis | Redis-py |
| `train_model` | Manual / webhook | PostgreSQL snapshots | MLflow run + MinIO artifacts | XGBoost, MLflow |
| `evaluate_model` | Sau train | MLflow run_id | MLflow metrics + plots | scikit-learn, MLflow |
| `register_model` | Sau evaluate | MLflow metrics | MLflow Registry alias | MLflow |
| `batch_score` | Daily 3:00 SA | feature_snapshots + MLflow/MinIO | prediction_log | BentoML client |
| `backfill_labels` | Monthly ngày 1 | prediction_log (lag 30d) | label_feedback, update feature_snapshots | Polars |
| `monitor_model` | Monthly cuối tháng | prediction_log + snapshots | monitoring_log + Prometheus | NannyML |
| `retrain_model_if_needed` | Grafana webhook | monitoring_log flag | Trigger train_model | Mage.ai internal |

---

## G. Data Layer (đầy đủ 5 tầng)

```
TẦNG 1 — RAW DATA (PostgreSQL)
├── customers          : hồ sơ khách hàng (KKBox members)
├── transactions       : lịch sử giao dịch / subscription
└── user_events        : nhật ký nghe nhạc theo ngày
    Cột data_source: 'real' | 'synthetic_t2' | 'synthetic_t3'

TẦNG 2 — OFFLINE FEATURE STORE (PostgreSQL)
└── feature_snapshots  : snapshot feature window 7d/30d/90d hàng ngày
    is_churn: NULL (chưa có label) → được backfill sau 30 ngày

TẦNG 3 — ONLINE FEATURE STORE (Redis)
└── customer:{id}:features  : hash, TTL 48h
    customer:{id}:last_prediction : hash, TTL 24h

TẦNG 4 — PREDICTION & LABEL LOGS (PostgreSQL)
├── prediction_log     : mỗi prediction của model (score, label, version)
└── label_feedback     : label thật sau 30 ngày (delayed label backfill)

TẦNG 5 — MONITORING LOGS (PostgreSQL)
└── monitoring_log     : kết quả NannyML theo chunk tháng
    (estimated_auc, realized_auc, drift_score, drift_status, retrain_triggered)

MODEL ARTIFACTS (MinIO)
└── bucket: churn-models/
    ├── {run_id}/xgboost.pkl
    ├── {run_id}/scaler.pkl
    └── {run_id}/feature_names.json
```

---

# PHẦN 3 — BẢN CHỐT CUỐI CÙNG

## Stack chính thức (13 công cụ)

```
Infrastructure : Docker Compose
Orchestration  : Mage.ai
Data Storage   : PostgreSQL + Redis + MinIO
Data Processing: Polars + SDV
ML Training    : XGBoost (+ Logistic Regression baseline)
ML Tracking    : MLflow (Tracking Server + Registry)
ML Serving     : BentoML
Application    : Flask (backend) + React (frontend)
Monitoring     : NannyML + Prometheus + Grafana
CI/CD          : GitHub Actions
```

## Luồng end-to-end chốt (1 hình)

```
KKBox CSV
    │
    ▼
[Mage: ingest] → PostgreSQL (raw)
    │
    ▼
[Mage: SDV generate] → PostgreSQL (T2, T3 synthetic)
    │
    ▼
[Mage: Polars feature] → PostgreSQL (feature_snapshots) ←── Offline Store
    │
    ▼
[Mage: materialize] → Redis ←────────────────────────────── Online Store
    │
    ▼
[Mage: train] → MLflow (track) → MinIO (artifact)
    │
    ▼
[Mage: register] → MLflow Registry (alias: production)
    │
    ▼
BentoML ←─ load model từ MLflow+MinIO
    │
    │◄─── User → React → Flask → BentoML → Redis
    │                              └──────────────► prediction_log (Postgres)
    ▼
[Mage: backfill] → label_feedback (Postgres)
    │
    ▼
[Mage: NannyML monitor] → monitoring_log (Postgres) → Prometheus
    │
    ▼
Grafana → Alert → Webhook
    │
    ▼
[Mage: retrain] ──────────────────────────────────► [quay lại train]
```

## Các màn hình web chốt (6 màn hình)

| # | Route | Tên | Chức năng chính |
|---|---|---|---|
| 1 | `/` | Dashboard | KPI, model status, drift status, recent predictions |
| 2 | `/predict` | Single Prediction | Form nhập tay → score + risk + explanation |
| 3 | `/batch` | Batch Prediction | Upload CSV → bảng kết quả → download |
| 4 | `/customer` | Customer Lookup | Tra theo ID → features từ Redis + prediction |
| 5 | `/monitoring` | Monitoring | AUC chart, drift chart, alert log, retrain history |
| 6 | `/system` | System Info | Service status, model registry, training history |

## Core / Optional / Future Work

**Core (phải có để demo và báo cáo):**
- Toàn bộ 9 bước pipeline hoạt động được
- 4 màn hình React: Dashboard, Single, Batch, Customer Lookup
- MLflow + MinIO tracking và serving
- NannyML phát hiện drift tháng 3
- Docker Compose khởi chạy toàn bộ stack

**Optional (làm thêm nếu còn thời gian):**
- Màn hình Monitoring Dashboard với chart NannyML
- Màn hình System Info với MLflow Registry summary
- Grafana webhook trigger retrain tự động (thay vì trigger thủ công)
- SHAP values trong prediction response

**Future Work (đề cập trong báo cáo nhưng không implement):**
- Kafka thay batch ingestion hàng ngày
- Kubernetes scale BentoML
- Cloud deployment (AWS / GCP)
- A/B testing framework cho model rollout

## Cập nhật tài liệu tham khảo

Thay thế:
- MLflow docs: https://mlflow.org/docs/latest/
- Flask docs: https://flask.palletsprojects.com/ + React docs: https://react.dev/

Bổ sung mới:
- SDV: https://docs.sdv.dev/sdv/
- Polars: https://docs.pola.rs/
- MinIO: https://min.io/docs/minio/linux/developers/python/minio-py.html

## Cập nhật phân công nhóm

| Thành viên | Vai trò | Deliverables |
|---|---|---|
| 1 — Data Engineer | PostgreSQL schema, Mage.ai pipelines (ingest, SDV, feature/Polars, materialize, backfill) | SQL schema, Mage pipelines, SDV scripts |
| 2 — ML Engineer | Train XGBoost + baseline, MLflow tracking, MinIO artifact, register/evaluate pipelines | Training code, MLflow workspace, model artifacts |
| 3 — Backend/Serving | BentoML service (3 endpoints), Flask API (6 routes), Docker Compose, GitHub Actions | BentoML service, Flask app, docker-compose.yml |
| 4 — Frontend/Monitoring | React app (6 màn hình), NannyML monitor pipeline, Grafana dashboard, báo cáo + slide | React app, NannyML code, Grafana dashboards, báo cáo |