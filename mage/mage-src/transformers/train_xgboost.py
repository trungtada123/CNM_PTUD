import os
import mlflow
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, f1_score
from pandas import DataFrame
import socket
from urllib.parse import urlparse


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def train_xgboost_model(df: DataFrame, *args, **kwargs):
    print(f"-- Kích thước dữ liệu nhận được: {df.shape}")

    # 1. Tách Features (X) và Target (y)
    X = df.drop(columns=['msno', 'is_churn'])
    y = df['is_churn']
    
    # Cần convert các cột string/object sang category để XGBoost hiểu được (nếu có)
    for col in X.select_dtypes(include=['object']).columns:
        X[col] = X[col].astype('category')
        
    # Chia tập Train (80%) và Test (20%)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 2. Cấu hình kết nối tới MLflow container
    raw_url = os.getenv('MLFLOW_ENDPOINT_URL', 'http://mlflow:5000') 
    
    parsed_url = urlparse(raw_url)
    # print(parsed_url)
    ip_address = socket.gethostbyname(parsed_url.hostname)
    mlflow_url = f"{parsed_url.scheme}://{ip_address}:{parsed_url.port}"
    
    # print(f"-- kết nối tới MLflow tại IP: {mlflow_url}")
    mlflow.set_tracking_uri(mlflow_url)
    
    # KÍCH HOẠT KIỂM TRA KẾT NỐI:
    try:
        from mlflow.tracking import MlflowClient
        client = MlflowClient()
        # Thử lấy danh sách các experiments
        experiments = client.search_experiments()
        # print(f"✅ Kết nối MLflow THÀNH CÔNG! Đã tìm thấy {len(experiments)} experiments.")
        
        # Tạo hoặc chọn Experiment (Sổ ghi chép)
        mlflow.set_experiment("Churn_Prediction_KKBox")
        
        # 3. Khởi tạo Run và Huấn luyện
        with mlflow.start_run() as run:
            print(f"-- Đang huấn luyện với MLflow Run ID: {run.info.run_id}")
            
            # Thiết lập siêu tham số
            params = {
                'max_depth': 6,
                'learning_rate': 0.05,
                'n_estimators': 100,
                'enable_categorical': True, # Bật tính năng support dữ liệu category
                'eval_metric': 'auc',
                'random_state': 42
            }
            mlflow.log_params(params)
            
            # Khởi tạo và Train mô hình
            model = xgb.XGBClassifier(**params)
            model.fit(X_train, y_train)
            
            
            # 4. Dự đoán và Đánh giá trên tập Test
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            y_pred = model.predict(X_test)
            
            auc = roc_auc_score(y_test, y_pred_proba)
            f1 = f1_score(y_test, y_pred)
            
            print(f"-- Kết quả: AUC = {auc:.4f}, F1 = {f1:.4f}")
            
            # Ghi điểm số lên MLflow
            mlflow.xgboost.log_model(
                xgb_model=model,
                artifact_path="model_artifacts",
                registered_model_name="Churn_Prediction_KKBox"
            )
            mlflow.log_metric("val_auc", auc)
            mlflow.log_metric("val_f1", f1)
            
        return model

    except Exception as e:
        print(f"❌ LỖI KẾT NỐI MLFLOW: Không thể kết nối tới {mlflow_url}")
        print(f"Chi tiết lỗi: {e}")
        raise e 