import pandas as pd
import os
import bentoml
from bentoml.models import BentoModel

with bentoml.importing():
    import mlflow
    import xgboost as xgb


# Sửa lỗi 1: Khai báo chuẩn BentoModel
# bento_model = BentoModel("churn_xgboost_model:latest")

@bentoml.service(name="churn_prediction_service")
class ChurnService:
    # Khai báo model metadata
    model_ref = BentoModel("churn_xgboost_model:latest")

    def __init__(self):
        # mlflow.set_tracking_uri("http://localhost:5000")
        mlflow.set_tracking_uri(os.getenv("MLFLOW_ENDPOINT_URL"))
        
        # Load mô hình thực tế vào RAM
        self.model = bentoml.mlflow.load_model(self.model_ref)

    @bentoml.api
    def predict(self, input_df: pd.DataFrame) -> dict:
        # 1. ÉP KIỂU LẠI: Chuyển tất cả các cột dạng object/string sang category
        for col in input_df.select_dtypes(include=['object']).columns:
            input_df[col] = input_df[col].astype('category')
        
        # 2. ĐẢM BẢO CỘT SỐ: Ép các cột số về float để đồng nhất
        numeric_cols = ['city', 'registered_via', 'actual_amount_paid', 'is_auto_renew']
        for col in numeric_cols:
            if col in input_df.columns:
                input_df[col] = pd.to_numeric(input_df[col], errors='coerce').astype(float)

        # 3. DỰ ĐOÁN
        result = self.model.predict(input_df)
        
        return {
            "predictions": result.tolist(),
            "status": "success"
        }
