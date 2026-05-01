import os
import sys
import bentoml
from mlflow.tracking import MlflowClient
import mlflow
# from dotenv import load_dotenv
# load_dotenv()


# 1. Lấy thông tin từ biến môi trường (khớp với logic của file Mage bạn cung cấp)
# Đảm bảo bạn đã có các biến này trong file .env cục bộ
minio_user = os.getenv('MINIO_ROOT_USER')
minio_password = os.getenv('MINIO_ROOT_PASSWORD')
minio_endpoint = os.getenv('MINIO_ENDPOINT_URL')
# minio_endpoint = "http://localhost:9000"

# 2. Cấu hình để botocore/mlflow hiểu rằng đây là MinIO (giả lập S3)
# Việc set các biến AWS_... này sẽ ghi đè các token cũ đã hết hạn
# os.environ['AWS_ACCESS_KEY_ID'] = minio_user
# os.environ['AWS_SECRET_ACCESS_KEY'] = minio_password
# os.environ['MLFLOW_S3_ENDPOINT_URL'] = minio_endpoint

# os.environ['AWS_DEFAULT_REGION'] = 'us-east-1' 
# os.environ['MLFLOW_S3_IGNORE_TLS'] = 'true'



def import_from_mlflow(model_name: str = "Churn_Prediction_KKBox",
                       bentoml_name: str = "churn_xgboost_model"):
    """
    Connects to MLflow, finds the Production version of `model_name` (or latest fallback),
    and imports it into BentoML under `bentoml_name`.
    Returns the imported BentoML model object.
    """
    mlflow_uri = os.getenv('MLFLOW_ENDPOINT_URL')
    # mlflow_uri = "http://localhost:5000"
    print(f"-- Using MLflow tracking URI: {mlflow_uri}")
    mlflow.set_tracking_uri(mlflow_uri)

    client = MlflowClient()
    try:
        # Tìm theo Stage "Production" (cột màu xanh lá trong ảnh của bạn)
        prod_versions = client.get_latest_versions(model_name, stages=["Production"])
        if prod_versions:
            version_num = prod_versions[0].version
            print(f"-- Found Production stage for version {version_num}")
            model_uri = f"models:/{model_name}/{version_num}"
        else:
            # Fallback nếu không có stage Production
            latest_version_info = client.get_registered_model(model_name).latest_versions[0]
            model_uri = f"models:/{model_name}/{latest_version_info.version}"
            
    except Exception as e:
        print(f"❌ Error when querying MLflow Registry: {e}")
        raise

    try:
        print(f"-- Importing MLflow model '{model_uri}' into BentoML as '{bentoml_name}'...")
        bento_model = bentoml.mlflow.import_model(bentoml_name, model_uri)
        print(f"✅ BentoML import successful: {bento_model.tag}")
        return bento_model
    except Exception as e:
        print(f"❌ Failed to import model into BentoML: {e}")
        raise

if __name__ == "__main__":
    # Allow optional CLI args: python import_model.py <mlflow_model_name> <bentoml_name>
    mlflow_model = sys.argv[1] if len(sys.argv) > 1 else "Churn_Prediction_KKBox"
    bento_name = sys.argv[2] if len(sys.argv) > 2 else "churn_xgboost_model"
    import_from_mlflow(mlflow_model, bento_name)
    
