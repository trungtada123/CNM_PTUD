import os
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_features_from_minio(*args, **kwargs) -> pd.DataFrame:
    # 1. Lấy thông tin cấu hình y hệt như lúc Export
    minio_user = os.getenv('MINIO_ROOT_USER')
    minio_password = os.getenv('MINIO_ROOT_PASSWORD')
    bucket_name = os.getenv('MINIO_FEATURES_BUCKET_NAME')
        
    storage_options = {
        "key": minio_user,
        "secret": minio_password,
        "client_kwargs": {
            "endpoint_url": os.getenv('MINIO_ENDPOINT_URL')
        }
    }
    
    # 2. Đường dẫn tới file features bạn đã lưu ở Pipeline trước
    file_path = f"s3://{bucket_name}/churn_users_features.parquet"
    
    # 3. Đọc thẳng file parquet thành DataFrame
    print(f"-- Đang tải dữ liệu đặc trưng từ {file_path}...")
    df = pd.read_parquet(file_path, storage_options=storage_options)
    
    print(f"-- Tải thành công! Kích thước dữ liệu: {df.shape}")
    
    # 4. Trả DataFrame này về cho block Transformer (để train XGBoost)
    return df