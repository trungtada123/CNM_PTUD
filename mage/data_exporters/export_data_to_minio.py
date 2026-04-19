from pandas import DataFrame
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_minio(df: DataFrame, **kwargs) -> None:
    # Lấy thông tin từ biến môi trường của Mage container
    minio_user = os.getenv('MINIO_ROOT_USER', 'admin')
    minio_password = os.getenv('MINIO_ROOT_PASSWORD', 'admin')
    
    storage_options = {
        "key": minio_user,
        "secret": minio_password,
        "client_kwargs": {
            "endpoint_url": "http://minio:9000"
        }
    }
    
    # Lưu file dưới dạng parquet vào bucket 'features'
    file_path = "s3://features/churn_users_features.parquet"
    df.to_parquet(file_path, storage_options=storage_options, index=False)
    print(f"-- Đã lưu thành công vào MinIO tại {file_path}")