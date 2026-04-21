from pandas import DataFrame
import os
import s3fs

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_minio(df: DataFrame, **kwargs) -> None:
    # Lấy thông tin từ biến môi trường của Mage container
    minio_user = os.getenv('MINIO_ROOT_USER', 'admin')
    minio_password = os.getenv('MINIO_ROOT_PASSWORD', 'admin123')
    
    storage_options = {
        "key": minio_user,
        "secret": minio_password,
        "client_kwargs": {
            "endpoint_url": "http://minio:9000"
        }
    }
    
    # Tạo S3FileSystem để kiểm tra và tạo bucket
    fs = s3fs.S3FileSystem(**storage_options)
    bucket_name = "features"
    
    # Kiểm tra và tạo bucket nếu chưa tồn tại
    if not fs.exists(bucket_name):
        fs.mkdir(bucket_name)
        print(f"-- Đã tạo bucket '{bucket_name}'")
    
    # Lưu file dưới dạng parquet vào bucket 'features'
    file_path = f"s3://{bucket_name}/churn_users_features.parquet"
    df.to_parquet(file_path, storage_options=storage_options, index=False)
    print(f"-- Đã lưu thành công vào MinIO tại {file_path}")