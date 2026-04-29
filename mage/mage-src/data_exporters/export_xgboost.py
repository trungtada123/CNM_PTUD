import os
import pickle
import boto3
from botocore.exceptions import ClientError

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_model_to_minio(model, *args, **kwargs) -> None:
    # 1. Cấu hình thông tin kết nối MinIO
    minio_user = os.getenv('MINIO_ROOT_USER')
    minio_password = os.getenv('MINIO_ROOT_PASSWORD')
    endpoint_url = os.getenv('MINIO_ENDPOINT_URL')
    bucket_name = os.getenv('MINIO_MODELS_BUCKET_NAME')
    
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=minio_user,
        aws_secret_access_key=minio_password,
        region_name='us-east-1'
    )
    
    # 2. Kiểm tra và tạo Bucket nếu chưa có (Dùng cách chuẩn của boto3)
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"-- Bucket '{bucket_name}' chưa có. Đang tạo mới...")
            s3_client.create_bucket(Bucket=bucket_name)
    
    # 3. Đóng gói mô hình thành file vật lý (.pkl)
    # Lấy Pipeline Run ID của Mage để đặt tên file cho khỏi trùng
    run_id = kwargs.get('execution_date', 'latest').strftime("%Y%m%d_%H%M%S") if hasattr(kwargs.get('execution_date'), 'strftime') else 'latest'
    file_name = f"xgboost_churn_{run_id}.pkl"
    local_file_path = f"/tmp/{file_name}" # Lưu tạm trên ổ cứng của container Mage
    
    with open(local_file_path, "wb") as f:
        pickle.dump(model, f)
        
    # 4. Upload file lên MinIO
    print(f"-- Đang upload {file_name} lên MinIO bucket '{bucket_name}'...")
    s3_client.upload_file(local_file_path, bucket_name, file_name)
    
    # Xóa file tạm cho nhẹ máy
    if os.path.exists(local_file_path):
        os.remove(local_file_path)
        
    print(f"Hoàn tất! \nMô hình đã được cất an toàn tại s3://{bucket_name}/{file_name}")
    