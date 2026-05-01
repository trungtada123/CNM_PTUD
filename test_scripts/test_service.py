import redis
import json
import pandas as pd
import os

from dotenv import load_dotenv
load_dotenv()




def load_features_from_redis(user_ids: list):
    """
    Load feature của danh sách msno từ Redis và trả về DataFrame.
    """
    r = redis.Redis(
        # host=os.getenv('REDIS_HOST', 'localhost'), 
        host='localhost', 
        port=os.getenv('REDIS_PORT', 6379), 
        db=0, 
        decode_responses=True
    )
    
    features_list = []
    for user_id in user_ids:
        raw_data = r.get(f"user_features:{user_id}")
        if raw_data:
            data = json.loads(raw_data)
            data['msno'] = user_id # Gán lại ID để biết của ai
            features_list.append(data)
    
    return pd.DataFrame(features_list)


import pandas as pd
import os

def load_features_from_minio():
    """
    Load file parquet 'sạch' từ MinIO.
    """
    minio_user = os.getenv('MINIO_ROOT_USER')
    minio_password = os.getenv('MINIO_ROOT_PASSWORD')
    # Lưu ý: Dùng localhost nếu chạy script từ máy, dùng 'minio' nếu chạy trong Docker
    # endpoint = os.getenv('MINIO_ENDPOINT_URL')
    endpoint = "http://localhost:9000"
    print(endpoint)
    bucket = os.getenv('MINIO_FEATURES_BUCKET_NAME')
    
    storage_options = {
        "key": minio_user,
        "secret": minio_password,
        "client_kwargs": {"endpoint_url": endpoint}
    }
    
    file_path = f"s3://{bucket}/churn_users_features.parquet"
    print(f"-- Đang load dữ liệu từ: {file_path}")
    
    return pd.read_parquet(file_path, storage_options=storage_options)


def finalize_features(df):
    temp = df.copy()
    
    # 1. Xử lý NaN cho cột số và chữ trước khi ép kiểu
    numeric_cols = temp.select_dtypes(include=['number']).columns
    temp[numeric_cols] = temp[numeric_cols].fillna(0)
    
    object_cols = temp.select_dtypes(include=['object']).columns
    temp[object_cols] = temp[object_cols].fillna('unknown')

    # 2. Loại bỏ các cột định danh/mục tiêu
    # Đảm bảo CHỈ còn lại các features dùng để train
    features_to_drop = ['msno', 'is_churn']
    temp = temp.drop(columns=[c for c in features_to_drop if c in temp.columns])

    # 3. Ép kiểu category (Bắt buộc vì bạn bật enable_categorical=True)
    for col in temp.select_dtypes(include=['object', 'category']).columns:
        temp[col] = temp[col].astype('category')
        
    return temp



import requests
import pandas as pd

# 1. Load dữ liệu từ MinIO (Dữ liệu đã được Mage xử lý sạch)
try:
    df_sample = load_features_from_minio().head(10)
    print(f"✅ Load thành công {len(df_sample)} bản ghi từ MinIO.")
except Exception as e:
    print(f"❌ Không load được MinIO, kiểm tra lại kết nối: {e}")
    raise SystemExit(1)


# df_final = finalize_features(df_sample)
df_final = finalize_features(df_sample)
print(f"-- Các cột gửi đi: {df_final.columns.tolist()}")
print(f"-- Số lượng features gửi đi: {len(df_final.columns)}")
print(f"-- Danh sách features: {df_final.columns.tolist()}")

# 3. Gửi dự đoán
payload = {"input_df": df_final.to_dict(orient="records")}
response = requests.post("http://localhost:3000/predict", json=payload)

print(f"HTTP {response.status_code}")
print(response.json())

