from pandas import DataFrame
import redis
import json

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_redis(df: DataFrame, **kwargs) -> None:
    # Kết nối đến Redis container
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    
    # Sử dụng pipeline để tối ưu tốc độ insert (batch insert)
    pipe = r.pipeline()
    count = 0
    
    for _, row in df.iterrows():
        user_id = row['msno']
        # Biến đổi feature thành dictionary, bỏ id
        features_dict = row.drop('msno').to_dict()
        
        # Lưu vào Redis dạng String JSON, key là 'user_features:{msno}'
        pipe.set(f"user_features:{user_id}", json.dumps(features_dict))
        count += 1
        
        # Cứ 1000 record thì execute 1 lần để tránh tràn RAM
        if count % 1000 == 0:
            pipe.execute()
    
    # Execute phần dư còn lại
    pipe.execute()
    print(f"-- Đã lưu thành công {count} records vào Redis.")