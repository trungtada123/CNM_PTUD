import polars as pl
import os

@data_loader
def load_data_from_minio(*args, **kwargs):
    bucket_name = os.getenv('MINIO_FEATURES_BUCKET_NAME')
    if not fs.exists(bucket_name):
        raise ValueError(f"Bucket '{bucket_name}' does not exist in MinIO.")
        
    df = pl.read_parquet(f"s3://{bucket_name}/snapshots/*.parquet", ...)
    return df.to_pandas()