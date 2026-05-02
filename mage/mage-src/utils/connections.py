import os
from urllib.parse import quote_plus

from utils.redis_client import get_redis_client


def get_postgres_url() -> str:
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgres')
    host = os.getenv('DB_HOST', 'postgres')
    port = os.getenv('DB_PORT', '5432')
    database = os.getenv('DB_NAME', 'postgres')
    return (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{database}"
    )


def get_mlflow_tracking_uri() -> str:
    return os.getenv('MLFLOW_TRACKING_URI') or os.getenv('MLFLOW_ENDPOINT_URL', 'http://mlflow:5000')


def get_minio_config() -> dict:
    return {
        'endpoint_url': os.getenv('MINIO_ENDPOINT_URL', 'http://minio:9000'),
        'aws_access_key_id': os.getenv('MINIO_ROOT_USER', ''),
        'aws_secret_access_key': os.getenv('MINIO_ROOT_PASSWORD', ''),
        'region_name': os.getenv('MINIO_REGION', 'us-east-1'),
        'features_bucket': os.getenv('MINIO_FEATURES_BUCKET_NAME', 'features-store'),
        'models_bucket': os.getenv('MINIO_MODELS_BUCKET_NAME', 'mlflow-artifacts'),
    }
