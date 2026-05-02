import os

import redis


def get_redis_client():
    host = os.getenv('REDIS_HOST', 'redis')
    port = int(os.getenv('REDIS_PORT', '6379'))
    db = int(os.getenv('REDIS_DB', '0'))
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)
