from sqlalchemy import create_engine

from utils.connections import get_postgres_url


def get_sqlalchemy_engine():
    return create_engine(get_postgres_url(), pool_pre_ping=True)
