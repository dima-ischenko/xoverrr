import pytest
from sqlalchemy import create_engine

@pytest.fixture(scope="session")
def postgres_engine():
    """PostgreSQL SQLAlchemy engine for integration tests"""
    return create_engine(
        "postgresql+psycopg2://test:test@localhost:5433/testdb"
    )


@pytest.fixture(scope="session")
def oracle_engine():
    """Oracle XE SQLAlchemy engine for integration tests"""
    return create_engine(
        "oracle+oracledb://test:test@localhost:1521/?service_name=XEPDB1"
    )


@pytest.fixture(scope="session")
def clickhouse_engine():
    """ClickHouse SQLAlchemy engine for integration tests"""
    return create_engine(
        "clickhouse+native://default:@localhost:9000/default"
    )
