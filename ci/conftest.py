import pytest
from sqlalchemy import create_engine


@pytest.fixture(scope="session")
def postgres_engine():
    """
    PostgreSQL SQLAlchemy engine for integration tests
    """
    return create_engine(
        "postgresql+psycopg2://test:test_pass@localhost:5433/test_db",
        pool_pre_ping=True,
    )


@pytest.fixture(scope="session")
def oracle_engine():
    """
    Oracle SQLAlchemy engine for integration tests
    """
    return create_engine(
        "oracle+oracledb://test:test_pass@localhost:1521/?service_name=test_db",
    )


@pytest.fixture(scope="session")
def clickhouse_engine():
    """
    ClickHouse SQLAlchemy engine for integration tests
    """
    return create_engine(
        "clickhouse+native://test_user:test_pass@localhost:9000/test_db",
    )
