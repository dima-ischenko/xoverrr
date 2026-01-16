
import pytest
from sqlalchemy import create_engine, text, exc
import time
import logging
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(10),
    retry=retry_if_exception_type((exc.OperationalError, exc.DBAPIError)),
    reraise=True
)
def wait_for_database(engine, db_name: str):
    """Wait for database to be ready with retry logic"""
    logger.info(f"Waiting for {db_name} to be ready...")
    with engine.begin() as conn:
        if "oracle" in str(engine.url).lower():
            conn.execute(text("SELECT 1 FROM dual"))
        else:
            conn.execute(text("SELECT 1"))
    logger.info(f"{db_name} is ready!")

@pytest.fixture(scope="session")
def postgres_engine():
    """PostgreSQL SQLAlchemy engine"""
    engine = create_engine(
        "postgresql+psycopg2://test:test_pass@localhost:5433/test_db",
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={"connect_timeout": 10}
    )
    wait_for_database(engine, "PostgreSQL")
    return engine

@pytest.fixture(scope="session")
def oracle_engine():
    """Oracle SQLAlchemy engine"""
    engine = create_engine(
        "oracle+oracledb://test:test_pass@localhost:1521/?service_name=test_db",
        pool_pre_ping=True,
        pool_recycle=3600
    )
    wait_for_database(engine, "Oracle")
    return engine

@pytest.fixture(scope="session")
def clickhouse_engine():
    """ClickHouse SQLAlchemy engine"""
    engine = create_engine(
        "clickhouse+native://test_user:test_pass@localhost:9000/test_db",
        pool_recycle=3600
    )
    wait_for_database(engine, "ClickHouse")
    return engine

# Helper фикстура для создания/удаления тестовых таблиц
@pytest.fixture
def db_cleanup():
    """Yield None and cleanup after test if needed"""
    yield