
import pytest
from sqlalchemy import create_engine, text, exc
import time
import logging
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(12),
    wait=wait_fixed(10),
    retry=retry_if_exception_type((exc.OperationalError, exc.DBAPIError)),
    reraise=True
)
def wait_for_database(engine, db_name: str):
    """Wait for database to be ready with retry logic"""
    logger.info(f"Waiting for {db_name} to be ready...")
    with engine.begin() as conn:
        conn.execute(text("SELECT id FROM imalive"))

    logger.info(f"{db_name} is ready!")

@pytest.fixture(scope="session")
def postgres_engine():
    """PostgreSQL SQLAlchemy engine"""
    engine = create_engine(
        "postgresql+psycopg2://test_user:test_pass@localhost:5433/test_db",
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
        "clickhouse+native://test_user:test_pass@localhost:9000/test",
        pool_recycle=3600
    )
    wait_for_database(engine, "ClickHouse")
    return engine

# Helper фикстура для создания/удаления тестовых таблиц
@pytest.fixture
def db_cleanup():
    """Yield None and cleanup after test if needed"""
    yield

class TableHelper:
    """Helper class for managing test tables"""
    
    def __init__(self):
        self._cleanup_stack = []
    
    @staticmethod
    def get_drop_sql(engine, table_name: str) -> str:
        """Get drop SQL for specific database"""
        dialect = engine.dialect.name
        
        if dialect == 'oracle':
            return f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """
        elif dialect == 'clickhouse':
            return f"DROP TABLE IF EXISTS {table_name}"
        elif dialect in ('postgresql', 'postgres'):
            return f"DROP TABLE IF EXISTS {table_name} CASCADE"
        else:
            raise ValueError(f"Unsupported dialect: {dialect}")
    
    def create_table(self, engine, table_name: str, create_sql: str, 
                    insert_sql: str = None, schema: str = "test") -> None:
        """
        Create a test table and register it for automatic cleanup
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        drop_sql = self.get_drop_sql(engine, full_table_name)
        
        # Clean up if exists
        with engine.begin() as conn:
            try:
                conn.execute(text(drop_sql))
            except Exception:
                pass  # Table might not exist
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text(create_sql))
            if insert_sql:
                conn.execute(text(insert_sql))
        
        # Register for cleanup
        self._cleanup_stack.append((engine, full_table_name, drop_sql))
    
    def cleanup(self) -> None:
        """Cleanup all registered tables"""
        for engine, table_name, drop_sql in reversed(self._cleanup_stack):
            with engine.begin() as conn:
                try:
                    conn.execute(text(drop_sql))
                except Exception:
                    pass
        self._cleanup_stack.clear()


@pytest.fixture
def table_helper():
    """Fixture providing TableHelper instance"""
    helper = TableHelper()
    yield helper
    #helper.cleanup() #do not cleanup for the debug