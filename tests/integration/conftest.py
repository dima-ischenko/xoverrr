import logging
import time

import pytest
from sqlalchemy import create_engine, exc, text
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_fixed)

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(12),
    wait=wait_fixed(10),
    retry=retry_if_exception_type((exc.OperationalError, exc.DBAPIError)),
    reraise=True,
)
def wait_for_database(engine, db_name: str):
    """Wait for database to be ready with retry logic"""
    logger.info(f'Waiting for {db_name} to be ready...')
    with engine.begin() as conn:
        conn.execute(text('SELECT id FROM imalive'))

    logger.info(f'{db_name} is ready!')


@pytest.fixture(scope='session')
def postgres_engine():
    """PostgreSQL SQLAlchemy engine"""
    engine = create_engine(
        'postgresql+psycopg2://test_user:test_pass@localhost:5433/test_db',
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={'connect_timeout': 10},
    )
    wait_for_database(engine, 'PostgreSQL')
    return engine


@pytest.fixture(scope='session')
def oracle_engine():
    """Oracle SQLAlchemy engine"""
    engine = create_engine(
        'oracle+oracledb://test:test_pass@localhost:1521/?service_name=test_db',
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    wait_for_database(engine, 'Oracle')
    return engine


@pytest.fixture(scope='session')
def clickhouse_engine():
    """ClickHouse SQLAlchemy engine"""
    engine = create_engine(
        'clickhouse+native://test_user:test_pass@localhost:9000/test', pool_recycle=3600
    )
    wait_for_database(engine, 'ClickHouse')
    return engine


# Helper фикстура для создания/удаления тестовых таблиц
@pytest.fixture
def db_cleanup():
    """Yield None and cleanup after test if needed"""
    yield


class DBHelper:
    """Helper class for managing test tables"""

    def __init__(self):
        self._cleanup_stack = []

    @staticmethod
    def get_drop_sql(engine, object_name: str, object_type: str = 'table') -> str:
        """Get drop SQL for specific database and object type"""
        dialect = engine.dialect.name

        if dialect == 'oracle':
            if object_type == 'view':
                return f"""
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP VIEW {object_name}';
                    EXCEPTION
                        WHEN OTHERS THEN
                            IF SQLCODE != -942 THEN
                                RAISE;
                            END IF;
                    END;
                """
            else:
                return f"""
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE {object_name}';
                    EXCEPTION
                        WHEN OTHERS THEN
                            IF SQLCODE != -942 THEN
                                RAISE;
                            END IF;
                    END;
                """
        elif dialect == 'clickhouse':
            if object_type == 'view':
                return f'DROP VIEW IF EXISTS {object_name}'
            else:
                return f'DROP TABLE IF EXISTS {object_name}'
        elif dialect in ('postgresql', 'postgres'):
            if object_type == 'view':
                return f'DROP VIEW IF EXISTS {object_name} CASCADE'
            else:
                return f'DROP TABLE IF EXISTS {object_name} CASCADE'
        else:
            raise ValueError(f'Unsupported dialect: {dialect}')

    def drop_table(self, engine, table_name: str) -> None:
        """
        Drop a table from database
        """
        drop_sql = self.get_drop_sql(engine, table_name, 'table')

        with engine.begin() as conn:
            conn.execute(text(drop_sql))

    def drop_view(self, engine, view_name: str) -> None:
        """
        Drop a view from database
        """
        drop_sql = self.get_drop_sql(engine, view_name, 'view')

        with engine.begin() as conn:
            conn.execute(text(drop_sql))

    def create_table(
        self, engine, table_name: str, create_sql: str, insert_sql: str = None
    ) -> None:
        """
        Create a test table and register it for automatic cleanup
        """
        # Clean up if exists
        self.drop_table(engine, table_name)

        # Create table
        with engine.begin() as conn:
            conn.execute(text(create_sql))
            if insert_sql:
                conn.execute(text(insert_sql))

        # Register for cleanup
        self._cleanup_stack.append((engine, table_name, 'table'))

    def create_view(self, engine, view_name: str, view_sql: str) -> None:
        """
        Create a view and register it for automatic cleanup
        """
        # Clean up if exists
        self.drop_view(engine, view_name)

        # Create view
        with engine.begin() as conn:
            conn.execute(text(view_sql))

        # Register for cleanup
        self._cleanup_stack.append((engine, view_name, 'view'))

    def cleanup(self) -> None:
        """Cleanup all registered objects in reverse order"""
        for engine, object_name, object_type in reversed(self._cleanup_stack):
            if object_type == 'table':
                self.drop_table(engine, object_name)
            elif object_type == 'view':
                self.drop_view(engine, object_name)
        self._cleanup_stack.clear()


@pytest.fixture
def table_helper():
    """Fixture providing DBHelper instance"""
    helper = DBHelper()
    yield helper
    # helper.cleanup() #do not cleanup for the debug
