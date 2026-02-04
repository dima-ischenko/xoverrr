"""
Test sample comparison with compound primary key between ClickHouse and PostgreSQL.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestClickHousePostgresCompoundKey:
    """Cross-database sample comparison with compound primary key"""

    @pytest.fixture(autouse=True)
    def setup_compound_key_data(self, clickhouse_engine, postgres_engine, table_helper):
        """Setup test data with compound primary key"""

        table_name = 'test_ch_pg_compound_key'

        # ClickHouse setup
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    user_id UInt32,
                    session_id String,
                    page_views UInt32,
                    duration Float64,
                    event_date Date,
                    device_type String
                )
                ENGINE = MergeTree()
                ORDER BY (user_id, session_id)
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (user_id, session_id, page_views, duration, event_date, device_type) VALUES
                (1001, 'sess_001', 15, 120.5, '2024-01-01', 'mobile'),
                (1002, 'sess_002', 8, 45.25, '2024-01-01', 'desktop'),
                (1001, 'sess_004', 12, 95.75, '2024-01-02', 'mobile')
            """,
        )

        # PostgreSQL setup
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    user_id INTEGER,
                    session_id TEXT,
                    page_views INTEGER,
                    duration DOUBLE PRECISION,
                    event_date DATE,
                    device_type TEXT,
                    PRIMARY KEY (user_id, session_id)
                )
            """,
            insert_sql=f"""
                 INSERT INTO {table_name} (user_id, session_id, page_views, duration, event_date, device_type) VALUES
                (1001, 'sess_001', 15, 120.5, '2024-01-01', 'mobile'),
                (1002, 'sess_002', 8, 45.25, '2024-01-01', 'desktop'),
                (1001, 'sess_004', 12, 95.75, '2024-01-02', 'mobile');
                """,
        )

        yield

    def test_sample_with_compound_key(self, clickhouse_engine, postgres_engine):
        """
        Test sample comparison with compound primary key.
        """
        table_name = 'test_ch_pg_compound_key'

        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='event_date',
            update_column=None,
            date_range=('2024-01-01', '2024-01-03'),
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(
            f'ClickHouse   PostgreSQL compound key comparison passed: {stats.final_score:.2f}%'
        )
