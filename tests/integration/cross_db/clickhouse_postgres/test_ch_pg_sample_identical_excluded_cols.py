"""
Test sample check with column exclusion between ClickHouse and PostgreSQL.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


class TestClickHousePostgresColumnExclusion:
    """Cross-database sample check with column exclusion"""

    @pytest.fixture(autouse=True)
    def setup_column_exclusion_data(
        self, clickhouse_engine, postgres_engine, table_helper
    ):
        """Setup test data for column exclusion test"""

        table_name = 'test_ch_pg_col_exclusion'

        # ClickHouse setup
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    name String,
                    created_at DateTime,
                    internal_id UInt32,
                    public_data String
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, created_at, internal_id, public_data) VALUES
                (1, 'Item A', '2024-01-01 10:00:00', 999, 'Public A'),
                (2, 'Item B', '2024-01-02 11:00:00', 888, 'Public B')
            """,
        )

        # PostgreSQL setup
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP,
                    internal_id INTEGER,
                    public_data TEXT
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, created_at, internal_id, public_data) VALUES
                (1, 'Item A', '2024-01-01 12:00:00', 999, 'Public A'),
                (2, 'Item B', '2024-01-02 13:00:00', 888, 'Public B')
            """,
        )

        yield

    def test_sample_with_column_exclusion(self, clickhouse_engine, postgres_engine):
        """
        Test sample check with excluded columns.
        """
        table_name = 'test_ch_pg_col_exclusion'

        checker = DataQualityChecker(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = checker.check_sample(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-03'),
            exclude_columns=['internal_id'],  # Exclude internal column
            tolerance_pct=0.0,
        )
        print(report)
        assert status == CHECK_SUCCESS
        assert stats.final_diff_score == 0.0
        print(
            f'ClickHouse   PostgreSQL with column exclusion passed: {stats.final_score:.2f}%'
        )
