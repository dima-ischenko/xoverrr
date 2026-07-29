"""
Self-comparison test for ClickHouse table vs table.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


class TestClickHouseTableVsTable:
    """Self-comparison tests within ClickHouse database"""

    @pytest.fixture(autouse=True)
    def setup_table_vs_table_data(self, clickhouse_engine, table_helper):
        """Setup test data for ClickHouse table vs table comparison"""

        table_name_main = 'test_self_ch_table_main'
        table_name_copy = 'test_self_ch_table_copy'

        # Create main table
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name_main,
            create_sql=f"""
                CREATE TABLE {table_name_main} (
                    id UInt32,
                    value String,
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name_main} VALUES
                (1, 'Value A', '2024-01-01'),
                (2, 'Value B', '2024-01-02'),
                (3, 'Value C', '2024-01-03')
            """,
        )

        # Create copy table (identical structure, same data)
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name_copy,
            create_sql=f"""
                CREATE TABLE {table_name_copy} (
                    id UInt32,
                    value String,
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name_copy} VALUES
                (1, 'Value A', '2024-01-01'),
                (2, 'Value B', '2024-01-02'),
                (3, 'Value C', '2024-01-03')
            """,
        )

        yield

    def test_clickhouse_table_vs_table(self, clickhouse_engine):
        """
        Test comparison between two identical ClickHouse tables.
        """
        table_name_main = 'test_self_ch_table_main'
        table_name_copy = 'test_self_ch_table_copy'

        checker = DataQualityChecker(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = checker.check_sample(
            source_table=DataReference(table_name_main, 'test'),
            target_table=DataReference(table_name_copy, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_pct=0.0,
        )

        assert status == CHECK_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f'ClickHouse table vs table check passed: {stats.final_score:.2f}%')
