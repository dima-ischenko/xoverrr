"""
Test count-based check between ClickHouse and Oracle.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


class TestClickHouseOracleCountsCheck:
    """Cross-database count-based check tests"""

    @pytest.fixture(autouse=True)
    def setup_count_data(self, clickhouse_engine, oracle_engine, table_helper):
        """Setup test data for count comparison"""

        table_name = 'test_ch_ora_counts'

        # ClickHouse setup
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_date Date,
                    event_type String
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, event_type) VALUES
                (1, '2024-01-01', 'login'),
                (2, '2024-01-01', 'purchase'),
                (3, '2024-01-01', 'logout'),
                (4, '2024-01-02', 'login'),
                (5, '2024-01-02', 'view')
            """,
        )

        # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    event_date DATE,
                    event_type VARCHAR2(50)
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, event_type) VALUES
                (1, DATE '2024-01-01', 'login'),
                (2, DATE '2024-01-01', 'purchase'),
                (3, DATE '2024-01-01', 'logout'),
                (4, DATE '2024-01-02', 'login'),
                (5, DATE '2024-01-02', 'view')
            """,
        )

        yield

    def test_counts_check(self, clickhouse_engine, oracle_engine):
        """
        Test count-based check between ClickHouse and Oracle.
        """
        table_name = 'test_ch_ora_counts'

        checker = DataQualityChecker(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = checker.check_counts(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='event_date',
            date_range=('2024-01-01', '2024-01-03'),
            tolerance_pct=0.0,
        )
        print(report)
        assert status == CHECK_SUCCESS
        print(f'ClickHouse   Oracle count check passed: {stats.final_score:.2f}%')
