"""
Test ClickHouse numeric types comparison with PostgreSQL.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestClickHouseNumericTypes:
    """Tests for ClickHouse numeric types comparison with PostgreSQL"""

    @pytest.fixture(autouse=True)
    def setup_clickhouse_numeric_data(
        self, clickhouse_engine, postgres_engine, table_helper
    ):
        """Setup numeric test data for ClickHouse vs PostgreSQL"""

        table_name = 'test_ch_numerics'

        # ClickHouse setup
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    price Decimal(10,2),
                    quantity UInt32,
                    discount Float64,
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, '2024-01-01'),
                (2, 250.75, 5, 0.15, '2024-01-02'),
                (3, 99.99, 20, 0.05, '2024-01-03')
            """,
        )

        # PostgreSQL setup
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    price NUMERIC(10,2),
                    quantity INTEGER,
                    discount DOUBLE PRECISION,
                    created_at DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, '2024-01-01'),
                (2, 250.75, 5, 0.15, '2024-01-02'),
                (3, 99.99, 20, 0.05, '2024-01-03')
            """,
        )

        yield

    def test_clickhouse_numeric_types_comparison(
        self, clickhouse_engine, postgres_engine
    ):
        """
        Compare numeric types between ClickHouse and PostgreSQL.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference('test_ch_numerics', 'test'),
            target_table=DataReference('test_ch_numerics', 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f'ClickHouse numeric types comparison passed: {stats.final_score:.2f}%')
