"""
Test NULL values comparison between ClickHouse and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestClickHouseNullValues:
    """Tests for NULL values comparison with ClickHouse"""
    
    @pytest.fixture(autouse=True)
    def setup_clickhouse_null_data(self, clickhouse_engine, postgres_engine, table_helper):
        """Setup NULL test data for ClickHouse vs PostgreSQL"""
        
        table_name = "test_ch_nulls"
        
        # ClickHouse setup
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    nullable_string Nullable(String),
                    nullable_number Nullable(Int32),
                    nullable_date Nullable(Date),
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, nullable_string, nullable_number, nullable_date, created_at) VALUES
                (1, NULL, NULL, NULL, '2024-01-01'),
                (2, 'Some text', 123, '2024-01-02', '2024-01-02'),
                (3, NULL, 456, NULL, '2024-01-03')
            """
        )
        
        # PostgreSQL setup
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    nullable_string TEXT,
                    nullable_number INTEGER,
                    nullable_date DATE,
                    created_at DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, nullable_string, nullable_number, nullable_date, created_at) VALUES
                (1, NULL, NULL, NULL, '2024-01-01'),
                (2, 'Some text', 123, '2024-01-02', '2024-01-02'),
                (3, NULL, 456, NULL, '2024-01-03')
            """
        )
        
        yield

    def test_clickhouse_null_values_comparison(self, clickhouse_engine, postgres_engine):
        """
        Compare tables with NULL values between ClickHouse and PostgreSQL.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_ch_nulls", "test"),
            target_table=DataReference("test_ch_nulls", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"ClickHouse NULL values comparison passed: {stats.final_score:.2f}%")