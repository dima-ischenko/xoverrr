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
    def setup_clickhouse_null_data(self, clickhouse_engine, postgres_engine):
        """Setup NULL test data for ClickHouse vs PostgreSQL"""
        # ClickHouse
        with clickhouse_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_ch_nulls"))
            
            conn.execute(text("""
                CREATE TABLE test_ch_nulls (
                    id UInt32,
                    nullable_string Nullable(String),
                    nullable_number Nullable(Int32),
                    nullable_date Nullable(Date),
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """))
            
            conn.execute(text("""
                INSERT INTO test_ch_nulls (id, nullable_string, nullable_number, nullable_date, created_at) VALUES
                (1, NULL, NULL, NULL, '2024-01-01'),
                (2, 'Some text', 123, '2024-01-02', '2024-01-02'),
                (3, NULL, 456, NULL, '2024-01-03')
            """))
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_ch_nulls CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_ch_nulls (
                    id INTEGER PRIMARY KEY,
                    nullable_string TEXT,
                    nullable_number INTEGER,
                    nullable_date DATE,
                    created_at DATE NOT NULL
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_ch_nulls (id, nullable_string, nullable_number, nullable_date, created_at) VALUES
                (1, NULL, NULL, NULL, '2024-01-01'),
                (2, 'Some text', 123, '2024-01-02', '2024-01-02'),
                (3, NULL, 456, NULL, '2024-01-03')
            """))
        
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
        print(f"✓ ClickHouse NULL values comparison passed: {stats.final_score:.2f}%")