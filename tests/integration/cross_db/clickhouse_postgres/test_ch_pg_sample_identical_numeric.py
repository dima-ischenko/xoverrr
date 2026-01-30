"""
Test ClickHouse numeric types comparison with PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestClickHouseNumericTypes:
    """Tests for ClickHouse numeric types comparison with PostgreSQL"""
    
    @pytest.fixture(autouse=True)
    def setup_clickhouse_numeric_data(self, clickhouse_engine, postgres_engine):
        """Setup numeric test data for ClickHouse vs PostgreSQL"""
        # ClickHouse
        with clickhouse_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_ch_numerics"))
            
            conn.execute(text("""
                CREATE TABLE test_ch_numerics (
                    id UInt32,
                    price Decimal(10,2),
                    quantity UInt32,
                    discount Float64,
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """))
            
            conn.execute(text("""
                INSERT INTO test_ch_numerics (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, '2024-01-01'),
                (2, 250.75, 5, 0.15, '2024-01-02'),
                (3, 99.99, 20, 0.05, '2024-01-03')
            """))
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_ch_numerics CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_ch_numerics (
                    id INTEGER PRIMARY KEY,
                    price NUMERIC(10,2),
                    quantity INTEGER,
                    discount DOUBLE PRECISION,
                    created_at DATE
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_ch_numerics (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, '2024-01-01'),
                (2, 250.75, 5, 0.15, '2024-01-02'),
                (3, 99.99, 20, 0.05, '2024-01-03')
            """))
        
        yield

    def test_clickhouse_numeric_types_comparison(self, clickhouse_engine, postgres_engine):
        """
        Compare numeric types between ClickHouse and PostgreSQL.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_ch_numerics", "test"),
            target_table=DataReference("test_ch_numerics", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"ClickHouse numeric types comparison passed: {stats.final_score:.2f}%")