"""
Test sample comparison with column exclusion between ClickHouse and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS

class TestClickHousePostgresColumnExclusion:
    """Cross-database sample comparison with column exclusion"""
    
    @pytest.fixture(autouse=True)
    def setup_column_exclusion_data(self, clickhouse_engine, postgres_engine):
        """Setup test data for column exclusion test"""
        
        table_name = "test_ch_pg_col_exclusion"
        
        # ClickHouse setup
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    name String,
                    created_at DateTime,
                    internal_id UInt32,
                    public_data String
                )
                ENGINE = MergeTree()
                ORDER BY id
            """))
            
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, name, created_at, internal_id, public_data) VALUES
                (1, 'Item A', '2024-01-01 10:00:00', 999, 'Public A'),
                (2, 'Item B', '2024-01-02 11:00:00', 888, 'Public B')
            """))
        
        # PostgreSQL setup
        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP,
                    internal_id INTEGER,
                    public_data TEXT
                )
            """))
            
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, name, created_at, internal_id, public_data) VALUES
                (1, 'Item A', '2024-01-01 10:00:00', 999, 'Public A'),
                (2, 'Item B', '2024-01-02 11:00:00', 888, 'Public B')
            """))
        
        yield
        
        # Cleanup
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))

    def test_sample_with_column_exclusion(self, clickhouse_engine, postgres_engine):
        """
        Test sample comparison with excluded columns.
        """
        table_name = "test_ch_pg_col_exclusion"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-03"),
            exclude_columns=["internal_id"],  # Exclude internal column
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"✓ ClickHouse → PostgreSQL with column exclusion passed: {stats.final_score:.2f}%")