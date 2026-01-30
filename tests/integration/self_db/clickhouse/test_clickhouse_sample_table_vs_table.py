"""
Self-comparison test for ClickHouse table vs table.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS

class TestClickHouseTableVsTable:
    """Self-comparison tests within ClickHouse database"""
    
    @pytest.fixture(autouse=True)
    def setup_table_vs_table_data(self, clickhouse_engine):
        """Setup test data for ClickHouse table vs table comparison"""
        
        table_name_main = "test_self_ch_table_main"
        table_name_copy = "test_self_ch_table_copy"
        
        with clickhouse_engine.begin() as conn:
            # Clean up
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name_main}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name_copy}"))
            
            # Create main table
            conn.execute(text(f"""
                CREATE TABLE {table_name_main} (
                    id UInt32,
                    value String,
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """))
            
            # Insert data
            conn.execute(text(f"""
                INSERT INTO {table_name_main} VALUES
                (1, 'Value A', '2024-01-01'),
                (2, 'Value B', '2024-01-02'),
                (3, 'Value C', '2024-01-03')
            """))
            
            # Create copy table (identical structure, same data)
            conn.execute(text(f"""
                CREATE TABLE {table_name_copy} (
                    id UInt32,
                    value String,
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """))
            
            conn.execute(text(f"""
                INSERT INTO {table_name_copy} 
                SELECT * FROM {table_name_main}
            """))
        
        yield
        
        # Cleanup
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name_main}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name_copy}"))

    def test_clickhouse_table_vs_table(self, clickhouse_engine):
        """
        Test comparison between two identical ClickHouse tables.
        """
        table_name_main = "test_self_ch_table_main"
        table_name_copy = "test_self_ch_table_copy"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name_main, "test"),
            target_table=DataReference(table_name_copy, "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-04"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"ClickHouse table vs table comparison passed: {stats.final_score:.2f}%")