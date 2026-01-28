"""
Self-comparison test for ClickHouse table vs view.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS

class TestClickHouseTableVsView:
    """Self-comparison tests for ClickHouse table vs view"""
    
    @pytest.fixture(autouse=True)
    def setup_table_vs_view_data(self, clickhouse_engine):
        """Setup test data for ClickHouse table vs view comparison"""
        
        table_name = "test_self_ch_table_view_main"
        view_name = "v_test_self_ch_table_view"
        
        with clickhouse_engine.begin() as conn:
            # Clean up
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            
            # Create main table
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    name String,
                    price Decimal(10,2),
                    created_at Date,
                    internal_flag UInt8
                )
                ENGINE = MergeTree()
                ORDER BY id
            """))
            
            # Insert data
            conn.execute(text(f"""
                INSERT INTO {table_name} VALUES
                (1, 'Product A', 99.99, '2024-01-01', 1),
                (2, 'Product B', 149.50, '2024-01-02', 0),
                (3, 'Product C', 199.00, '2024-01-03', 1)
            """))
            
            # Create a view
            conn.execute(text(f"""
                CREATE VIEW {view_name} AS
                SELECT 
                    id,
                    name,
                    price,
                    created_at
                FROM {table_name}
            """))
        
        yield
        
        # Cleanup
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_clickhouse_table_vs_view(self, clickhouse_engine):
        """
        Test comparison between ClickHouse table and view.
        """
        table_name = "test_self_ch_table_view_main"
        view_name = "v_test_self_ch_table_view"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(view_name, "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-04"),
            include_columns=["id", "name", "price", "created_at"],
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"✓ ClickHouse table vs view comparison passed: {stats.final_score:.2f}%")