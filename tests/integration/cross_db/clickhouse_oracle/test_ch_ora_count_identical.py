"""
Test count-based comparison between ClickHouse and Oracle.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS

class TestClickHouseOracleCountsComparison:
    """Cross-database count-based comparison tests"""
    
    @pytest.fixture(autouse=True)
    def setup_count_data(self, clickhouse_engine, oracle_engine):
        """Setup test data for count comparison"""
        
        table_name = "test_ch_ora_counts"
        
        # ClickHouse setup
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_date Date,
                    event_type String
                )
                ENGINE = MergeTree()
                ORDER BY id
            """))
            
            # 3 records on 2024-01-01, 2 on 2024-01-02
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_date, event_type) VALUES
                (1, '2024-01-01', 'login'),
                (2, '2024-01-01', 'purchase'),
                (3, '2024-01-01', 'logout'),
                (4, '2024-01-02', 'login'),
                (5, '2024-01-02', 'view')
            """))
        
        # Oracle setup
        with oracle_engine.begin() as conn:
            conn.execute(text(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))
            
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    event_date DATE,
                    event_type VARCHAR2(50)
                )
            """))
            
            # Same counts
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_date, event_type) VALUES
                (1, DATE '2024-01-01', 'login'),
                (2, DATE '2024-01-01', 'purchase'),
                (3, DATE '2024-01-01', 'logout'),
                (4, DATE '2024-01-02', 'login'),
                (5, DATE '2024-01-02', 'view')
            """))
        
        yield
        
        # Cleanup
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
        with oracle_engine.begin() as conn:
            try:
                conn.execute(text(f"DROP TABLE {table_name} CASCADE CONSTRAINTS"))
            except:
                pass

    def test_counts_comparison(self, clickhouse_engine, oracle_engine):
        """
        Test count-based comparison between ClickHouse and Oracle.
        """
        table_name = "test_ch_ora_counts"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f"ClickHouse → Oracle count comparison passed: {stats.final_score:.2f}%")