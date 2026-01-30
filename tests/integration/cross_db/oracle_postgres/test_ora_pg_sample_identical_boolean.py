"""
Test boolean type comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS

class TestBooleanComparison:
    """Tests for boolean type comparison"""
    
    @pytest.fixture(autouse=True)
    def setup_boolean_data(self, oracle_engine, postgres_engine):
        """Setup boolean test data"""
        
        table_name = "test_types_boolean"
        
        # Oracle: boolean as NUMBER(1)
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
                    is_active NUMBER(1) CHECK (is_active IN (0, 1)),
                    created_at DATE
                )
            """))
            
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, is_active, created_at) VALUES
                (1, 1, DATE '2024-01-01'),
                (2, 0, DATE '2024-01-02'),
                (3, 1, DATE '2024-01-03')
            """))
        
        # PostgreSQL: boolean as BOOLEAN
        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    is_active BOOLEAN,
                    created_at DATE
                )
            """))
            
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, is_active, created_at) VALUES
                (1, TRUE, '2024-01-01'),
                (2, FALSE, '2024-01-02'),
                (3, TRUE, '2024-01-03')
            """))
        
        yield
        
        # Cleanup
        with oracle_engine.begin() as conn:
            try:
                conn.execute(text(f"DROP TABLE {table_name} CASCADE CONSTRAINTS"))
            except:
                pass
        
        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))

    def test_boolean_comparison(self, oracle_engine, postgres_engine):
        """
        Compare boolean values between Oracle (0/1) and PostgreSQL (TRUE/FALSE).
        Adapters should handle type conversion.
        """
        table_name = "test_types_boolean"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )

        # Adapters should convert both to string representation
        assert status == COMPARISON_SUCCESS
        print(f"Boolean comparison passed: {stats.final_score:.2f}%")