"""
Test DATE type comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestDateTypeComparison:
    """Tests for DATE type comparison"""
    
    @pytest.fixture(autouse=True)
    def setup_date_data(self, oracle_engine, postgres_engine):
        """Setup DATE test data"""
        # Oracle
        with oracle_engine.begin() as conn:
            conn.execute(text("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_dates CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))
            
            conn.execute(text("""
                CREATE TABLE test_dates (
                    id NUMBER PRIMARY KEY,
                    event_date DATE NOT NULL,
                    event_name VARCHAR2(100)
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_dates (id, event_date, event_name) VALUES
                (1, DATE '2024-01-01', 'New Year'),
                (2, DATE '2024-01-02', 'Second day'),
                (3, DATE '2024-01-03', 'Third day')
            """))
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_dates CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_dates (
                    id INTEGER PRIMARY KEY,
                    event_date DATE NOT NULL,
                    event_name TEXT
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_dates (id, event_date, event_name) VALUES
                (1, '2024-01-01', 'New Year'),
                (2, '2024-01-02', 'Second day'),
                (3, '2024-01-03', 'Third day')
            """))
        
        yield

    def test_date_type_comparison(self, oracle_engine, postgres_engine):
        """
        Compare DATE type between Oracle and PostgreSQL.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_dates", "test"),
            target_table=DataReference("test_dates", "test"),
            date_column="event_date",
            date_range=("2024-01-01", "2024-01-10"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"✓ Date type comparison passed: {stats.final_score:.2f}%")