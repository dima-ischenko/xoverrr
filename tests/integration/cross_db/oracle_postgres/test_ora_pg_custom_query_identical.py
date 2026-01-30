"""
Test custom query comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestCustomQueryComparison:
    """Tests for custom query comparison"""
    
    @pytest.fixture(autouse=True)
    def setup_custom_data(self, oracle_engine, postgres_engine):
        """Setup custom query test data"""
        # Oracle
        with oracle_engine.begin() as conn:
            conn.execute(text("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_custom_data CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))
            
            conn.execute(text("""
                CREATE TABLE test_custom_data (
                    id          INTEGER PRIMARY KEY,
                    name        varchar2(256) NOT NULL,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_custom_data (id, name, created_at, updated_at) VALUES
                (1, 'Alice',   date'2024-01-01', timestamp'2024-01-01 10:00:00'),
                (2, 'Robert',  date'2024-01-02', timestamp'2024-01-02 11:00:00'),
                (3, 'Charlie', date'2024-01-03', timestamp'2024-01-03 12:00:00')
            """))
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_custom_data CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_custom_data (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_custom_data (id, name, created_at, updated_at) VALUES
                (1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
                (2, 'Robert',  '2024-01-02', '2024-01-02 11:00:00'),
                (3, 'Charlie', '2024-01-03', '2024-01-03 12:00:00')
            """))
        
        yield

    def test_custom_query_comparison(self, oracle_engine, postgres_engine):
        """
        Test custom query comparison between databases.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        source_query = """
            SELECT id, name, created_at
            FROM test.test_custom_data
            WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """
        
        target_query = """
            SELECT id, name, created_at
            FROM test.test_custom_data
            WHERE created_at >= date_trunc('day', %(start_date)s::date)
              AND created_at < date_trunc('day', %(end_date)s::date) + interval '1 days'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-03'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-03'},
            custom_primary_key=["id"],
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"Custom query comparison passed: {stats.final_score:.2f}%")