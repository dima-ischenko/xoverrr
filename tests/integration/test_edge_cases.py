# ci/tests/integration/test_edge_cases.py
import pytest
import sys
import os
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../src'))

from src import (
    DataQualityComparator,
    DataReference,
    COMPARISON_SUCCESS,
    COMPARISON_FAILED,
    COMPARISON_SKIPPED,
)

class TestNullValuesComparison:
    """Tests for NULL values comparison"""
    
    @pytest.fixture(autouse=True)
    def setup_null_data(self, oracle_engine, postgres_engine):
        """Setup NULL test data"""
        # Oracle
        with oracle_engine.begin() as conn:
            conn.execute(text("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_nulls CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))
            
            conn.execute(text("""
                CREATE TABLE test_nulls (
                    id NUMBER PRIMARY KEY,
                    nullable_string VARCHAR2(100),
                    nullable_number NUMBER,
                    nullable_date DATE,
                    created_at DATE NOT NULL
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_nulls (id, nullable_string, nullable_number, nullable_date, created_at) VALUES
                (1, NULL, NULL, NULL, DATE '2024-01-01'),
                (2, 'Some text', 123, DATE '2024-01-02', DATE '2024-01-02'),
                (3, NULL, 456, NULL, DATE '2024-01-03')
            """))
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_nulls CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_nulls (
                    id INTEGER PRIMARY KEY,
                    nullable_string TEXT,
                    nullable_number INTEGER,
                    nullable_date DATE,
                    created_at DATE NOT NULL
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_nulls (id, nullable_string, nullable_number, nullable_date, created_at) VALUES
                (1, NULL, NULL, NULL, '2024-01-01'),
                (2, 'Some text', 123, '2024-01-02', '2024-01-02'),
                (3, NULL, 456, NULL, '2024-01-03')
            """))
        
        yield

    def test_null_values_comparison(self, oracle_engine, postgres_engine):
        """
        Compare tables with NULL values in different columns.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_nulls", "test"),
            target_table=DataReference("test_nulls", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"✓ NULL values comparison passed: {stats.final_score:.2f}%")


class TestUnicodeComparison:
    """Tests for Unicode and special characters"""
    
    @pytest.fixture(autouse=True)
    def setup_unicode_data(self, oracle_engine, postgres_engine):
        """Setup Unicode test data"""
        # Oracle
        with oracle_engine.begin() as conn:
            conn.execute(text("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_unicode CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))
            
            conn.execute(text("""
                CREATE TABLE test_unicode (
                    id NUMBER PRIMARY KEY,
                    text_english VARCHAR2(200),
                    text_russian VARCHAR2(200),
                    text_emoji VARCHAR2(200),
                    created_date DATE
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_unicode (id, text_english, text_russian, text_emoji, created_date) VALUES
                (1, 'Hello World', 'Привет мир', '😀 🚀 📊', DATE '2024-01-01'),
                (2, 'Test data', 'Тестовые данные', '✅ ❌ ⚠️', DATE '2024-01-02')
            """))
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_unicode CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_unicode (
                    id INTEGER PRIMARY KEY,
                    text_english TEXT,
                    text_russian TEXT,
                    text_emoji TEXT,
                    created_date DATE
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_unicode (id, text_english, text_russian, text_emoji, created_date) VALUES
                (1, 'Hello World', 'Привет мир', '😀 🚀 📊', '2024-01-01'),
                (2, 'Test data', 'Тестовые данные', '✅ ❌ ⚠️', '2024-01-02')
            """))
        
        yield

    def test_unicode_special_chars(self, oracle_engine, postgres_engine):
        """
        Compare strings with Unicode and special characters.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_unicode", "test"),
            target_table=DataReference("test_unicode", "test"),
            date_column="created_date",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"✓ Unicode comparison passed: {stats.final_score:.2f}%")


class TestCustomQueryComparison:
    """Tests for custom query comparison"""
    
    def test_custom_query_comparison(self, oracle_engine, postgres_engine):
        """
        Test custom query comparison between databases.
        Uses existing customers table.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        source_query = """
            SELECT id, name, created_at, 
                   case when updated_at > (sysdate - 1/24) then 'y' end as xrecently_changed
            FROM test.customers
            WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """
        
        target_query = """
            SELECT id, name, created_at,
                   case when updated_at > (now() - INTERVAL :exclude_recent_hours hours) then 'y' end as xrecently_changed
            FROM test.customers
            WHERE created_at >= date_trunc('day', :start_date::date)
              AND created_at < date_trunc('day', :end_date::date) + interval '1 days'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-03'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-03', 'exclude_recent_hours': 1},
            custom_primary_key=["id"],
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"✓ Custom query comparison passed: {stats.final_score:.2f}%")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])