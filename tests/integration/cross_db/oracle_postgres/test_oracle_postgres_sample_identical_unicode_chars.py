"""
Test Unicode and special characters comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


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