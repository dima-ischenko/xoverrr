"""
Test PostgreSQL self-comparison with identical data.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestPostgresSelfComparison:
    """
    Tests comparing PostgreSQL with itself (same engine).
    """
    
    @pytest.fixture(autouse=True)
    def setup_postgres_data(self, postgres_engine):
        """Setup PostgreSQL test data for self-comparison"""
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_custom_data2 CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_custom_data2 (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_custom_data2 (id, name, created_at, updated_at) VALUES
                (1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
                (2, 'Robert',  '2024-01-02', '2024-01-02 11:00:00'),
                (3, 'Charlie', '2024-01-03', '2024-01-03 12:00:00')
            """))

        """Setup view for PostgreSQL self-comparison"""
        with postgres_engine.begin() as conn:
            # Create a view
            conn.execute(text("DROP VIEW IF EXISTS vtest_custom_data2 CASCADE"))
            conn.execute(text("""
                CREATE VIEW vtest_custom_data2 AS
                SELECT id, name, created_at, updated_at
                FROM test_custom_data2
            """))
        
        yield

    def test_postgres_self_comparison_identical(self, postgres_engine):
        """
        Compare identical tables within same PostgreSQL database.
        """
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_custom_data2", "test"),
            target_table=DataReference("test_custom_data2", "test"),
            date_column="created_at",
            update_column="updated_at",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"PostgreSQL self-comparison passed: {stats.final_score:.2f}%")