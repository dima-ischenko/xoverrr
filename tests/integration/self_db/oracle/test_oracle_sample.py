"""
Test Oracle self-comparison with identical data.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestOracleSelfComparison:
    """
    Tests comparing Oracle with itself (same engine).
    """
    
    @pytest.fixture(autouse=True)
    def setup_oracle_data(self, oracle_engine, table_helper):
        """Setup Oracle test data for self-comparison"""
        
        table_name = "test_oracle_self"
        
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          NUMBER PRIMARY KEY,
                    name        VARCHAR2(100) NOT NULL,
                    amount      NUMBER(10,2),
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, amount, created_at, updated_at) VALUES
                (1, 'Product A', 100.50, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00'),
                (2, 'Product B', 250.75, DATE '2024-01-02', TIMESTAMP '2024-01-02 11:30:00'),
                (3, 'Product C', 99.99, DATE '2024-01-03', TIMESTAMP '2024-01-03 14:45:00')
            """
        )
        
        yield

    def test_oracle_self_comparison_identical(self, oracle_engine):
        """
        Compare identical tables within same Oracle database.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_oracle_self", "test"),
            target_table=DataReference("test_oracle_self", "test"),
            date_column="created_at",
            update_column="updated_at",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"Oracle self-comparison passed: {stats.final_score:.2f}%")

    def test_oracle_table_vs_view(self, oracle_engine, table_helper):
        """
        Compare Oracle table with view on the same data.
        """
      # Create a view for testing
        table_helper.create_view(
            engine=oracle_engine,
            view_name="v_test_oracle_self",
            view_sql=f"""
                CREATE VIEW v_test_oracle_self AS
                SELECT id, name, amount, created_at, updated_at
                FROM test.test_oracle_self
            """
        )

        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_oracle_self", "test"),  # таблица
            target_table=DataReference("v_test_oracle_self", "test"),  # вьюха
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"Oracle table vs view comparison passed: {stats.final_score:.2f}%")