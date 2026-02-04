"""
Test boolean type comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestBooleanComparison:
    """Tests for boolean type comparison"""

    @pytest.fixture(autouse=True)
    def setup_boolean_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup boolean test data"""

        table_name = 'test_types_boolean'

        # Oracle: boolean as NUMBER(1)
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    is_active NUMBER(1) CHECK (is_active IN (0, 1)),
                    created_at DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, is_active, created_at) VALUES
                (1, 1, DATE '2024-01-01'),
                (2, 0, DATE '2024-01-02'),
                (3, 1, DATE '2024-01-03')
            """,
        )

        # PostgreSQL: boolean as BOOLEAN
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    is_active BOOLEAN,
                    created_at DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, is_active, created_at) VALUES
                (1, TRUE, '2024-01-01'),
                (2, FALSE, '2024-01-02'),
                (3, TRUE, '2024-01-03')
            """,
        )

        yield

    def test_boolean_comparison(self, oracle_engine, postgres_engine):
        """
        Compare boolean values between Oracle (0/1) and PostgreSQL (TRUE/FALSE).
        Adapters should handle type conversion.
        """
        table_name = 'test_types_boolean'

        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )

        # Adapters should convert both to string representation
        assert status == COMPARISON_SUCCESS
        print(f'Boolean comparison passed: {stats.final_score:.2f}%')
