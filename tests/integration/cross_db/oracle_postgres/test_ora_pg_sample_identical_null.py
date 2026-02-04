"""
Test NULL values comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestNullValuesComparison:
    """Tests for NULL values comparison"""

    @pytest.fixture(autouse=True)
    def setup_null_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup NULL test data"""

        table_name = 'test_edge_nulls'

        # Oracle
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    nullable_string VARCHAR2(100),
                    nullable_number NUMBER,
                    nullable_date DATE,
                    created_at DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, nullable_string, nullable_number, nullable_date, created_at) VALUES
                (1, NULL, NULL, NULL, DATE '2024-01-01'),
                (2, 'Some text', 123, DATE '2024-01-02', DATE '2024-01-02'),
                (3, NULL, 456, NULL, DATE '2024-01-03')
            """,
        )

        # PostgreSQL
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    nullable_string TEXT,
                    nullable_number INTEGER,
                    nullable_date DATE,
                    created_at DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, nullable_string, nullable_number, nullable_date, created_at) VALUES
                (1, NULL, NULL, NULL, '2024-01-01'),
                (2, 'Some text', 123, '2024-01-02', '2024-01-02'),
                (3, NULL, 456, NULL, '2024-01-03')
            """,
        )

        yield

    def test_null_values_comparison(self, oracle_engine, postgres_engine):
        """
        Compare tables with NULL values in different columns.
        """
        table_name = 'test_edge_nulls'

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

        assert status == COMPARISON_SUCCESS
        print(f'NULL values comparison passed: {stats.final_score:.2f}%')
