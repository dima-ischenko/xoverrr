"""
Test numeric type comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestNumericTypesComparison:
    """Tests for numeric type comparison"""

    @pytest.fixture(autouse=True)
    def setup_numeric_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup numeric test data"""

        table_name = 'test_types_numeric'

        # Oracle
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    price NUMBER(10,2),
                    quantity INTEGER,
                    discount FLOAT,
                    created_at DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, DATE '2024-01-01'),
                (2, 250.75, 5, 0.15, DATE '2024-01-02'),
                (3, 99.99, 20, 0.05, DATE '2024-01-03')
            """,
        )

        # PostgreSQL
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    price NUMERIC(10,2),
                    quantity INTEGER,
                    discount DOUBLE PRECISION,
                    created_at DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, '2024-01-01'),
                (2, 250.75, 5, 0.15, '2024-01-02'),
                (3, 99.99, 20, 0.05, '2024-01-03')
            """,
        )

        yield

    def test_numeric_types_comparison(self, oracle_engine, postgres_engine):
        """
        Compare numeric types: Oracle NUMBER vs PostgreSQL NUMERIC.
        """
        table_name = 'test_types_numeric'

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
        print(f'Numeric types comparison passed: {stats.final_score:.2f}%')
