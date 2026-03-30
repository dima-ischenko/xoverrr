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

        table_name = 'test_types_numeric_large'

        # Oracle
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    large_id NUMBER
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, large_id) VALUES
                (1, 11112222333344445)
            """,
        )

        # PostgreSQL
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    large_id NUMERIC
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, large_id) VALUES
                (1, 11112222333344445)
            """,
        )

        yield

    def test_numeric_types_large_comparison(self, oracle_engine, postgres_engine):
        #pytest.skip('issue #48')
        """
        Compare numeric types with the large value (16+ digits)
        """
        table_name = 'test_types_numeric_large'

        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )
        print(report)

        assert status == COMPARISON_SUCCESS

