"""
Test PostgreSQL self-comparison with identical data.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestPostgresSelfComparison:
    """
    Tests comparing PostgreSQL with itself (same engine).
    """

    @pytest.fixture(autouse=True)
    def setup_postgres_data(self, postgres_engine, table_helper):
        """Setup PostgreSQL test data for self-comparison"""

        table_name = 'test_custom_data2'

        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, created_at, updated_at) VALUES
                (1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
                (2, 'Robert',  '2024-01-02', '2024-01-02 11:00:00'),
                (3, 'Charlie', '2024-01-03', '2024-01-03 12:00:00')
            """,
        )

        # Create a view
        table_helper.create_view(
            engine=postgres_engine,
            view_name='vtest_custom_data2',
            view_sql=f"""
                CREATE VIEW vtest_custom_data2 AS
                SELECT id, name, created_at, updated_at
                FROM {table_name}
            """,
        )

        yield

    @pytest.fixture(autouse=True)
    def setup_postgres_data_mv(self, postgres_engine, table_helper):
        """Setup PostgreSQL test data for self-comparison"""

        table_name = 'test_custom_data3'

        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, created_at, updated_at) VALUES
                (1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
                (2, 'Robert',  '2024-01-02', '2024-01-02 11:00:00'),
                (3, 'Charlie', '2024-01-03', '2024-01-03 12:00:00')
            """,
        )

        # Create a view
        table_helper.create_mview(
            engine=postgres_engine,
            mview_name='mvtest_custom_data3',
            mview_sql=f"""
              CREATE MATERIALIZED VIEW mvtest_custom_data3 AS
                SELECT id, name, created_at, updated_at
                FROM {table_name}
                WITH DATA
            """,
        )

        yield       

    def test_postgres_self_comparison_identical(self, postgres_engine, setup_postgres_data):
        """
        Compare identical tables within same PostgreSQL database.
        """
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference('test_custom_data2', 'test'),
            target_table=DataReference('test_custom_data2', 'test'),
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-01-03'),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f'PostgreSQL self-comparison passed: {stats.final_score:.2f}%')

    def test_postgres_self_comparison_identical_view(self, postgres_engine, setup_postgres_data):
        """
        Compare identical tables within same PostgreSQL database.
        """
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference('test_custom_data2', 'test'),
            target_table=DataReference('vtest_custom_data2', 'test'),
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-01-03'),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f'PostgreSQL self-comparison passed: {stats.final_score:.2f}%')

    def test_postgres_self_comparison_identical_mview(self, postgres_engine, setup_postgres_data_mv):
        """
        Compare identical tables within same PostgreSQL database.
        """
        #pytest.skip('issue #50')    
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference('test_custom_data3', 'test'),
            target_table=DataReference('mvtest_custom_data3', 'test'),
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-01-03'),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f'PostgreSQL self-comparison passed: {stats.final_score:.2f}%')       
