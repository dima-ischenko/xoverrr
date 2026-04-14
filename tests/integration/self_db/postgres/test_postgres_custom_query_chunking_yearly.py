"""
Year-range custom-query chunking integration tests for PostgreSQL.
"""

import pytest

from xoverrr.constants import COMPARISON_FAILED, COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator


class TestPostgresCustomQueryYearlyChunking:
    @pytest.fixture(autouse=True)
    def setup_yearly_data(self, postgres_engine, table_helper):
        source_table = 'test_pg_custom_query_chunking_yearly'
        target_table = 'test_pg_custom_query_chunking_yearly_target'

        table_helper.create_table(
            engine=postgres_engine,
            table_name=source_table,
            create_sql=f"""
                CREATE TABLE {source_table} (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {source_table} (id, name, created_at)
                SELECT gs, 'name-' || gs, DATE '2024-01-01' + (gs - 1)
                FROM generate_series(1, 365) AS gs
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=target_table,
            create_sql=f"""
                CREATE TABLE {target_table} (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {target_table} (id, name, created_at)
                SELECT
                    gs,
                    CASE
                        WHEN gs IN (60, 180, 300) THEN 'changed-' || gs
                        ELSE 'name-' || gs
                    END,
                    DATE '2024-01-01' + (gs - 1)
                FROM generate_series(1, 365) AS gs
            """,
        )
        yield

    def test_custom_query_chunking_yearly_positive(self, postgres_engine):
        comparator = DataQualityComparator(postgres_engine, postgres_engine, timezone='UTC')
        query = """
            SELECT id, name, created_at
            FROM test.test_pg_custom_query_chunking_yearly
            WHERE created_at >= cast(:start_date as date)
              AND created_at <= cast(:end_date as date)
        """
        params = {'start_date': '2024-01-01', 'end_date': '2024-12-31'}

        status_full, _, stats_full, _ = comparator.compare_custom_query(
            source_query=query,
            source_params=params,
            target_query=query,
            target_params=params,
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        status_chunked, _, stats_chunked, _ = comparator.compare_custom_query(
            source_query=query,
            source_params=params,
            target_query=query,
            target_params=params,
            custom_primary_key=['id'],
            chunk_size_days=30,
            tolerance_percentage=0.0,
        )
        assert status_full == COMPARISON_SUCCESS
        assert status_chunked == COMPARISON_SUCCESS
        assert stats_chunked.final_diff_score == stats_full.final_diff_score

    def test_custom_query_chunking_yearly_negative(self, postgres_engine):
        comparator = DataQualityComparator(postgres_engine, postgres_engine, timezone='UTC')
        source_query = """
            SELECT id, name, created_at
            FROM test.test_pg_custom_query_chunking_yearly
            WHERE created_at >= cast(:start_date as date)
              AND created_at <= cast(:end_date as date)
        """
        target_query = """
            SELECT id, name, created_at
            FROM test.test_pg_custom_query_chunking_yearly_target
            WHERE created_at >= cast(:start_date as date)
              AND created_at <= cast(:end_date as date)
        """
        params = {'start_date': '2024-01-01', 'end_date': '2024-12-31'}

        status_full, _, stats_full, details_full = comparator.compare_custom_query(
            source_query=source_query,
            source_params=params,
            target_query=target_query,
            target_params=params,
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        status_chunked, _, stats_chunked, details_chunked = comparator.compare_custom_query(
            source_query=source_query,
            source_params=params,
            target_query=target_query,
            target_params=params,
            custom_primary_key=['id'],
            chunk_size_days=30,
            tolerance_percentage=0.0,
        )
        assert status_full == COMPARISON_FAILED
        assert status_chunked == COMPARISON_FAILED
        assert stats_chunked.final_diff_score == stats_full.final_diff_score
        assert (
            int(details_full.mismatches_per_column.set_index('column_name').loc['name', 'mismatch_count'])
            == 3
        )
        assert (
            int(
                details_chunked.mismatches_per_column.set_index('column_name').loc[
                    'name', 'mismatch_count'
                ]
            )
            == 3
        )
