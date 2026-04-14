"""
Year-range custom-query chunking integration tests for ClickHouse.
"""

import pytest

from xoverrr.constants import COMPARISON_FAILED, COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator


class TestClickHouseCustomQueryYearlyChunking:
    @pytest.fixture(autouse=True)
    def setup_yearly_data(self, clickhouse_engine, table_helper):
        source_table = 'test_ch_custom_query_chunking_yearly'
        target_table = 'test_ch_custom_query_chunking_yearly_target'

        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=source_table,
            create_sql=f"""
                CREATE TABLE {source_table} (
                    id          UInt32,
                    name        String,
                    created_at  Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {source_table} (id, name, created_at)
                SELECT
                    number + 1,
                    concat('name-', toString(number + 1)),
                    toDate('2024-01-01') + toIntervalDay(number)
                FROM numbers(365)
            """,
        )
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=target_table,
            create_sql=f"""
                CREATE TABLE {target_table} (
                    id          UInt32,
                    name        String,
                    created_at  Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {target_table} (id, name, created_at)
                SELECT
                    number + 1,
                    if(
                        number + 1 IN (60, 180, 300),
                        concat('changed-', toString(number + 1)),
                        concat('name-', toString(number + 1))
                    ),
                    toDate('2024-01-01') + toIntervalDay(number)
                FROM numbers(365)
            """,
        )
        yield

    def test_custom_query_chunking_yearly_positive(self, clickhouse_engine):
        comparator = DataQualityComparator(clickhouse_engine, clickhouse_engine, timezone='UTC')
        query = """
            SELECT id, name, created_at
            FROM test_ch_custom_query_chunking_yearly
            WHERE created_at >= toDate(:start_date)
              AND created_at <= toDate(:end_date)
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

    def test_custom_query_chunking_yearly_negative(self, clickhouse_engine):
        comparator = DataQualityComparator(clickhouse_engine, clickhouse_engine, timezone='UTC')
        source_query = """
            SELECT id, name, created_at
            FROM test_ch_custom_query_chunking_yearly
            WHERE created_at >= toDate(:start_date)
              AND created_at <= toDate(:end_date)
        """
        target_query = """
            SELECT id, name, created_at
            FROM test_ch_custom_query_chunking_yearly_target
            WHERE created_at >= toDate(:start_date)
              AND created_at <= toDate(:end_date)
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
