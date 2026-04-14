"""
Year-range chunking integration tests for ClickHouse.
"""

import pytest

from xoverrr.constants import COMPARISON_FAILED, COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestClickHouseYearlyChunking:
    @pytest.fixture(autouse=True)
    def setup_yearly_data(self, clickhouse_engine, table_helper):
        table_name = 'test_ch_chunking_yearly'
        target_table_name = 'test_ch_chunking_yearly_target'

        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          UInt32,
                    name        String,
                    created_at  Date,
                    updated_at  DateTime
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, created_at, updated_at)
                SELECT
                    number + 1,
                    concat('name-', toString(number + 1)),
                    toDate('2024-01-01') + toIntervalDay(number),
                    toDateTime('2024-01-01 00:00:00') + toIntervalDay(number)
                FROM numbers(365)
            """,
        )
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=target_table_name,
            create_sql=f"""
                CREATE TABLE {target_table_name} (
                    id          UInt32,
                    name        String,
                    created_at  Date,
                    updated_at  DateTime
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {target_table_name} (id, name, created_at, updated_at)
                SELECT
                    number + 1,
                    if(
                        number + 1 IN (60, 180, 300),
                        concat('changed-', toString(number + 1)),
                        concat('name-', toString(number + 1))
                    ),
                    toDate('2024-01-01') + toIntervalDay(number),
                    toDateTime('2024-01-01 00:00:00') + toIntervalDay(number)
                FROM numbers(365)
            """,
        )
        yield

    def test_clickhouse_chunking_30_days_matches_non_chunked(self, clickhouse_engine):
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone='UTC',
        )
        table_ref = DataReference('test_ch_chunking_yearly', 'test')

        status_counts_full, _, stats_counts_full, _ = comparator.compare_counts(
            source_table=table_ref,
            target_table=table_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-12-31'),
            tolerance_percentage=0.0,
        )
        status_counts_chunked, _, stats_counts_chunked, _ = comparator.compare_counts(
            source_table=table_ref,
            target_table=table_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-12-31'),
            chunk_size_days=30,
            tolerance_percentage=0.0,
        )

        status_sample_full, _, stats_sample_full, _ = comparator.compare_sample(
            source_table=table_ref,
            target_table=table_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-12-31'),
            tolerance_percentage=0.0,
        )
        status_sample_chunked, _, stats_sample_chunked, _ = comparator.compare_sample(
            source_table=table_ref,
            target_table=table_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-12-31'),
            chunk_size_days=30,
            tolerance_percentage=0.0,
        )

        assert status_counts_full == COMPARISON_SUCCESS
        assert status_counts_chunked == COMPARISON_SUCCESS
        assert stats_counts_chunked.final_diff_score == stats_counts_full.final_diff_score

        assert status_sample_full == COMPARISON_SUCCESS
        assert status_sample_chunked == COMPARISON_SUCCESS
        assert stats_sample_chunked.final_diff_score == stats_sample_full.final_diff_score

    def test_clickhouse_chunking_30_days_negative_sample(self, clickhouse_engine):
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone='UTC',
        )
        source_ref = DataReference('test_ch_chunking_yearly', 'test')
        target_ref = DataReference('test_ch_chunking_yearly_target', 'test')

        status_sample_full, _, stats_sample_full, details_sample_full = comparator.compare_sample(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-12-31'),
            tolerance_percentage=0.0,
        )
        status_sample_chunked, _, stats_sample_chunked, details_sample_chunked = (
            comparator.compare_sample(
                source_table=source_ref,
                target_table=target_ref,
                date_column='created_at',
                update_column='updated_at',
                date_range=('2024-01-01', '2024-12-31'),
                chunk_size_days=30,
                tolerance_percentage=0.0,
            )
        )

        assert status_sample_full == COMPARISON_FAILED
        assert status_sample_chunked == COMPARISON_FAILED
        assert stats_sample_chunked.final_diff_score == stats_sample_full.final_diff_score
        mismatch_full = details_sample_full.mismatches_per_column.set_index('column_name')
        mismatch_chunked = details_sample_chunked.mismatches_per_column.set_index(
            'column_name'
        )
        assert int(mismatch_full.loc['name', 'mismatch_count']) == 3
        assert int(mismatch_chunked.loc['name', 'mismatch_count']) == 3
