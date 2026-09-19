"""
Year-range chunking integration tests for ClickHouse.
"""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


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
        checker = DataQualityChecker(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone='UTC',
        )
        table_ref = DataReference('test_ch_chunking_yearly', 'test')

        result = checker.check_counts_group_by_date(
            source_table=table_ref,
            target_table=table_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-12-31'),
            tolerance_pct=0.0,
        )
        status_counts_full = result.status
        stats_counts_full = result.stats
        result = checker.check_counts_group_by_date(
            source_table=table_ref,
            target_table=table_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-12-31'),
            chunk_size_days=30,
            tolerance_pct=0.0,
        )
        status_counts_chunked = result.status
        stats_counts_chunked = result.stats

        result = checker.check_samples(
            source_table=table_ref,
            target_table=table_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-12-31'),
            tolerance_pct=0.0,
        )
        status_sample_full = result.status
        stats_sample_full = result.stats
        result = checker.check_samples(
            source_table=table_ref,
            target_table=table_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-12-31'),
            chunk_size_days=30,
            tolerance_pct=0.0,
        )
        status_sample_chunked = result.status
        stats_sample_chunked = result.stats

        assert status_counts_full == CHECK_SUCCESS
        assert status_counts_chunked == CHECK_SUCCESS
        assert (
            stats_counts_chunked.final_diff_score == stats_counts_full.final_diff_score
        )

        assert status_sample_full == CHECK_SUCCESS
        assert status_sample_chunked == CHECK_SUCCESS
        assert (
            stats_sample_chunked.final_diff_score == stats_sample_full.final_diff_score
        )

    def test_clickhouse_chunking_30_days_negative_sample(self, clickhouse_engine):
        checker = DataQualityChecker(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone='UTC',
        )
        source_ref = DataReference('test_ch_chunking_yearly', 'test')
        target_ref = DataReference('test_ch_chunking_yearly_target', 'test')

        result = checker.check_samples(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-12-31'),
            tolerance_pct=0.0,
        )
        status_sample_full = result.status
        stats_sample_full = result.stats
        details_sample_full = result.details
        result = checker.check_samples(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-12-31'),
            chunk_size_days=30,
            tolerance_pct=0.0,
        )
        status_sample_chunked = result.status
        stats_sample_chunked = result.stats
        details_sample_chunked = result.details

        assert status_sample_full == CHECK_FAILED
        assert status_sample_chunked == CHECK_FAILED
        assert (
            stats_sample_chunked.final_diff_score == stats_sample_full.final_diff_score
        )
        mismatch_full = details_sample_full.issue_breakdown.set_index('column_name')
        mismatch_chunked = details_sample_chunked.issue_breakdown.set_index(
            'column_name'
        )
        assert int(mismatch_full.loc['name', 'issue_count']) == 3
        assert int(mismatch_chunked.loc['name', 'issue_count']) == 3

    def test_clickhouse_total_counts_chunking_matches_non_chunked(
        self, clickhouse_engine
    ):
        checker = DataQualityChecker(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone='UTC',
        )
        source_ref = DataReference('test_ch_chunking_yearly', 'test')
        target_ref = DataReference('test_ch_chunking_yearly_target', 'test')

        result_full = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-12-31'),
            tolerance_pct=0.0,
        )
        result_chunked = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-12-31'),
            chunk_size_days=30,
            tolerance_pct=0.0,
        )

        assert result_full.status == CHECK_SUCCESS
        assert result_chunked.status == CHECK_SUCCESS
        assert result_full.stats.total_source_rows == 365
        assert result_chunked.stats.total_source_rows == 365
        assert result_chunked.stats.total_target_rows == 365
        assert result_chunked.stats.final_diff_score == 0.0
        assert 'chunks processed' in result_chunked.report

    def test_open_ended_date_range_without_chunking(self, clickhouse_engine):
        checker = DataQualityChecker(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone='UTC',
        )
        source_ref = DataReference('test_ch_chunking_yearly', 'test')
        target_ref = DataReference('test_ch_chunking_yearly_target', 'test')

        result_start_total = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-07-01', None),
            tolerance_pct=0.0,
        )
        result_end_total = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=(None, '2024-06-30'),
            tolerance_pct=0.0,
        )
        result_start_daily = checker.check_counts_group_by_date(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-07-01', None),
            tolerance_pct=0.0,
        )
        result_end_daily = checker.check_counts_group_by_date(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=(None, '2024-06-30'),
            tolerance_pct=0.0,
        )
        result_start_samples = checker.check_samples(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-07-01', None),
            tolerance_pct=0.0,
        )
        result_end_samples = checker.check_samples(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=(None, '2024-06-30'),
            tolerance_pct=0.0,
        )

        assert result_start_total.status == CHECK_SUCCESS
        assert result_end_total.status == CHECK_SUCCESS
        assert result_start_daily.status == CHECK_SUCCESS
        assert result_end_daily.status == CHECK_SUCCESS
        assert result_start_samples.status == CHECK_FAILED
        assert result_end_samples.status == CHECK_FAILED
        assert result_start_total.stats.total_source_rows == 183
        assert result_end_total.stats.total_source_rows == 182
        assert result_start_daily.stats.total_source_rows == 183
        assert result_end_daily.stats.total_source_rows == 182
        assert result_start_samples.stats.total_source_rows == 183
        assert result_end_samples.stats.total_source_rows == 182
        assert 'chunks processed' not in result_start_total.report
        assert 'chunks processed' not in result_end_total.report
        start_mismatch = result_start_samples.details.issue_breakdown.set_index(
            'column_name'
        )
        end_mismatch = result_end_samples.details.issue_breakdown.set_index(
            'column_name'
        )
        assert int(start_mismatch.loc['name', 'issue_count']) == 1
        assert int(end_mismatch.loc['name', 'issue_count']) == 2
