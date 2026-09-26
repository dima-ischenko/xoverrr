"""ClickHouse total counts on 100k-row tables, chunked vs not."""

import pytest

from xoverrr.constants import CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


class TestClickHouseTotalCountsLarge:
    @pytest.fixture(autouse=True)
    def setup_large_count_data(self, clickhouse_engine, table_helper):
        source_table = 'test_ch_total_counts_100k'
        target_table = 'test_ch_total_counts_100k_target'

        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=source_table,
            create_sql=f"""
                CREATE TABLE {source_table} (
                    id          UInt32,
                    created_at  Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {source_table} (id, created_at)
                SELECT
                    number + 1,
                    toDate('2024-01-01') + toIntervalDay(number % 365)
                FROM numbers(100000)
            """,
        )
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=target_table,
            create_sql=f"""
                CREATE TABLE {target_table} (
                    id          UInt32,
                    created_at  Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {target_table} (id, created_at)
                SELECT id, created_at FROM {source_table}
            """,
        )
        yield

    def test_total_counts_chunked_matches_non_chunked(self, clickhouse_engine):
        checker = DataQualityChecker(
            source_engine=clickhouse_engine,
            target_engine=clickhouse_engine,
            timezone='UTC',
        )
        source_ref = DataReference('test_ch_total_counts_100k', 'test')
        target_ref = DataReference('test_ch_total_counts_100k_target', 'test')

        result_full = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
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
        assert result_full.stats.total_source_rows == 100000
        assert result_full.stats.total_target_rows == 100000
        assert result_chunked.stats.total_source_rows == result_full.stats.total_source_rows
        assert result_chunked.stats.total_target_rows == result_full.stats.total_target_rows
        assert result_chunked.stats.final_diff_score == result_full.stats.final_diff_score
        assert 'chunks processed' not in result_full.report
        assert 'chunks processed' in result_chunked.report
