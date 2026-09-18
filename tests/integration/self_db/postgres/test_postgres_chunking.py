"""
Integration tests for chunked check mode.
"""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


class TestPostgresChunkedCheck:
    @pytest.fixture(autouse=True)
    def setup_chunked_data(self, postgres_engine, table_helper):
        source_table = 'test_chunked_source'
        target_table = 'test_chunked_target'

        table_helper.create_table(
            engine=postgres_engine,
            table_name=source_table,
            create_sql=f"""
                CREATE TABLE {source_table} (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {source_table} (id, name, created_at, updated_at) VALUES
                (1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
                (2, 'Bob',     '2024-01-02', '2024-01-02 10:00:00'),
                (3, 'Charlie', '2024-01-03', '2024-01-03 10:00:00'),
                (4, 'Diana',   '2024-01-04', '2024-01-04 10:00:00')
            """,
        )

        table_helper.create_table(
            engine=postgres_engine,
            table_name=target_table,
            create_sql=f"""
                CREATE TABLE {target_table} (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {target_table} (id, name, created_at, updated_at) VALUES
                (1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
                (2, 'Bobby',   '2024-01-02', '2024-01-02 10:00:00'),
                (3, 'Charlie', '2024-01-03', '2024-01-03 10:00:00'),
                (4, 'Dina',    '2024-01-04', '2024-01-04 10:00:00')
            """,
        )

        yield

    def test_chunked_counts_matches_non_chunked(self, postgres_engine):
        checker = DataQualityChecker(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )

        source_ref = DataReference('test_chunked_source', 'test')
        target_ref = DataReference('test_chunked_target', 'test')

        result = checker.check_counts_group_by_date(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_pct=0.0,
        )
        status_non_chunked = result.status
        stats_non_chunked = result.stats
        result = checker.check_counts_group_by_date(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            chunk_size_days=2,
            tolerance_pct=0.0,
        )
        status_chunked = result.status
        stats_chunked = result.stats

        assert status_non_chunked == CHECK_SUCCESS
        assert status_chunked == CHECK_SUCCESS
        assert stats_non_chunked.final_diff_score == 0.0
        assert stats_chunked.final_diff_score == 0.0
        assert stats_chunked.total_source_rows == stats_non_chunked.total_source_rows
        assert stats_chunked.total_target_rows == stats_non_chunked.total_target_rows

    def test_chunked_sample_aggregates_differences_across_chunks(self, postgres_engine):
        checker = DataQualityChecker(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )

        source_ref = DataReference('test_chunked_source', 'test')
        target_ref = DataReference('test_chunked_target', 'test')

        result = checker.check_samples(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_pct=0.0,
        )
        status_non_chunked = result.status
        stats_non_chunked = result.stats
        details_non_chunked = result.details

        result = checker.check_samples(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-01-04'),
            chunk_size_days=1,
            tolerance_pct=0.0,
        )
        status_chunked = result.status
        stats_chunked = result.stats
        details_chunked = result.details

        assert status_non_chunked == CHECK_FAILED
        assert status_chunked == CHECK_FAILED
        assert stats_chunked.final_diff_score == stats_non_chunked.final_diff_score
        assert stats_chunked.passed_rows == stats_non_chunked.passed_rows

        non_chunked_mismatch = details_non_chunked.issue_breakdown.set_index(
            'column_name'
        )['issue_count']
        chunked_mismatch = details_chunked.issue_breakdown.set_index('column_name')[
            'issue_count'
        ]
        assert int(chunked_mismatch['name']) == int(non_chunked_mismatch['name']) == 2

    def test_chunked_total_counts_matches_non_chunked(self, postgres_engine):
        checker = DataQualityChecker(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )
        source_ref = DataReference('test_chunked_source', 'test')
        target_ref = DataReference('test_chunked_target', 'test')

        result_full = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_pct=0.0,
        )
        result_chunked = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            chunk_size_days=2,
            tolerance_pct=0.0,
        )
        result_partial = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-02'),
            tolerance_pct=0.0,
        )

        assert result_full.status == CHECK_SUCCESS
        assert result_chunked.status == CHECK_SUCCESS
        assert result_full.stats.total_source_rows == 4
        assert result_chunked.stats.total_source_rows == 4
        assert result_chunked.stats.total_target_rows == 4
        assert 'chunks processed (2 intervals)' in result_chunked.report
        assert result_partial.status == CHECK_SUCCESS
        assert result_partial.stats.total_source_rows == 2
        assert result_partial.stats.total_target_rows == 2
