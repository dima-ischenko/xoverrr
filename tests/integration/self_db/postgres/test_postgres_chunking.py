"""
Integration tests for chunked comparison mode.
"""

import pytest

from xoverrr.constants import COMPARISON_FAILED, COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestPostgresChunkedComparison:
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
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )

        source_ref = DataReference('test_chunked_source', 'test')
        target_ref = DataReference('test_chunked_target', 'test')

        status_non_chunked, _, stats_non_chunked, _ = comparator.compare_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_percentage=0.0,
        )
        status_chunked, _, stats_chunked, _ = comparator.compare_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            chunk_size_days=2,
            tolerance_percentage=0.0,
        )

        assert status_non_chunked == COMPARISON_SUCCESS
        assert status_chunked == COMPARISON_SUCCESS
        assert stats_non_chunked.final_diff_score == 0.0
        assert stats_chunked.final_diff_score == 0.0
        assert stats_chunked.total_source_rows == stats_non_chunked.total_source_rows
        assert stats_chunked.total_target_rows == stats_non_chunked.total_target_rows

    def test_chunked_sample_aggregates_differences_across_chunks(self, postgres_engine):
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )

        source_ref = DataReference('test_chunked_source', 'test')
        target_ref = DataReference('test_chunked_target', 'test')

        status_non_chunked, _, stats_non_chunked, details_non_chunked = (
            comparator.compare_sample(
                source_table=source_ref,
                target_table=target_ref,
                date_column='created_at',
                update_column='updated_at',
                date_range=('2024-01-01', '2024-01-04'),
                tolerance_percentage=0.0,
            )
        )

        status_chunked, _, stats_chunked, details_chunked = comparator.compare_sample(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=('2024-01-01', '2024-01-04'),
            chunk_size_days=1,
            tolerance_percentage=0.0,
        )

        assert status_non_chunked == COMPARISON_FAILED
        assert status_chunked == COMPARISON_FAILED
        assert stats_chunked.final_diff_score == stats_non_chunked.final_diff_score
        assert stats_chunked.total_matched_rows == stats_non_chunked.total_matched_rows

        non_chunked_mismatch = details_non_chunked.mismatches_per_column.set_index(
            'column_name'
        )['mismatch_count']
        chunked_mismatch = details_chunked.mismatches_per_column.set_index('column_name')[
            'mismatch_count'
        ]
        assert int(chunked_mismatch['name']) == int(non_chunked_mismatch['name']) == 2
