import pytest

from xoverrr import (
    DataQualityComparator,
    DataReference,
    COMPARISON_SUCCESS,
    COMPARISON_FAILED,
    COMPARISON_SKIPPED,
)


class TestOraclePostgresComparison:
    """
    Integration tests for Oracle → PostgreSQL comparison.

    The goal is to validate that:
    - counts comparison works correctly
    - sample comparison detects mismatches
    - empty datasets are handled gracefully
    """

    def test_compare_counts_success(self, oracle_engine, postgres_engine):
        """
        Count comparison should succeed when row counts match
        within zero tolerance.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference("orders", "test"),
            target_table=DataReference("orders", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats is not None
        assert stats.final_diff_score == 0.0


    def test_compare_sample_with_detected_difference(self, oracle_engine, postgres_engine):
        """
        Sample comparison should fail when at least one column
        has mismatched values.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("customers", "test"),
            target_table=DataReference("customers", "test"),
            date_column="created_at",
            update_column="updated_at",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_FAILED
        assert stats.final_diff_score > 0
        assert details.mismatches_per_column is not None


    def test_compare_sample_empty_tables_skipped(self, oracle_engine, postgres_engine):
        """
        Comparison should be skipped when both source and target
        tables are empty for the selected date range.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("empty_table", "test"),
            target_table=DataReference("empty_table", "test"),
            date_column="created_at",
            date_range=("1999-01-01", "1999-01-02"),
        )

        assert status == COMPARISON_SKIPPED
        assert report is None
