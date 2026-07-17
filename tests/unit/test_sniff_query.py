import pandas as pd
import pytest

from xoverrr.constants import (
    COMPARISON_FAILED,
    COMPARISON_SUCCESS,
    COMPARISON_TYPE_SNIFF_QUERY,
    FLAG_VALUE_NO,
    FLAG_VALUE_YES,
    XSNIFF_PASSED_COLUMN,
)
from xoverrr.utils import (
    build_sniff_issue_stats,
    evaluate_sniff_query_data,
    resolve_sniff_query_passed_column,
    sniff_mismatched_row_count,
)


class TestSniffQueryUtils:
    def test_evaluate_sniff_query_row_level(self):
        df = pd.DataFrame(
            {
                'id': [1, 2, 3, 4],
                XSNIFF_PASSED_COLUMN: [
                    FLAG_VALUE_YES,
                    FLAG_VALUE_YES,
                    FLAG_VALUE_NO,
                    FLAG_VALUE_YES,
                ],
            }
        )

        stats, details = evaluate_sniff_query_data(df, max_examples=2)

        assert stats.total_source_rows == 4
        assert stats.total_matched_rows == 3
        assert stats.only_source_rows == 0
        assert stats.total_diff_percentage_rows == pytest.approx(25.0)
        assert stats.final_diff_score == pytest.approx(25.0)
        assert sniff_mismatched_row_count(stats) == 1
        assert len(details.discrepant_data_examples) == 1

    def test_evaluate_sniff_query_pass_fail_pass(self):
        df = pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_YES]})

        stats, details = evaluate_sniff_query_data(df)

        assert stats.total_source_rows == 1
        assert stats.total_matched_rows == 1
        assert stats.total_diff_percentage_rows == pytest.approx(0.0)

    def test_evaluate_sniff_query_pass_fail_fail(self):
        df = pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_NO]})

        stats, details = evaluate_sniff_query_data(df)

        assert stats.total_source_rows == 1
        assert stats.total_matched_rows == 0
        assert stats.total_diff_percentage_rows == pytest.approx(100.0)
        assert stats.final_diff_score == pytest.approx(100.0)

    def test_resolve_sniff_query_passed_column_row_level(self):
        assert (
            resolve_sniff_query_passed_column(['id', XSNIFF_PASSED_COLUMN])
            == XSNIFF_PASSED_COLUMN
        )

    def test_resolve_sniff_query_passed_column_scalar(self):
        assert (
            resolve_sniff_query_passed_column([XSNIFF_PASSED_COLUMN])
            == XSNIFF_PASSED_COLUMN
        )

    def test_resolve_sniff_query_passed_column_rejects_unknown_shape(self):
        with pytest.raises(ValueError, match=XSNIFF_PASSED_COLUMN):
            resolve_sniff_query_passed_column(['id', 'name'])

    def test_build_sniff_issue_stats_empty(self):
        stats = build_sniff_issue_stats(0, 0, 0)

        assert stats.total_source_rows == 0
        assert stats.final_score == 100.0
        assert stats.final_diff_score == 0.0


class TestSniffQuery:
    def _build_comparator(self, monkeypatch, source_df, metadata):
        from xoverrr.core import DataQualityComparator

        class DummyAdapter:
            def convert_types(self, df, metadata, timezone):
                return df

        comparator = DataQualityComparator.__new__(DataQualityComparator)
        comparator.source_engine = object()
        comparator.target_engine = None
        comparator.source_db_type = type('DB', (), {'name': 'POSTGRESQL'})()
        comparator.target_db_type = None
        comparator.timezone = 'UTC'
        comparator.comparison_stats = {
            'compared': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'tables_success': set(),
            'tables_failed': set(),
            'tables_skipped': set(),
            'start_time': '2025-01-01 00:00:00',
            'end_time': None,
        }
        comparator.result_persister = type(
            'Persister', (), {'persist': lambda *args, **kwargs: None}
        )()
        comparator._report_context = {
            'library_version': '1.2.5',
            'source_db_type': 'postgresql',
            'target_db_type': None,
        }
        comparator._finalize_calls = []

        monkeypatch.setattr(
            comparator,
            '_get_metadata_cols_for_custom_query',
            lambda query, engine: metadata,
        )
        monkeypatch.setattr(
            comparator,
            '_execute_query',
            lambda query, engine, timezone=None, query_side=None: source_df.copy(),
        )
        monkeypatch.setattr(
            comparator,
            '_get_adapter',
            lambda db_type: DummyAdapter(),
        )
        monkeypatch.setattr(
            comparator,
            '_start_comparison_run',
            lambda comparison_type, comparison_name: ('run123', '2025-01-01 00:00:00'),
        )

        def _capture_finalize(**kwargs):
            comparator._finalize_calls.append(kwargs)
            return kwargs['report']

        monkeypatch.setattr(comparator, '_finalize_comparison', _capture_finalize)
        monkeypatch.setattr(comparator, '_update_stats', lambda status, table: None)
        comparator._run_timings = type(
            'Timings',
            (),
            {
                'mark_dataset_compare_start': lambda self: None,
                'mark_dataset_compare_end': lambda self: None,
            },
        )()
        return comparator

    def test_sniff_query_row_level(self, monkeypatch):
        source_df = pd.DataFrame(
            {
                'order_id': [1, 2, 3],
                XSNIFF_PASSED_COLUMN: [FLAG_VALUE_YES, FLAG_VALUE_YES, FLAG_VALUE_NO],
            }
        )
        metadata = pd.DataFrame(
            {'column_name': ['order_id', XSNIFF_PASSED_COLUMN]}
        )
        comparator = self._build_comparator(monkeypatch, source_df, metadata)

        status, report, stats, details = comparator.sniff_query(
            source_query='SELECT order_id, xsniff_passed FROM orders',
            tolerance_percentage=50.0,
        )

        assert status == COMPARISON_SUCCESS
        assert sniff_mismatched_row_count(stats) == 1
        assert stats.total_diff_percentage_rows == pytest.approx(100 / 3)
        assert 'SNIFF QUERY REPORT' in report
        assert comparator._finalize_calls[-1]['comparison_type'] == COMPARISON_TYPE_SNIFF_QUERY

    def test_sniff_query_pass_fail_scalar(self, monkeypatch):
        source_df = pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_YES]})
        metadata = pd.DataFrame({'column_name': [XSNIFF_PASSED_COLUMN]})
        comparator = self._build_comparator(monkeypatch, source_df, metadata)

        status, report, stats, details = comparator.sniff_query(
            source_query="SELECT 'y' AS xsniff_passed",
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_score == pytest.approx(100.0)

    def test_sniff_query_failure(self, monkeypatch):
        source_df = pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_NO]})
        metadata = pd.DataFrame({'column_name': [XSNIFF_PASSED_COLUMN]})
        comparator = self._build_comparator(monkeypatch, source_df, metadata)

        status, report, stats, details = comparator.sniff_query(
            source_query="SELECT 'n' AS xsniff_passed",
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_FAILED
        assert stats.final_score == pytest.approx(0.0)

    def test_compare_methods_require_target_engine(self):
        from xoverrr.core import DataQualityComparator

        comparator = DataQualityComparator.__new__(DataQualityComparator)
        comparator.target_engine = None

        with pytest.raises(ValueError, match='target_engine is required'):
            comparator._require_target_engine()

    def test_compare_custom_query_requires_pk(self, monkeypatch):
        from xoverrr.core import DataQualityComparator

        comparator = DataQualityComparator.__new__(DataQualityComparator)
        comparator.source_engine = object()
        comparator.target_engine = object()
        comparator.timezone = 'UTC'
        comparator.comparison_stats = {'compared': 0}
        comparator.result_persister = type(
            'Persister', (), {'persist': lambda *args, **kwargs: None}
        )()

        monkeypatch.setattr(
            comparator,
            '_start_comparison_run',
            lambda comparison_type, comparison_name: ('run123', '2025-01-01 00:00:00'),
        )

        with pytest.raises(ValueError, match='custom_primary_key'):
            comparator.compare_custom_query(
                source_query='SELECT id FROM source_table',
                source_params={},
                target_query='SELECT id FROM target_table',
                target_params={},
                custom_primary_key=[],
            )
