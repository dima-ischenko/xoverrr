import pandas as pd
import pytest

from xoverrr.constants import (
    CHECK_FAILED,
    CHECK_SUCCESS,
    CHECK_TYPE_SNIFF_QUERY,
    FLAG_VALUE_NO,
    FLAG_VALUE_YES,
    XSNIFF_PASSED_COLUMN,
)
from xoverrr.utils import (
    build_sniff_issue_stats,
    evaluate_check_sniff_query_data,
    resolve_check_sniff_query_passed_column,
    sniff_issue_row_count,
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

        stats, details = evaluate_check_sniff_query_data(df, max_examples=2)

        assert stats.total_source_rows == 4
        assert stats.passed_rows == 3
        assert stats.only_source_rows == 0
        assert stats.issue_rows_pct == pytest.approx(25.0)
        assert stats.final_diff_score == pytest.approx(25.0)
        assert sniff_issue_row_count(stats) == 1
        assert len(details.issue_row_examples) == 1

    def test_evaluate_sniff_query_pass_fail_pass(self):
        df = pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_YES]})

        stats, details = evaluate_check_sniff_query_data(df)

        assert stats.total_source_rows == 1
        assert stats.passed_rows == 1
        assert stats.issue_rows_pct == pytest.approx(0.0)

    def test_evaluate_sniff_query_pass_fail_fail(self):
        df = pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_NO]})

        stats, details = evaluate_check_sniff_query_data(df)

        assert stats.total_source_rows == 1
        assert stats.passed_rows == 0
        assert stats.issue_rows_pct == pytest.approx(100.0)
        assert stats.final_diff_score == pytest.approx(100.0)

    def test_resolve_check_sniff_query_passed_column_row_level(self):
        assert (
            resolve_check_sniff_query_passed_column(['id', XSNIFF_PASSED_COLUMN])
            == XSNIFF_PASSED_COLUMN
        )

    def test_resolve_check_sniff_query_passed_column_scalar(self):
        assert (
            resolve_check_sniff_query_passed_column([XSNIFF_PASSED_COLUMN])
            == XSNIFF_PASSED_COLUMN
        )

    def test_resolve_check_sniff_query_passed_column_rejects_unknown_shape(self):
        with pytest.raises(ValueError, match=XSNIFF_PASSED_COLUMN):
            resolve_check_sniff_query_passed_column(['id', 'name'])

    def test_build_sniff_issue_stats_empty(self):
        stats = build_sniff_issue_stats(0, 0, 0)

        assert stats.total_source_rows == 0
        assert stats.final_score == 100.0
        assert stats.final_diff_score == 0.0


class TestSniffQuery:
    def _build_checker(self, monkeypatch, source_df, metadata):
        from xoverrr.core import DataQualityChecker

        class DummyAdapter:
            def convert_types(self, df, metadata, timezone):
                return df

        checker = DataQualityChecker.__new__(DataQualityChecker)
        checker.source_engine = object()
        checker.target_engine = None
        checker.source_db_type = type('DB', (), {'name': 'POSTGRESQL'})()
        checker.target_db_type = None
        checker.timezone = 'UTC'
        checker.check_stats = {
            'checked': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'tables_success': set(),
            'tables_failed': set(),
            'tables_skipped': set(),
            'start_time': '2025-01-01 00:00:00',
            'end_time': None,
        }
        checker.result_persister = type(
            'Persister', (), {'persist': lambda *args, **kwargs: None}
        )()
        checker._report_context = {
            'library_version': '1.2.5',
            'source_db_type': 'postgresql',
            'target_db_type': None,
        }
        checker._finalize_calls = []

        monkeypatch.setattr(
            checker,
            '_get_metadata_cols_for_custom_query',
            lambda query, engine: metadata,
        )
        monkeypatch.setattr(
            checker,
            '_execute_query',
            lambda query, engine, timezone=None, query_side=None: source_df.copy(),
        )
        monkeypatch.setattr(
            checker,
            '_get_adapter',
            lambda db_type: DummyAdapter(),
        )
        monkeypatch.setattr(
            checker,
            '_start_check_run',
            lambda check_type, check_name: ('run123', '2025-01-01 00:00:00'),
        )

        def _capture_finalize(**kwargs):
            checker._finalize_calls.append(kwargs)
            return kwargs['report']

        monkeypatch.setattr(checker, '_finalize_check', _capture_finalize)
        monkeypatch.setattr(checker, '_update_stats', lambda status, table: None)
        checker._run_timings = type(
            'Timings',
            (),
            {
                'mark_dataset_check_start': lambda self: None,
                'mark_dataset_check_end': lambda self: None,
            },
        )()
        return checker

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
        checker = self._build_checker(monkeypatch, source_df, metadata)

        status, report, stats, details = checker.check_sniff_query(
            source_query='SELECT order_id, xsniff_passed FROM orders',
            tolerance_pct=50.0,
        )

        assert status == CHECK_SUCCESS
        assert sniff_issue_row_count(stats) == 1
        assert stats.issue_rows_pct == pytest.approx(100 / 3)
        assert 'SNIFF QUERY CHECK REPORT' in report
        assert checker._finalize_calls[-1]['check_type'] == CHECK_TYPE_SNIFF_QUERY

    def test_sniff_query_pass_fail_scalar(self, monkeypatch):
        source_df = pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_YES]})
        metadata = pd.DataFrame({'column_name': [XSNIFF_PASSED_COLUMN]})
        checker = self._build_checker(monkeypatch, source_df, metadata)

        status, report, stats, details = checker.check_sniff_query(
            source_query="SELECT 'y' AS xsniff_passed",
            tolerance_pct=0.0,
        )

        assert status == CHECK_SUCCESS
        assert stats.final_score == pytest.approx(100.0)

    def test_sniff_query_failure(self, monkeypatch):
        source_df = pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_NO]})
        metadata = pd.DataFrame({'column_name': [XSNIFF_PASSED_COLUMN]})
        checker = self._build_checker(monkeypatch, source_df, metadata)

        status, report, stats, details = checker.check_sniff_query(
            source_query="SELECT 'n' AS xsniff_passed",
            tolerance_pct=0.0,
        )

        assert status == CHECK_FAILED
        assert stats.final_score == pytest.approx(0.0)
        assert 'ISSUE ROW EXAMPLES' in report
        assert not details.issue_row_examples.empty

    def test_check_methods_require_target_engine(self):
        from xoverrr.core import DataQualityChecker

        checker = DataQualityChecker.__new__(DataQualityChecker)
        checker.target_engine = None

        with pytest.raises(ValueError, match='target_engine is required'):
            checker._require_target_engine()

    def test_check_custom_queries_requires_pk(self, monkeypatch):
        from xoverrr.core import DataQualityChecker

        checker = DataQualityChecker.__new__(DataQualityChecker)
        checker.source_engine = object()
        checker.target_engine = object()
        checker.timezone = 'UTC'
        checker.check_stats = {'checked': 0}
        checker.result_persister = type(
            'Persister', (), {'persist': lambda *args, **kwargs: None}
        )()

        monkeypatch.setattr(
            checker,
            '_start_check_run',
            lambda check_type, check_name: ('run123', '2025-01-01 00:00:00'),
        )

        with pytest.raises(ValueError, match='custom_primary_key'):
            checker.check_custom_queries(
                source_query='SELECT id FROM source_table',
                source_params={},
                target_query='SELECT id FROM target_table',
                target_params={},
                custom_primary_key=[],
            )
