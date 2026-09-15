import pandas as pd
import pytest

from xoverrr.constants import FLAG_VALUE_NO, FLAG_VALUE_YES, XSNIFF_PASSED_COLUMN
from xoverrr.core import DataQualityChecker
from xoverrr.utils import (
    build_sniff_issue_stats,
    evaluate_check_sniff_query_data,
    resolve_check_sniff_query_passed_column,
    sniff_issue_row_count,
)


def test_evaluate_sniff_query_row_level():
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
    assert stats.issue_rows_pct == pytest.approx(25.0)
    assert sniff_issue_row_count(stats) == 1
    assert len(details.issue_row_examples) == 1


def test_evaluate_sniff_query_pass():
    stats, _ = evaluate_check_sniff_query_data(
        pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_YES]})
    )

    assert stats.passed_rows == 1
    assert stats.issue_rows_pct == pytest.approx(0.0)


def test_evaluate_sniff_query_fail():
    stats, _ = evaluate_check_sniff_query_data(
        pd.DataFrame({XSNIFF_PASSED_COLUMN: [FLAG_VALUE_NO]})
    )

    assert stats.passed_rows == 0
    assert stats.final_diff_score == pytest.approx(100.0)


def test_resolve_sniff_passed_column():
    assert (
        resolve_check_sniff_query_passed_column(['id', XSNIFF_PASSED_COLUMN])
        == XSNIFF_PASSED_COLUMN
    )
    assert (
        resolve_check_sniff_query_passed_column([XSNIFF_PASSED_COLUMN])
        == XSNIFF_PASSED_COLUMN
    )
    with pytest.raises(ValueError, match=XSNIFF_PASSED_COLUMN):
        resolve_check_sniff_query_passed_column(['id', 'name'])


def test_build_sniff_issue_stats_empty():
    stats = build_sniff_issue_stats(0, 0, 0)

    assert stats.total_source_rows == 0
    assert stats.final_score == 100.0
    assert stats.final_diff_score == 0.0


def test_check_methods_require_target_engine():
    checker = DataQualityChecker.__new__(DataQualityChecker)
    checker.target_engine = None

    with pytest.raises(ValueError, match='target_engine is required'):
        checker._require_target_engine()


def test_check_custom_queries_requires_pk():
    checker = DataQualityChecker.__new__(DataQualityChecker)
    checker.source_engine = object()
    checker.target_engine = object()
    checker.timezone = 'UTC'

    with pytest.raises(ValueError, match='custom_primary_key'):
        checker.check_custom_queries(
            source_query='SELECT id FROM source_table',
            source_params={},
            target_query='SELECT id FROM target_table',
            target_params={},
            custom_primary_key=[],
        )
