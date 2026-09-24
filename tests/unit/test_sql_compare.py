import pandas as pd
import pytest

from xoverrr.sql_compare import (SECTION_DUP_SRC, SECTION_ISSUE_COL,
                                 SECTION_ISSUE_EX, SECTION_ONLY_SRC,
                                 SECTION_STATS, STAT_COMPARABLE, STAT_PASSED,
                                 STAT_TOTAL_SOURCE, STAT_TOTAL_TARGET,
                                 parse_compare_query_result, parse_pk_value)
from xoverrr.utils import build_check_stats


def test_parse_pk_value_single_and_compound():
    assert parse_pk_value('42', 1) == ('42',)
    assert parse_pk_value('a\x1fb', 2) == ('a', 'b')
    assert parse_pk_value(None, 1) is None


def test_parse_compare_query_result_empty():
    assert parse_compare_query_result(pd.DataFrame(), ['id'], ['name']) == (None, None)


def test_parse_compare_query_result_stats_and_issues():
    df = pd.DataFrame(
        [
            {
                'section': SECTION_STATS,
                'name': STAT_TOTAL_SOURCE,
                'pk_value': None,
                'source_value': None,
                'target_value': None,
                'xflg': None,
                'cnt': 4,
                'row_payload': None,
            },
            {
                'section': SECTION_STATS,
                'name': STAT_TOTAL_TARGET,
                'pk_value': None,
                'source_value': None,
                'target_value': None,
                'xflg': None,
                'cnt': 3,
                'row_payload': None,
            },
            {
                'section': SECTION_STATS,
                'name': STAT_COMPARABLE,
                'pk_value': None,
                'source_value': None,
                'target_value': None,
                'xflg': None,
                'cnt': 2,
                'row_payload': None,
            },
            {
                'section': SECTION_STATS,
                'name': STAT_PASSED,
                'pk_value': None,
                'source_value': None,
                'target_value': None,
                'xflg': None,
                'cnt': 1,
                'row_payload': None,
            },
            {
                'section': SECTION_ISSUE_COL,
                'name': 'name',
                'pk_value': None,
                'source_value': None,
                'target_value': None,
                'xflg': None,
                'cnt': 1,
                'row_payload': None,
            },
            {
                'section': SECTION_ONLY_SRC,
                'name': None,
                'pk_value': '9',
                'source_value': None,
                'target_value': None,
                'xflg': None,
                'cnt': 1,
                'row_payload': None,
            },
            {
                'section': SECTION_DUP_SRC,
                'name': None,
                'pk_value': '1',
                'source_value': None,
                'target_value': None,
                'xflg': None,
                'cnt': 1,
                'row_payload': None,
            },
            {
                'section': SECTION_ISSUE_EX,
                'name': 'name',
                'pk_value': '2',
                'source_value': 'Alice',
                'target_value': 'Alicia',
                'xflg': None,
                'cnt': 1,
                'row_payload': None,
            },
        ]
    )
    stats, details = parse_compare_query_result(df, ['id'], ['name'], max_examples=3)

    expected = build_check_stats(
        total_source_rows=4,
        total_target_rows=3,
        dup_source_rows=0,
        dup_target_rows=0,
        only_source_rows=0,
        only_target_rows=0,
        comparable_rows=2,
        passed_rows=1,
        issue_counts=[1],
    )
    assert stats.total_source_rows == expected.total_source_rows
    assert stats.comparable_rows == 2
    assert stats.passed_rows == 1
    assert stats.final_diff_score == pytest.approx(expected.final_diff_score)
    assert details.issue_breakdown.iloc[0]['column_name'] == 'name'
    assert details.issue_examples.iloc[0]['source_value'] == 'Alice'
    assert '9' in details.source_only_keys_examples
    assert '1' in details.dup_source_keys_examples


def test_parse_compare_query_result_wide_row():
    from xoverrr.sql_compare import (EX_DUP_SRC, EX_ISSUE_ROW_SRC,
                                     EX_ISSUE_ROW_TRG, EX_ONLY_SRC,
                                     FIELD_SEPARATOR, STAT_COMPARABLE,
                                     STAT_DUP_SOURCE, STAT_DUP_TARGET,
                                     STAT_ONLY_SOURCE, STAT_ONLY_TARGET,
                                     STAT_PASSED, STAT_TOTAL_SOURCE,
                                     STAT_TOTAL_TARGET)

    field = FIELD_SEPARATOR
    df = pd.DataFrame(
        [
            {
                STAT_TOTAL_SOURCE: 4,
                STAT_TOTAL_TARGET: 3,
                STAT_DUP_SOURCE: 0,
                STAT_DUP_TARGET: 0,
                STAT_ONLY_SOURCE: 1,
                STAT_ONLY_TARGET: 0,
                STAT_COMPARABLE: 2,
                STAT_PASSED: 1,
                'i_name': 1,
                EX_DUP_SRC: '1',
                'ex_dup_trg': None,
                EX_ONLY_SRC: '9',
                'ex_only_trg': None,
                'ex_issue_name': f'2{field}Alice{field}Alicia',
                EX_ISSUE_ROW_SRC: f'2{field}id=2;name=Alice;',
                EX_ISSUE_ROW_TRG: f'2{field}id=2;name=Alicia;',
            }
        ]
    )
    stats, details = parse_compare_query_result(df, ['id'], ['name'], max_examples=3)
    assert stats.total_source_rows == 4
    assert stats.comparable_rows == 2
    assert stats.passed_rows == 1
    assert details.issue_breakdown.iloc[0]['column_name'] == 'name'
    assert details.issue_examples.iloc[0]['source_value'] == 'Alice'
    assert '9' in details.source_only_keys_examples
    assert '1' in details.dup_source_keys_examples
    assert details.issue_row_examples.iloc[0]['xflg'] == 'src'
