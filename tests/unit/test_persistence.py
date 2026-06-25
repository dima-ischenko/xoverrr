import json

import pandas as pd
import pytest
from sqlalchemy import create_engine

from xoverrr import constants as ct
from xoverrr.models import DataReference
from xoverrr.persistence import ComparisonResultPersister, parse_persist_result_option
from xoverrr.reporting import (
    build_comparison_result,
    format_comparison_result,
    validate_report_output_format,
)
from xoverrr.utils import ComparisonDiffDetails, ComparisonStats


def _build_stats() -> ComparisonStats:
    return ComparisonStats(
        total_source_rows=10,
        total_target_rows=10,
        dup_source_rows=0,
        dup_target_rows=0,
        only_source_rows=0,
        only_target_rows=0,
        common_pk_rows=10,
        total_matched_rows=10,
        dup_source_percentage_rows=0.0,
        dup_target_percentage_rows=0.0,
        source_only_percentage_rows=0.0,
        target_only_percentage_rows=0.0,
        total_diff_percentage_rows=0.0,
        max_diff_percentage_cols=0.0,
        median_diff_percentage_cols=0.0,
        final_diff_score=0.0,
        final_score=100.0,
    )


def _build_details() -> ComparisonDiffDetails:
    return ComparisonDiffDetails(
        mismatches_per_column=pd.DataFrame(columns=['column_name', 'mismatch_count']),
        discrepancies_per_col_examples=pd.DataFrame(),
        dup_source_keys_examples=tuple(),
        dup_target_keys_examples=tuple(),
        source_only_keys_examples=tuple(),
        target_only_keys_examples=tuple(),
        discrepant_data_examples=pd.DataFrame(),
        common_attribute_columns=['id', 'name'],
        skipped_source_columns=[],
        skipped_target_columns=[],
    )


def test_format_comparison_result_returns_json_report():
    result = build_comparison_result(
        timezone='UTC',
        status='success',
        report='FULL TEXT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        comparison_type=ct.COMPARISON_TYPE_SAMPLE,
        source_table='public.source_table',
        target_table='public.target_table',
    )

    report = format_comparison_result(
        result, report_output_format=ct.REPORT_OUTPUT_FORMAT_JSON
    )

    payload = json.loads(report)
    assert payload['comparison_type'] == ct.COMPARISON_TYPE_SAMPLE
    assert payload['status'] == 'success'
    assert payload['report'] == 'FULL TEXT REPORT'
    assert payload['stats']['final_score'] == 100.0


def test_format_comparison_result_returns_text_report():
    result = build_comparison_result(
        timezone='UTC',
        status='success',
        report='FULL TEXT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        comparison_type=ct.COMPARISON_TYPE_SAMPLE,
        source_table='public.source_table',
        target_table='public.target_table',
    )

    report = format_comparison_result(
        result, report_output_format=ct.REPORT_OUTPUT_FORMAT_TEXT
    )

    assert report == 'FULL TEXT REPORT'


def test_persist_writes_to_results_engine():
    results_engine = create_engine('sqlite:///:memory:')
    persister = ComparisonResultPersister(
        results_engine=results_engine,
        results_table='dq_results',
    )
    result = build_comparison_result(
        timezone='UTC',
        status='failed',
        report='COUNT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        comparison_type=ct.COMPARISON_TYPE_COUNT,
        source_table='public.a',
        target_table='public.b',
    )

    persister.persist(result, persist_result=True)

    stored = pd.read_sql('select * from dq_results', results_engine)
    assert len(stored) == 1
    assert stored.iloc[0]['comparison_type'] == ct.COMPARISON_TYPE_COUNT
    assert stored.iloc[0]['status'] == 'failed'
    assert stored.iloc[0]['report'] == 'COUNT REPORT'
    assert stored.iloc[0]['stats_total_source_rows'] == 10
    assert stored.iloc[0]['stats_total_target_rows'] == 10
    assert stored.iloc[0]['stats_final_score'] == 100.0
    assert stored.iloc[0]['details_common_attribute_columns_json'] == json.dumps(
        ['id', 'name'], ensure_ascii=False
    )
    assert stored.iloc[0]['source_table'] == 'public.a'
    assert 'payload_json' not in stored.columns
    assert stored.iloc[0]['details_dup_source_keys_examples_json'] == '[]'


def test_validate_report_output_format_rejects_unknown_format():
    with pytest.raises(ValueError, match='report_output_format'):
        validate_report_output_format('xml')


def test_parse_persist_result_option():
    table_ref = DataReference('custom_results')

    enabled_only = parse_persist_result_option(True)
    assert enabled_only.enabled is True
    assert enabled_only.table_ref is None

    disabled = parse_persist_result_option(False)
    assert disabled.enabled is False
    assert disabled.table_ref is None

    custom = parse_persist_result_option(table_ref)
    assert custom.enabled is True
    assert custom.table_ref is table_ref


def test_persist_with_datareference_target_and_tags():
    results_engine = create_engine('sqlite:///:memory:')
    persister = ComparisonResultPersister(
        results_engine=results_engine,
        results_table='dq_results_default',
    )
    result = build_comparison_result(
        timezone='UTC',
        status='success',
        report='TAGGED REPORT',
        stats=_build_stats(),
        details=_build_details(),
        comparison_type=ct.COMPARISON_TYPE_SAMPLE,
        comparison_name='orders_daily_compare',
        comparison_tags={'env': 'dev', 'domain': 'orders'},
        source_table='public.orders_src',
        target_table='public.orders_trg',
    )

    persister.persist(
        result,
        persist_result=True,
        persist_result_ref=DataReference('dq_results_custom'),
    )

    stored = pd.read_sql(
        'select * from dq_results_custom', results_engine
    )
    assert len(stored) == 1
    assert stored.iloc[0]['comparison_name'] == 'orders_daily_compare'
    assert json.loads(stored.iloc[0]['comparison_tags_json']) == {
        'env': 'dev',
        'domain': 'orders',
    }
