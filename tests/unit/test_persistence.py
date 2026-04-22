import json

import pandas as pd
import pytest
from sqlalchemy import create_engine

from xoverrr.core import DataQualityComparator
from xoverrr.models import DataReference
from xoverrr.persistence import ComparisonResultPersister
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


def test_persist_outputs_returns_json_report():
    comparator = DataQualityComparator.__new__(DataQualityComparator)
    comparator.timezone = 'UTC'
    comparator.results_engine = None
    comparator.results_table = 'dq_results'
    comparator.result_persister = ComparisonResultPersister()

    report = comparator._persist_outputs(
        status='success',
        report='FULL TEXT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        comparison_type='sample',
        source_table='public.source_table',
        target_table='public.target_table',
        report_output_format='json',
    )

    payload = json.loads(report)
    assert payload['comparison_type'] == 'sample'
    assert payload['status'] == 'success'
    assert payload['report'] == 'FULL TEXT REPORT'
    assert payload['stats']['final_score'] == 100.0


def test_persist_outputs_returns_text_report():
    comparator = DataQualityComparator.__new__(DataQualityComparator)
    comparator.timezone = 'UTC'
    comparator.results_engine = None
    comparator.results_table = 'dq_results'
    comparator.result_persister = ComparisonResultPersister()

    report = comparator._persist_outputs(
        status='success',
        report='FULL TEXT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        comparison_type='sample',
        source_table='public.source_table',
        target_table='public.target_table',
        report_output_format='text',
    )

    assert report == 'FULL TEXT REPORT'


def test_persist_outputs_writes_to_results_engine():
    comparator = DataQualityComparator.__new__(DataQualityComparator)
    comparator.timezone = 'UTC'
    comparator.results_engine = create_engine('sqlite:///:memory:')
    comparator.results_table = 'dq_results'
    comparator.result_persister = ComparisonResultPersister(
        results_engine=comparator.results_engine,
        results_table=comparator.results_table,
    )

    comparator._persist_outputs(
        status='failed',
        report='COUNT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        comparison_type='count',
        source_table='public.a',
        target_table='public.b',
        persist_result=True,
    )

    stored = pd.read_sql('select * from dq_results', comparator.results_engine)
    assert len(stored) == 1
    assert stored.iloc[0]['comparison_type'] == 'count'
    assert stored.iloc[0]['status'] == 'failed'
    assert stored.iloc[0]['final_data_quality_score'] == 100.0
    assert stored.iloc[0]['report'] == 'COUNT REPORT'
    assert stored.iloc[0]['stats_total_source_rows'] == 10
    assert stored.iloc[0]['stats_total_target_rows'] == 10
    assert stored.iloc[0]['stats_final_score'] == 100.0
    assert stored.iloc[0]['details_common_attribute_columns_json'] == json.dumps(
        ['id', 'name'], ensure_ascii=False
    )

    payload = json.loads(stored.iloc[0]['payload_json'])
    assert payload['report'] == 'COUNT REPORT'
    assert payload['source_table'] == 'public.a'


def test_validate_report_output_options_rejects_unknown_format():
    comparator = DataQualityComparator.__new__(DataQualityComparator)
    with pytest.raises(ValueError, match='report_output_format'):
        comparator._validate_report_output_options(
            report_output_format='xml',
        )


def test_persist_outputs_with_datareference_target_and_tags():
    comparator = DataQualityComparator.__new__(DataQualityComparator)
    comparator.timezone = 'UTC'
    comparator.results_engine = create_engine('sqlite:///:memory:')
    comparator.results_table = 'dq_results_default'
    comparator.result_persister = ComparisonResultPersister(
        results_engine=comparator.results_engine,
        results_table=comparator.results_table,
    )

    comparator._persist_outputs(
        status='success',
        report='TAGGED REPORT',
        stats=_build_stats(),
        details=_build_details(),
        comparison_type='sample',
        comparison_name='orders_daily_compare',
        comparison_tags={'env': 'dev', 'domain': 'orders'},
        source_table='public.orders_src',
        target_table='public.orders_trg',
        persist_result=True,
        persist_result_ref=DataReference('dq_results_custom'),
    )

    stored = pd.read_sql(
        'select * from dq_results_custom', comparator.results_engine
    )
    assert len(stored) == 1
    assert stored.iloc[0]['comparison_name'] == 'orders_daily_compare'
    assert json.loads(stored.iloc[0]['comparison_tags_json']) == {
        'env': 'dev',
        'domain': 'orders',
    }
