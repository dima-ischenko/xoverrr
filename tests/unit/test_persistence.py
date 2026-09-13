import json

import pandas as pd
import pytest
from sqlalchemy import create_engine

from xoverrr import constants as ct
from xoverrr.core import DataQualityChecker
from xoverrr.models import DataReference
from xoverrr.adapters.base import PERSIST_INSERTED_AT_COLUMN
from xoverrr.persistence import (
    CheckResultPersister,
    CheckRunTimings,
    build_run_id,
    normalize_persist_result,
    validate_run_id,
)
from xoverrr.reporting import (
    build_check_result,
    format_check_result,
    validate_report_output_format,
)
from xoverrr.utils import CheckDetails, CheckStats

RUN_STARTED_AT = '2026-01-01 00:00:00'
RUN_FINISHED_AT = '2026-01-01 00:00:05'
RUN_ID = 'internal-run-id'


def _build_timings() -> CheckRunTimings:
    return CheckRunTimings(
        run_started_at=RUN_STARTED_AT,
        run_finished_at=RUN_FINISHED_AT,
        source_query_started_at='2026-01-01 00:00:01',
        source_query_finished_at='2026-01-01 00:00:02',
        target_query_started_at='2026-01-01 00:00:02',
        target_query_finished_at='2026-01-01 00:00:03',
        dataset_check_started_at='2026-01-01 00:00:03',
        dataset_check_finished_at='2026-01-01 00:00:04',
    )


def _build_stats() -> CheckStats:
    return CheckStats(
        total_source_rows=10,
        total_target_rows=10,
        dup_source_rows=0,
        dup_target_rows=0,
        only_source_rows=0,
        only_target_rows=0,
        comparable_rows=10,
        passed_rows=10,
        dup_source_rows_pct=0.0,
        dup_target_rows_pct=0.0,
        source_only_rows_pct=0.0,
        target_only_rows_pct=0.0,
        issue_rows_pct=0.0,
        max_issue_pct=0.0,
        median_issue_pct=0.0,
        final_diff_score=0.0,
        final_score=100.0,
    )


def _build_details() -> CheckDetails:
    return CheckDetails(
        issue_breakdown=pd.DataFrame(columns=['column_name', 'issue_count']),
        issue_examples=pd.DataFrame(),
        dup_source_keys_examples=tuple(),
        dup_target_keys_examples=tuple(),
        source_only_keys_examples=tuple(),
        target_only_keys_examples=tuple(),
        issue_row_examples=pd.DataFrame(),
        evaluated_columns=['id', 'name'],
        skipped_source_columns=[],
        skipped_target_columns=[],
    )


def test_format_check_result_returns_json_report():
    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status='success',
        report='FULL TEXT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_SAMPLES,
        check_name='unit_test_compare',
        source_table='public.source_table',
        target_table='public.target_table',
    )

    report = format_check_result(
        result, report_output_format=ct.REPORT_OUTPUT_FORMAT_JSON
    )

    payload = json.loads(report)
    assert 'run_id' not in payload
    assert payload['check_type'] == ct.CHECK_TYPE_SAMPLES
    assert payload['status'] == 'success'
    assert payload['report'] == 'FULL TEXT REPORT'
    assert payload['stats']['final_score'] == 100.0


def test_format_check_result_returns_text_report():
    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status='success',
        report='FULL TEXT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_SAMPLES,
        source_table='public.source_table',
        target_table='public.target_table',
    )

    report = format_check_result(
        result, report_output_format=ct.REPORT_OUTPUT_FORMAT_TEXT
    )

    assert report == 'FULL TEXT REPORT'


def test_persist_writes_to_results_engine():
    results_engine = create_engine('sqlite:///:memory:')
    persister = CheckResultPersister(results_engine=results_engine)
    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status='failed',
        report='COUNT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_COUNTS,
        source_table='public.a',
        target_table='public.b',
    )

    persister.persist(result, DataReference('dq_results'))

    stored = pd.read_sql('select * from dq_results', results_engine)
    row = stored.iloc[0]
    assert len(stored) == 1
    assert row['run_id'] == RUN_ID
    assert row['check_type'] == ct.CHECK_TYPE_COUNTS
    assert row['status'] == 'failed'
    assert row['report'] == 'COUNT REPORT'
    assert row['stats_final_score'] == 100.0
    assert row['source_table'] == 'public.a'
    assert row[PERSIST_INSERTED_AT_COLUMN] is not None
    assert pd.notna(row[PERSIST_INSERTED_AT_COLUMN])
    assert 'payload_json' not in stored.columns
    assert 'timestamp' not in stored.columns


def test_persist_rounds_stats_floats_to_report_precision():
    results_engine = create_engine('sqlite:///:memory:')
    persister = CheckResultPersister(results_engine=results_engine)
    stats = _build_stats()
    stats.final_score = 83.33333333333333
    stats.final_diff_score = 16.666666666666668
    stats.issue_rows_pct = 33.333333333333336

    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status=ct.CHECK_FAILED,
        report='FAILED REPORT',
        stats=stats,
        details=_build_details(),
        check_type=ct.CHECK_TYPE_SAMPLES,
        source_table='public.a',
        target_table='public.b',
    )

    persister.persist(result, DataReference('dq_results_rounded'))

    stored = pd.read_sql('select * from dq_results_rounded', results_engine)
    assert stored.iloc[0]['stats_final_score'] == 83.33333
    assert stored.iloc[0]['stats_final_diff_score'] == 16.66667
    assert stored.iloc[0]['stats_issue_rows_pct'] == 33.33333


def test_persist_writes_timing_columns():
    results_engine = create_engine('sqlite:///:memory:')
    persister = CheckResultPersister(results_engine=results_engine)
    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status='success',
        report='TIMED REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_SAMPLES,
        source_table='public.a',
        target_table='public.b',
        timings=_build_timings(),
    )

    persister.persist(result, DataReference('dq_results_timings'))

    stored = pd.read_sql('select * from dq_results_timings', results_engine)
    row = stored.iloc[0]
    assert row['run_started_at'] == RUN_STARTED_AT
    assert row['run_finished_at'] == RUN_FINISHED_AT
    assert row['source_query_started_at'] == '2026-01-01 00:00:01'
    assert row['source_query_finished_at'] == '2026-01-01 00:00:02'
    assert row['target_query_started_at'] == '2026-01-01 00:00:02'
    assert row['target_query_finished_at'] == '2026-01-01 00:00:03'
    assert row['dataset_check_started_at'] == '2026-01-01 00:00:03'
    assert row['dataset_check_finished_at'] == '2026-01-01 00:00:04'


def test_validate_report_output_format_rejects_unknown_format():
    with pytest.raises(ValueError, match='report_output_format'):
        validate_report_output_format('xml')


def test_build_run_id_is_always_non_empty_and_unique():
    run_id = build_run_id()
    assert run_id
    assert len(run_id) == 16
    assert run_id != build_run_id()


def test_validate_run_id_rejects_empty_values():
    with pytest.raises(ValueError, match='run_id'):
        validate_run_id('')
    with pytest.raises(ValueError, match='run_id'):
        validate_run_id('   ')
    with pytest.raises(ValueError, match='run_id'):
        validate_run_id(None)


def test_clickhouse_persist_primary_key_column_is_not_nullable():
    from xoverrr.adapters.clickhouse import ClickHouseAdapter

    adapter = ClickHouseAdapter()
    assert (
        adapter._format_persist_column('run_id', 'short_string', 'run_id')
        == 'run_id String'
    )
    assert (
        adapter._format_persist_column('status', 'string', 'run_id')
        == 'status Nullable(String)'
    )
    assert (
        adapter._format_persist_column('run_started_at', 'datetime', 'run_id')
        == 'run_started_at Nullable(DateTime)'
    )


def test_oracle_persist_type_map_uses_native_types():
    from xoverrr.adapters.oracle import OracleAdapter

    adapter = OracleAdapter()
    assert adapter.PERSIST_TYPE_MAP['datetime'] == 'TIMESTAMP'
    assert adapter.PERSIST_TYPE_MAP['float'] == 'NUMBER'
    assert adapter.PERSIST_TYPE_MAP['text'] == 'VARCHAR2(4000)'
    assert adapter._format_persist_column('report', 'text', 'run_id') == 'report CLOB'
    assert (
        adapter._format_persist_column('source_query', 'text', 'run_id')
        == 'source_query VARCHAR2(4000)'
    )


def test_oracle_persist_insert_avoids_ora_24816_for_large_varchar_binds():
    from xoverrr.adapters.oracle import OracleAdapter

    adapter = OracleAdapter()
    oversized_json = json.dumps(['foobar'] * 500, ensure_ascii=False)
    record = {
        'check_type': 'samples',
        'report': 'FULL TEXT REPORT ' + ('R' * 8000),
        'details_issue_row_examples_json': oversized_json,
    }
    column_types = {
        'check_type': 'string',
        'report': 'text',
        'details_issue_row_examples_json': 'text',
    }

    insert_sql, bind_record = adapter._build_persist_insert(
        DataReference('dq_results'), record, column_types
    )

    assert len(oversized_json.encode('utf-8')) > 4000
    assert insert_sql.endswith(':report)')
    assert len(bind_record['report'].encode('utf-8')) > 4000
    assert len(bind_record['details_issue_row_examples_json'].encode('utf-8')) <= 4000


def test_persist_writes_oversized_json_details():
    results_engine = create_engine('sqlite:///:memory:')
    persister = CheckResultPersister(results_engine=results_engine)
    oversized_json = [
        {'column_name': 'value', 'payload': 'x' * 200} for _ in range(40)
    ]
    details = _build_details()
    details.issue_examples = pd.DataFrame(oversized_json)

    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status='success',
        report='OVERSIZED REPORT',
        stats=_build_stats(),
        details=details,
        check_type=ct.CHECK_TYPE_SAMPLES,
        source_table='public.a',
        target_table='public.b',
    )

    assert persister.persist(result, DataReference('dq_results_oversized')) is True

    stored = pd.read_sql('select * from dq_results_oversized', results_engine)
    stored_json = stored.iloc[0]['details_issue_examples_json']
    assert len(stored_json) > 4000
    assert json.loads(stored_json) == oversized_json


def test_persist_returns_false_on_storage_error():
    persister = CheckResultPersister(
        results_engine=create_engine(
            'sqlite:////this/path/does/not/exist/xoverrr.db'
        ),
    )
    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status='success',
        report='COUNT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_COUNTS,
        source_table='public.a',
        target_table='public.b',
    )

    assert persister.persist(result, DataReference('dq_results_boom')) is False


def test_persist_returns_false_when_engine_is_missing():
    persister = CheckResultPersister()
    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status='success',
        report='COUNT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_COUNTS,
        source_table='public.a',
        target_table='public.b',
    )

    assert persister.persist(result) is True
    assert persister.persist(result, DataReference('dq_results')) is False


def _checker_for_finalize(persister) -> DataQualityChecker:
    checker = DataQualityChecker.__new__(DataQualityChecker)
    checker.timezone = 'UTC'
    checker._active_run_id = RUN_ID
    checker._active_run_started_at = RUN_STARTED_AT
    checker._active_check_name = 'unit_test_compare'
    checker._run_timings = CheckRunTimings(run_started_at=RUN_STARTED_AT)
    checker.result_persister = persister
    return checker


def test_finalize_check_fails_status_when_persist_fails():
    class FailingPersister:
        def persist(self, *args, **kwargs):
            return False

    checker = _checker_for_finalize(FailingPersister())
    status, report = checker._finalize_check(
        status=ct.CHECK_SUCCESS,
        report='FULL TEXT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_SAMPLES,
        persist_result=DataReference('dq_results'),
        report_output_format=ct.REPORT_OUTPUT_FORMAT_TEXT,
        source_table='public.a',
        target_table='public.b',
    )

    assert status == ct.CHECK_FAILED
    assert report == 'FULL TEXT REPORT'


def test_finalize_check_keeps_status_when_persist_succeeds():
    class OkPersister:
        def persist(self, *args, **kwargs):
            return True

    checker = _checker_for_finalize(OkPersister())
    status, report = checker._finalize_check(
        status=ct.CHECK_SUCCESS,
        report='FULL TEXT REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_SAMPLES,
        persist_result=DataReference('dq_results'),
        report_output_format=ct.REPORT_OUTPUT_FORMAT_TEXT,
        source_table='public.a',
        target_table='public.b',
    )

    assert status == ct.CHECK_SUCCESS
    assert report == 'FULL TEXT REPORT'


def test_persist_uses_check_timezone_column():
    results_engine = create_engine('sqlite:///:memory:')
    persister = CheckResultPersister(results_engine=results_engine)
    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='Europe/Athens',
        status='success',
        report='TZ REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_SAMPLES,
        source_table='public.a',
        target_table='public.b',
    )

    persister.persist(result, DataReference('dq_results_tz'))

    stored = pd.read_sql('select * from dq_results_tz', results_engine)
    assert 'timezone' not in stored.columns
    assert stored.iloc[0]['check_timezone'] == 'Europe/Athens'


def test_normalize_persist_result():
    table_ref = DataReference('custom_results')
    assert normalize_persist_result(None) is None
    assert normalize_persist_result(table_ref) is table_ref
    with pytest.raises(TypeError, match='DataReference'):
        normalize_persist_result(True)


def test_persist_with_datareference_target_and_tags():
    results_engine = create_engine('sqlite:///:memory:')
    persister = CheckResultPersister(results_engine=results_engine)
    result = build_check_result(
        run_id=RUN_ID,
        timestamp=RUN_STARTED_AT,
        timezone='UTC',
        status='success',
        report='TAGGED REPORT',
        stats=_build_stats(),
        details=_build_details(),
        check_type=ct.CHECK_TYPE_SAMPLES,
        check_name='orders_daily_compare',
        check_tags={'env': 'dev', 'domain': 'orders'},
        source_table='public.orders_src',
        target_table='public.orders_trg',
    )

    persister.persist(result, DataReference('dq_results_custom'))

    stored = pd.read_sql(
        'select * from dq_results_custom', results_engine
    )
    assert stored.iloc[0]['check_name'] == 'orders_daily_compare'
    assert json.loads(stored.iloc[0]['check_tags_json']) == {
        'env': 'dev',
        'domain': 'orders',
    }
