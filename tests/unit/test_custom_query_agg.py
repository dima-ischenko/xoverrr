import pandas as pd
import pytest

from xoverrr.adapters.postgres import PostgresAdapter
from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker
from xoverrr.models import DBMSType
from xoverrr.persistence import CheckResultPersister
from xoverrr.utils import prepare_dataframe


INNER = (
    'SELECT id, amount, created_at FROM source_table WHERE created_at >= :start_date'
)


def test_adapter_builds_aggregate_sql():
    sql = PostgresAdapter().build_custom_query_aggregate_sql(
        INNER + ';',
        max_columns=['created_at'],
        sum_columns=['Amount'],
        include_count=True,
    )

    assert sql == (
        'SELECT max(created_at) as max_created_at, sum(amount) as sum_amount, '
        f'count(*) as cnt FROM ({INNER}) x_subq'
    )


def test_adapter_count_only_and_validation():
    adapter = PostgresAdapter()
    sql = adapter.build_custom_query_aggregate_sql(INNER, include_count=True)
    assert sql == f'SELECT count(*) as cnt FROM ({INNER}) x_subq'

    with pytest.raises(ValueError, match='include_count'):
        adapter.build_custom_query_aggregate_sql(INNER)
    with pytest.raises(ValueError, match='Invalid aggregate column'):
        adapter.build_custom_query_aggregate_sql(INNER, sum_columns=['amount;drop'])


def test_check_custom_queries_agg_requires_columns():
    checker = DataQualityChecker.__new__(DataQualityChecker)
    checker.source_engine = object()
    checker.target_engine = object()

    with pytest.raises(ValueError, match='include_count'):
        checker.check_custom_queries_agg(
            source_query='select 1',
            target_query='select 1',
        )


class _Adapter:
    def convert_types(self, df, metadata, timezone):
        return df

    def build_custom_query_aggregate_sql(self, *args, **kwargs):
        return PostgresAdapter().build_custom_query_aggregate_sql(*args, **kwargs)


def _checker():
    checker = DataQualityChecker.__new__(DataQualityChecker)
    checker.source_engine = object()
    checker.target_engine = object()
    checker.source_db_type = DBMSType.POSTGRESQL
    checker.target_db_type = DBMSType.POSTGRESQL
    checker.timezone = 'UTC'
    checker.result_persister = CheckResultPersister(None)
    checker._report_context = {
        'library_version': 'test',
        'source_db_type': 'postgresql',
        'target_db_type': 'postgresql',
    }
    checker.check_stats = {
        'checked': 0,
        CHECK_SUCCESS: 0,
        CHECK_FAILED: 0,
        'skipped': 0,
        'tables_success': set(),
        'tables_failed': set(),
        'tables_skipped': set(),
        'start_time': '2024-01-01 00:00:00',
        'end_time': None,
    }
    return checker


def test_check_custom_queries_agg_uses_compare_dataframes(monkeypatch):
    checker = _checker()
    metadata = pd.DataFrame({'column_name': ['amount'], 'data_type': ['numeric']})
    executed = []

    monkeypatch.setattr(
        checker,
        '_get_metadata_cols_for_custom_query',
        lambda query, engine: metadata,
    )
    monkeypatch.setattr(checker, '_get_adapter', lambda db_type: _Adapter())

    def _execute(query, engine, timezone=None, query_side=None):
        sql = query[0] if isinstance(query, tuple) else query
        executed.append(sql)
        if query_side == 'source':
            return prepare_dataframe(pd.DataFrame({'sum_amount': ['10'], 'cnt': ['1']}))
        return prepare_dataframe(pd.DataFrame({'sum_amount': ['10'], 'cnt': ['2']}))

    monkeypatch.setattr(checker, '_execute_query', _execute)

    result = checker.check_custom_queries_agg(
        source_query='SELECT amount FROM source_table',
        source_params={'start_date': '2024-01-01'},
        target_query='SELECT amount FROM target_table',
        target_params={'start_date': '2024-01-01'},
        sum_columns=['amount'],
        include_count=True,
        check_name='amount_sum',
    )

    assert result.status == CHECK_FAILED
    assert result.stats.final_diff_score == 100.0
    assert result.stats.final_score == 0.0
    assert 'count(*) as cnt' in executed[-1]
    assert 'sum(amount) as sum_amount' in executed[-1]
    assert 'cnt' in set(result.details.issue_examples['column_name'])
