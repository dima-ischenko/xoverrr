import pandas as pd
import pytest

from xoverrr.adapters.postgres import PostgresAdapter
from xoverrr.checks.aggregates import resolve_aggregate_relation
from xoverrr.compare import prepare_dataframe
from xoverrr.constants import (CHECK_FAILED, CHECK_SUCCESS,
                               CHECK_TYPE_AGGREGATES)
from xoverrr.core import DataQualityChecker
from xoverrr.models import DataReference, DBMSType
from xoverrr.persistence import CheckResultPersister

INNER = (
    'SELECT id, amount, created_at FROM source_table WHERE created_at >= :start_date'
)


def test_adapter_builds_aggregate_sql():
    sql = PostgresAdapter().build_aggregate_sql(
        INNER + ';',
        max_columns=['created_at'],
        sum_columns=['Amount'],
        include_count=True,
    )

    assert sql == (
        'SELECT max(created_at) as max_created_at, sum(amount) as sum_amount, '
        f'count(*) as cnt FROM ({INNER}) x_subq'
    )

    table_sql = PostgresAdapter().build_aggregate_sql(
        'SELECT * FROM sales.orders',
        sum_columns=['amount'],
        table='sales.orders',
    )
    assert table_sql == 'SELECT sum(amount) as sum_amount FROM sales.orders'


def test_adapter_count_only_and_validation():
    adapter = PostgresAdapter()
    sql = adapter.build_aggregate_sql(INNER, include_count=True)
    assert sql == f'SELECT count(*) as cnt FROM ({INNER}) x_subq'

    with pytest.raises(ValueError, match='include_count'):
        adapter.build_aggregate_sql(INNER)
    with pytest.raises(ValueError, match='Invalid aggregate column'):
        adapter.build_aggregate_sql(INNER, sum_columns=['amount;drop'])


def test_check_aggregates_requires_columns():
    checker = DataQualityChecker.__new__(DataQualityChecker)
    checker.source_engine = object()
    checker.target_engine = object()

    with pytest.raises(ValueError, match='include_count'):
        checker.check_aggregates(
            source='select 1',
            target='select 1',
        )


class _Adapter(PostgresAdapter):
    def convert_types(self, df, metadata, timezone):
        return df


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


def test_resolve_aggregate_relation_table_and_query():
    query, params, table = resolve_aggregate_relation(
        DataReference('orders', 'sales'), None, 'source'
    )
    assert query == 'SELECT * FROM sales.orders'
    assert params == {}
    assert table == 'sales.orders'

    query, params, table = resolve_aggregate_relation(
        'SELECT amount FROM orders WHERE id > :id',
        {'id': 1},
        'target',
    )
    assert query == 'SELECT amount FROM orders WHERE id > :id'
    assert params == {'id': 1}
    assert table is None


def test_resolve_aggregate_relation_rejects_params_on_table():
    with pytest.raises(ValueError, match='source_params'):
        resolve_aggregate_relation(
            DataReference('orders', 'sales'),
            {'start_date': '2024-01-01'},
            'source',
        )


def test_resolve_aggregate_relation_rejects_bad_type():
    with pytest.raises(TypeError, match='DataReference or a SQL string'):
        resolve_aggregate_relation(123, None, 'source')


def test_resolve_aggregate_relation_rejects_empty_query():
    with pytest.raises(ValueError, match='source query is empty'):
        resolve_aggregate_relation('   ', None, 'source')


def test_check_aggregates_table_vs_query_returns_built_sql(monkeypatch):
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
        return prepare_dataframe(pd.DataFrame({'sum_amount': ['10']}))

    monkeypatch.setattr(checker, '_execute_query', _execute)

    result = checker.check_aggregates(
        source=DataReference('orders', 'sales'),
        target='SELECT amount FROM archive.orders',
        sum_columns=['amount'],
    )

    source_sql = 'SELECT sum(amount) as sum_amount FROM sales.orders'
    target_sql = (
        'SELECT sum(amount) as sum_amount '
        'FROM (SELECT amount FROM archive.orders) x_subq'
    )
    assert result.status == CHECK_SUCCESS
    assert result.check_type == CHECK_TYPE_AGGREGATES
    assert result.source_table == 'sales.orders'
    assert result.source_query == source_sql
    assert result.target_table is None
    assert result.target_query == target_sql
    assert executed == [source_sql, target_sql]
    assert source_sql in result.report
    assert target_sql in result.report
    assert 'SELECT * FROM' not in result.report


def test_check_aggregates_hash_filter_skips_extra_subquery(monkeypatch):
    checker = _checker()
    metadata = pd.DataFrame(
        {'column_name': ['id', 'amount'], 'data_type': ['integer', 'numeric']}
    )
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
        return prepare_dataframe(pd.DataFrame({'sum_amount': ['10']}))

    monkeypatch.setattr(checker, '_execute_query', _execute)

    result = checker.check_aggregates(
        source=DataReference('orders', 'sales'),
        target='SELECT id, amount FROM archive.orders',
        sum_columns=['amount'],
        hash_columns=['id'],
        hash_pct=20,
    )

    assert result.status == CHECK_SUCCESS
    assert executed[0].startswith(
        'SELECT sum(amount) as sum_amount FROM sales.orders WHERE '
    )
    assert 'x_subq' not in executed[0]
    assert 'x_hash' not in executed[0]
    assert executed[1].startswith(
        'SELECT sum(amount) as sum_amount '
        'FROM (SELECT id, amount FROM archive.orders) x_subq WHERE '
    )
    assert 'x_hash' not in executed[1]
    assert executed[0] in result.report
    assert executed[1] in result.report


def test_check_aggregates_uses_compare_dataframes(monkeypatch):
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

    result = checker.check_aggregates(
        source='SELECT amount FROM source_table',
        source_params={'start_date': '2024-01-01'},
        target='SELECT amount FROM target_table',
        target_params={'start_date': '2024-01-01'},
        sum_columns=['amount'],
        include_count=True,
        check_name='amount_sum',
    )

    assert result.status == CHECK_FAILED
    assert result.check_type == CHECK_TYPE_AGGREGATES
    assert result.stats.final_diff_score == 100.0
    assert result.stats.final_score == 0.0
    source_sql = (
        'SELECT sum(amount) as sum_amount, count(*) as cnt '
        'FROM (SELECT amount FROM source_table) x_subq'
    )
    target_sql = (
        'SELECT sum(amount) as sum_amount, count(*) as cnt '
        'FROM (SELECT amount FROM target_table) x_subq'
    )
    assert executed == [source_sql, target_sql]
    assert result.source_query == source_sql
    assert result.target_query == target_sql
    assert source_sql in result.report
    assert target_sql in result.report
    assert 'cnt' in set(result.details.issue_examples['column_name'])
    breakdown = result.report.split('ISSUE BREAKDOWN:', 1)[1]
    assert 'aggregate' in breakdown
    assert 'source_value' in breakdown
    assert 'target_value' in breakdown
    assert 'cnt' in breakdown
    assert 'sum_amount' not in breakdown
    assert 'primary_key' not in breakdown
