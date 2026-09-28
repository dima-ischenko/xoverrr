import pandas as pd
import pytest

from xoverrr.adapters.clickhouse import ClickHouseAdapter
from xoverrr.adapters.oracle import OracleAdapter
from xoverrr.adapters.postgres import PostgresAdapter
from xoverrr.core import DataQualityChecker
from xoverrr.models import DataReference
from xoverrr.stats import normalize_hash_pct


def _meta(*pairs):
    return pd.DataFrame(list(pairs), columns=['column_name', 'data_type'])


def test_normalize_hash_pct():
    assert normalize_hash_pct(None) is None
    assert normalize_hash_pct(0) is None
    assert normalize_hash_pct(40) == 40
    with pytest.raises(ValueError, match='1 to 100'):
        normalize_hash_pct(101)
    with pytest.raises(ValueError, match='1 to 100'):
        normalize_hash_pct(-1)


def test_postgres_hash_filter_covers_types_and_composite_keys():
    sql = PostgresAdapter().build_hash_filter(
        ['id', 'created_at', 'updated_at', 'is_active'],
        25,
        _meta(
            ('id', 'integer'),
            ('created_at', 'date'),
            ('updated_at', 'timestamp without time zone'),
            ('is_active', 'boolean'),
        ),
        'UTC',
    )

    assert "to_char(created_at::date, 'YYYYMMDD')" in sql
    assert "to_char(updated_at::timestamp, 'YYYYMMDDHH24MISS')" in sql
    assert "case when is_active then '1'" in sql
    assert 'cast(id as text)' in sql
    assert " || '|' || " in sql
    assert sql.endswith('< 25')


def test_oracle_hash_filter_quotes_date_format():
    sql = OracleAdapter().build_hash_filter(
        ['created_at'],
        10,
        _meta(('created_at', 'date')),
        'UTC',
    )

    assert "to_char(created_at, 'YYYYMMDD')" in sql
    assert 'STANDARD_HASH' in sql
    assert sql.endswith('< 10')


def test_clickhouse_hash_filter_uses_datetime_not_timestamp_regex():
    sql = ClickHouseAdapter().build_hash_filter(
        ['updated_at'],
        50,
        _meta(('updated_at', 'datetime64(3)')),
        'UTC',
    )

    assert "formatDateTime(updated_at, '%Y%m%d%H%i%s')" in sql
    assert 'MD5' in sql


def test_oracle_build_data_query_keeps_hash_predicate():
    query, _params = OracleAdapter().build_data_query(
        DataReference('employees', 'hr'),
        ['id', 'name'],
        None,
        None,
        None,
        None,
        columns_meta=_meta(('id', 'number'), ('name', 'varchar2')),
        timezone='UTC',
        key_column=['id'],
        hash_pct=30,
    )

    assert 'STANDARD_HASH' in query
    assert '% 100 < 30' not in query
    assert '< 30' in query


def test_wrap_query_with_hash_sample():
    wrapped = PostgresAdapter().wrap_query_with_hash_sample(
        'SELECT id, amount FROM orders;',
        ['id'],
        20,
        _meta(('id', 'integer'), ('amount', 'numeric')),
    )

    assert wrapped.startswith(
        'SELECT * FROM (SELECT id, amount FROM orders) x_hash WHERE '
    )
    assert 'md5' in wrapped


def test_check_aggregates_requires_hash_columns():
    checker = DataQualityChecker.__new__(DataQualityChecker)
    checker.source_engine = object()
    checker.target_engine = object()

    with pytest.raises(ValueError, match='hash_columns'):
        checker.check_aggregates(
            source='select 1 as amount',
            target='select 1 as amount',
            sum_columns=['amount'],
            hash_pct=20,
        )
