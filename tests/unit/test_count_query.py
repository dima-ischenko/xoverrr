import pytest

from xoverrr.adapters.clickhouse import ClickHouseAdapter
from xoverrr.adapters.oracle import OracleAdapter
from xoverrr.adapters.postgres import PostgresAdapter
from xoverrr.constants import COUNTS_TOTAL_DT
from xoverrr.models import DataReference


@pytest.mark.parametrize(
    'adapter_cls', [PostgresAdapter, OracleAdapter, ClickHouseAdapter]
)
def test_count_query_without_date_column_is_whole_table_count(adapter_cls):
    adapter = adapter_cls()
    query, params = adapter.build_count_query_common(
        DataReference('users', 'public'),
        date_column=None,
        start_date=None,
        end_date=None,
        columns_meta=None,
        timezone=None,
    )

    normalized = ' '.join(query.lower().split())
    assert f"'{COUNTS_TOTAL_DT}' as dt" in normalized
    assert 'count(*) as cnt' in normalized
    assert 'group by' not in normalized
    assert params == {}


@pytest.mark.parametrize(
    'adapter_cls', [PostgresAdapter, OracleAdapter, ClickHouseAdapter]
)
def test_count_query_with_date_column_groups_by_day(adapter_cls):
    adapter = adapter_cls()
    query, params = adapter.build_count_query_common(
        DataReference('users', 'public'),
        date_column='created_at',
        start_date='2024-01-01',
        end_date='2024-01-31',
        columns_meta=None,
        timezone=None,
    )

    normalized = ' '.join(query.lower().split())
    assert 'group by' in normalized
    assert 'created_at' in normalized
    assert params == {'start_date': '2024-01-01', 'end_date': '2024-01-31'}
