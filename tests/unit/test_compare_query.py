import re
from unittest.mock import Mock

import pandas as pd
import pytest

from xoverrr.adapters.clickhouse import ClickHouseAdapter
from xoverrr.adapters.oracle import OracleAdapter
from xoverrr.adapters.postgres import PostgresAdapter
from xoverrr.core import InDatabaseChecker
from xoverrr.models import DataReference


@pytest.mark.parametrize(
    'adapter_cls', [PostgresAdapter, OracleAdapter, ClickHouseAdapter]
)
def test_build_compare_query_shape(adapter_cls):
    adapter = adapter_cls()
    meta = pd.DataFrame(
        {
            'column_name': ['id', 'name', 'created_at'],
            'data_type': ['int4', 'text', 'date'],
            'column_id': [1, 2, 3],
        }
    )
    query, params = adapter.build_compare_query(
        DataReference('src_table', 'public'),
        DataReference('trg_table', 'public'),
        common_columns=['id', 'name', 'created_at'],
        key_columns=['id'],
        source_columns_meta=meta,
        target_columns_meta=meta,
        date_column='created_at',
        update_column=None,
        start_date='2024-01-01',
        end_date='2024-01-31',
        exclude_recent_hours=None,
        timezone='UTC',
        max_examples=3,
    )
    normalized = ' '.join(query.lower().split())
    assert 'full outer join' in normalized
    assert 'src_raw' in normalized
    assert 'trg_raw' in normalized
    assert 'src_marked' in normalized
    assert 'classified' in normalized
    assert 'public.src_table' in normalized
    assert 'public.trg_table' in normalized
    assert 'src_norm' not in normalized
    assert 'union all' in normalized
    assert 'compare_result' in normalized
    assert 'nulls first' in normalized
    assert 'on src.id = trg.id' in normalized
    assert params['start_date'] == '2024-01-01'
    assert params['end_date'] == '2024-01-31'
    assert params['max_examples'] == 3
    if adapter_cls is OracleAdapter:
        assert 'decode(src_name, trg_name, 0, 1)' in normalized
        assert 'listagg(' not in normalized
    elif adapter_cls is ClickHouseAdapter:
        assert 'isnotdistinctfrom' not in normalized
        assert 'grouparrayif' not in normalized
        assert '(src_name = trg_name) or (src_name is null and trg_name is null)' in normalized
    else:
        assert 'src_name is not distinct from trg_name' in normalized
        assert 'array_agg(' not in normalized


@pytest.mark.parametrize(
    'adapter_cls', [PostgresAdapter, OracleAdapter, ClickHouseAdapter]
)
def test_recent_keys_uses_union_all(adapter_cls):
    adapter = adapter_cls()
    meta = pd.DataFrame(
        {
            'column_name': ['id', 'name', 'created_at', 'updated_at'],
            'data_type': ['int4', 'text', 'date', 'timestamp'],
            'column_id': [1, 2, 3, 4],
        }
    )
    query, _params = adapter.build_compare_query(
        DataReference('src_table', 'public'),
        DataReference('trg_table', 'public'),
        common_columns=['id', 'name', 'created_at', 'updated_at'],
        key_columns=['id'],
        source_columns_meta=meta,
        target_columns_meta=meta,
        date_column='created_at',
        update_column='updated_at',
        start_date='2024-01-01',
        end_date='2024-01-31',
        exclude_recent_hours=24,
        timezone='UTC',
        max_examples=3,
    )
    lowered = ' '.join(query.lower().split())
    assert 'recent_keys' in lowered
    assert 'left join recent_keys' in lowered
    assert 'r.x_rk is null' in lowered
    assert 'not exists' not in lowered
    assert 'union all' in lowered
    assert re.search(r'\bunion\b(?!\s+all)', lowered) is None
    if adapter_cls is ClickHouseAdapter:
        assert 'on n.id = r.id' in lowered
        assert 'on src.id = trg.id' in lowered
        assert 'isnotdistinctfrom' not in lowered
        assert '(src_name = trg_name) or (src_name is null and trg_name is null)' in lowered
        assert 'grouparrayif' not in lowered
    elif adapter_cls is PostgresAdapter:
        assert 'src_name is not distinct from trg_name' in lowered
        assert 'array_agg(' not in lowered
    else:
        assert 'decode(src_name, trg_name, 0, 1)' in lowered
        assert 'listagg(' not in lowered


def test_indatabase_checker_uses_single_engine():
    engine = Mock()
    engine.dialect.name = 'postgresql'
    checker = InDatabaseChecker(engine, timezone='UTC')
    assert checker.engine is engine
    assert checker.source_engine is engine
    assert checker.target_engine is engine
