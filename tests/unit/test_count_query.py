import pytest

from xoverrr.adapters.clickhouse import ClickHouseAdapter
from xoverrr.adapters.oracle import OracleAdapter
from xoverrr.adapters.postgres import PostgresAdapter
from xoverrr.models import DataReference


@pytest.mark.parametrize(
    'adapter_cls', [PostgresAdapter, OracleAdapter, ClickHouseAdapter]
)
def test_total_count_query(adapter_cls):
    adapter = adapter_cls()
    query, params = adapter.build_total_count_query(
        DataReference('users', 'public')
    )

    normalized = ' '.join(query.lower().split())
    assert 'count(*) as cnt' in normalized
    assert 'group by' not in normalized
    assert 'public.users' in normalized
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


def test_count_volume_scores():
    from xoverrr.utils import count_volume_scores, build_total_count_stats

    assert count_volume_scores(0, 4) == (0.0, 100.0)
    diff, score = count_volume_scores(1, 3)
    assert diff == pytest.approx(25.0)
    assert score == pytest.approx(75.0)
    assert count_volume_scores(0, 0) == (0.0, 100.0)

    stats = build_total_count_stats(4, 3)
    assert stats.total_source_rows == 4
    assert stats.total_target_rows == 3
    assert stats.final_diff_score == pytest.approx(25.0)
    assert stats.final_score == pytest.approx(75.0)
