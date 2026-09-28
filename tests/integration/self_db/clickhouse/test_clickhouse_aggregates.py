"""ClickHouse aggregate checks."""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker
from xoverrr.models import DataReference

MATCH_ROWS = 10
SRC_TABLE = 'test_ch_aggregates_src'
TRG_TABLE = 'test_ch_aggregates_trg'


def _ch_values(divergent_last: bool) -> str:
    rows = [
        f"({i}, {i * 10}, {i}, '2024-01-{i:02d}')" for i in range(1, MATCH_ROWS + 1)
    ]
    last = MATCH_ROWS + 1
    if divergent_last:
        rows.append(f"({last}, 9999, 50, '2024-01-{last:02d}')")
    else:
        rows.append(f"({last}, {last * 10}, {last}, '2024-01-{last:02d}')")
    return ',\n                '.join(rows)


class TestClickHouseAggregates:
    @pytest.fixture(autouse=True)
    def setup_agg_data(self, clickhouse_engine, table_helper):
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=SRC_TABLE,
            create_sql=f"""
                CREATE TABLE {SRC_TABLE} (
                    id          UInt32,
                    amount      Float64,
                    qty         UInt32,
                    created_at  Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {SRC_TABLE} (id, amount, qty, created_at) VALUES
                {_ch_values(divergent_last=True)}
            """,
        )
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=TRG_TABLE,
            create_sql=f"""
                CREATE TABLE {TRG_TABLE} (
                    id          UInt32,
                    amount      Float64,
                    qty         UInt32,
                    created_at  Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {TRG_TABLE} (id, amount, qty, created_at) VALUES
                {_ch_values(divergent_last=False)}
            """,
        )
        yield

    def test_aggregates_match(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        source_query = """
            SELECT id, amount, created_at
            FROM test_ch_aggregates_src
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date)
              AND id <= 10
        """
        target_query = """
            SELECT id, amount, created_at
            FROM test_ch_aggregates_trg
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date)
              AND id <= 10
        """
        params = {'start_date': '2024-01-01', 'end_date': '2024-01-11'}

        result = checker.check_aggregates(
            source=source_query,
            source_params=params,
            target=target_query,
            target_params=params,
            max_columns=['created_at'],
            sum_columns=['amount'],
            include_count=True,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0
        assert result.details.issue_examples.empty

    def test_multiple_max_and_sum_match(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        query = """
            SELECT amount, qty, created_at
            FROM {table}
            WHERE id <= 10
        """
        result = checker.check_aggregates(
            source=query.format(table='test_ch_aggregates_src'),
            target=query.format(table='test_ch_aggregates_trg'),
            max_columns=['created_at', 'qty'],
            sum_columns=['amount', 'qty'],
            include_count=True,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0
        assert result.details.issue_examples.empty

    def test_multiple_max_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source="""
                SELECT qty, created_at
                FROM test_ch_aggregates_src
            """,
            target="""
                SELECT qty, created_at
                FROM test_ch_aggregates_trg
                WHERE id <= 10
            """,
            max_columns=['created_at', 'qty'],
        )

        self._assert_failed(result, expected_columns={'max_created_at', 'max_qty'})

    def test_multiple_sum_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source='SELECT amount, qty FROM test_ch_aggregates_src',
            target='SELECT amount, qty FROM test_ch_aggregates_trg',
            sum_columns=['amount', 'qty'],
        )

        self._assert_failed(result, expected_columns={'sum_amount', 'sum_qty'})

    def test_sum_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source='SELECT amount FROM test_ch_aggregates_src',
            target='SELECT amount FROM test_ch_aggregates_trg',
            sum_columns=['amount'],
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'sum_amount'})

    def test_max_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source='SELECT created_at FROM test_ch_aggregates_src',
            target="""
                SELECT created_at
                FROM test_ch_aggregates_trg
                WHERE created_at < toDate('2024-01-11')
            """,
            max_columns=['created_at'],
        )

        self._assert_failed(result, expected_columns={'max_created_at'})

    def test_count_only_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source='SELECT id FROM test_ch_aggregates_src',
            target='SELECT id FROM test_ch_aggregates_trg WHERE id <= 10',
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'cnt'})

    def test_max_and_sum_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source="""
                SELECT amount, created_at
                FROM test_ch_aggregates_src
            """,
            target="""
                SELECT amount, created_at
                FROM test_ch_aggregates_trg
                WHERE id <= 10
            """,
            max_columns=['created_at'],
            sum_columns=['amount'],
        )

        self._assert_failed(result, expected_columns={'max_created_at', 'sum_amount'})

    def test_aggregates_table_vs_table_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source=DataReference('test_ch_aggregates_src', schema='test'),
            target=DataReference('test_ch_aggregates_trg', schema='test'),
            sum_columns=['amount'],
            include_count=True,
        )
        self._assert_failed(result, expected_columns={'sum_amount'})
        assert result.source_table == 'test.test_ch_aggregates_src'
        assert result.target_table == 'test.test_ch_aggregates_trg'

    def test_aggregates_query_vs_table_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source='SELECT amount FROM test_ch_aggregates_src WHERE id <= 10',
            target=DataReference('test_ch_aggregates_trg', schema='test'),
            sum_columns=['amount'],
        )
        self._assert_failed(result, expected_columns={'sum_amount'})
        assert result.source_table is None
        assert result.target_table == 'test.test_ch_aggregates_trg'

    def test_aggregates_table_vs_query_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_aggregates(
            source=DataReference('test_ch_aggregates_src', schema='test'),
            target='SELECT amount FROM test_ch_aggregates_trg WHERE id <= 10',
            sum_columns=['amount'],
        )
        self._assert_failed(result, expected_columns={'sum_amount'})
        assert result.source_table == 'test.test_ch_aggregates_src'
        assert result.target_table is None

    @staticmethod
    def _assert_failed(result, expected_columns):
        assert result.status == CHECK_FAILED
        assert result.stats.final_diff_score == 100
        assert result.stats.final_score == 0
        assert not result.details.issue_examples.empty
        columns = set(result.details.issue_examples['column_name'])
        assert expected_columns <= columns
