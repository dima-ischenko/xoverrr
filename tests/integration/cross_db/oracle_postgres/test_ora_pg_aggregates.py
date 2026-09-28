"""Aggregate check between Oracle and PostgreSQL."""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker
from xoverrr.models import DataReference

MATCH_ROWS = 10
TABLE_NAME = 'test_ora_pg_aggregates'


def _ora_values(divergent_last: bool) -> str:
    rows = [
        f"({i}, {i * 10}, {i}, date'2024-01-{i:02d}')" for i in range(1, MATCH_ROWS + 1)
    ]
    last = MATCH_ROWS + 1
    if divergent_last:
        rows.append(f"({last}, 9999, 50, date'2024-01-{last:02d}')")
    else:
        rows.append(f"({last}, {last * 10}, {last}, date'2024-01-{last:02d}')")
    return ',\n                '.join(rows)


def _pg_values(divergent_last: bool) -> str:
    rows = [
        f"({i}, {i * 10}, {i}, '2024-01-{i:02d}')" for i in range(1, MATCH_ROWS + 1)
    ]
    last = MATCH_ROWS + 1
    if divergent_last:
        rows.append(f"({last}, 9999, 50, '2024-01-{last:02d}')")
    else:
        rows.append(f"({last}, {last * 10}, {last}, '2024-01-{last:02d}')")
    return ',\n                '.join(rows)


class TestOraPgAggregates:
    @pytest.fixture(autouse=True)
    def setup_agg_data(self, oracle_engine, postgres_engine, table_helper):
        table_helper.create_table(
            engine=oracle_engine,
            table_name=TABLE_NAME,
            create_sql=f"""
                CREATE TABLE {TABLE_NAME} (
                    id          INTEGER PRIMARY KEY,
                    amount      number,
                    qty         number,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {TABLE_NAME} (id, amount, qty, created_at) VALUES
                {_ora_values(divergent_last=True)}
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=TABLE_NAME,
            create_sql=f"""
                CREATE TABLE {TABLE_NAME} (
                    id          INTEGER PRIMARY KEY,
                    amount      numeric,
                    qty         INTEGER,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {TABLE_NAME} (id, amount, qty, created_at) VALUES
                {_pg_values(divergent_last=False)}
            """,
        )
        yield

    def test_aggregates_match(self, oracle_engine, postgres_engine):
        checker = DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )
        result = checker.check_aggregates(
            source="""
                SELECT amount, created_at
                FROM test.test_ora_pg_aggregates
                WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
                  AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
            """,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-10'},
            target="""
                SELECT amount, created_at
                FROM test.test_ora_pg_aggregates
                WHERE created_at >= date_trunc('day', cast(:start_date as date))
                  AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 day'
            """,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-10'},
            max_columns=['created_at'],
            sum_columns=['amount'],
            include_count=True,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0

    def test_multiple_max_and_sum_match(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source="""
                SELECT amount, qty, created_at
                FROM test.test_ora_pg_aggregates
                WHERE id <= 10
            """,
            target="""
                SELECT amount, qty, created_at
                FROM test.test_ora_pg_aggregates
                WHERE id <= 10
            """,
            max_columns=['created_at', 'qty'],
            sum_columns=['amount', 'qty'],
            include_count=True,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0
        assert result.details.issue_examples.empty

    def test_multiple_max_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source="""
                SELECT qty, created_at
                FROM test.test_ora_pg_aggregates
            """,
            target="""
                SELECT qty, created_at
                FROM test.test_ora_pg_aggregates
                WHERE id <= 10
            """,
            max_columns=['created_at', 'qty'],
        )

        self._assert_failed(result, expected_columns={'max_created_at', 'max_qty'})

    def test_multiple_sum_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source="""
                SELECT amount, qty
                FROM test.test_ora_pg_aggregates
            """,
            target="""
                SELECT amount, qty
                FROM test.test_ora_pg_aggregates
                WHERE id <= 10
            """,
            sum_columns=['amount', 'qty'],
        )

        self._assert_failed(result, expected_columns={'sum_amount', 'sum_qty'})

    def test_sum_and_count_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source="""
                SELECT amount
                FROM test.test_ora_pg_aggregates
            """,
            target="""
                SELECT amount
                FROM test.test_ora_pg_aggregates
                WHERE id <= 10
            """,
            sum_columns=['amount'],
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'sum_amount', 'cnt'})

    def test_max_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source="""
                SELECT created_at
                FROM test.test_ora_pg_aggregates
            """,
            target="""
                SELECT created_at
                FROM test.test_ora_pg_aggregates
                WHERE created_at < DATE '2024-01-11'
            """,
            max_columns=['created_at'],
        )

        self._assert_failed(result, expected_columns={'max_created_at'})

    def test_count_only_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source="""
                SELECT id
                FROM test.test_ora_pg_aggregates
            """,
            target="""
                SELECT id
                FROM test.test_ora_pg_aggregates
                WHERE id <= 10
            """,
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'cnt'})

    def test_max_and_sum_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source="""
                SELECT amount, created_at
                FROM test.test_ora_pg_aggregates
            """,
            target="""
                SELECT amount, created_at
                FROM test.test_ora_pg_aggregates
                WHERE id <= 10
            """,
            max_columns=['created_at'],
            sum_columns=['amount'],
        )

        self._assert_failed(result, expected_columns={'max_created_at', 'sum_amount'})

    def test_aggregates_table_vs_table_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source=DataReference('test_ora_pg_aggregates', schema='test'),
            target=DataReference('test_ora_pg_aggregates', schema='test'),
            sum_columns=['amount'],
            include_count=True,
        )
        self._assert_failed(result, expected_columns={'sum_amount'})
        assert result.source_table == 'test.test_ora_pg_aggregates'
        assert result.target_table == 'test.test_ora_pg_aggregates'

    def test_aggregates_query_vs_table_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source='SELECT amount FROM test.test_ora_pg_aggregates WHERE id <= 10',
            target=DataReference('test_ora_pg_aggregates', schema='test'),
            sum_columns=['amount'],
        )
        self._assert_failed(result, expected_columns={'sum_amount'})
        assert result.source_table is None
        assert result.target_table == 'test.test_ora_pg_aggregates'

    def test_aggregates_table_vs_query_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_aggregates(
            source=DataReference('test_ora_pg_aggregates', schema='test'),
            target='SELECT amount FROM test.test_ora_pg_aggregates WHERE id <= 10',
            sum_columns=['amount'],
        )
        self._assert_failed(result, expected_columns={'sum_amount'})
        assert result.source_table == 'test.test_ora_pg_aggregates'
        assert result.target_table is None

    @staticmethod
    def _checker(oracle_engine, postgres_engine):
        return DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )

    @staticmethod
    def _assert_failed(result, expected_columns):
        assert result.status == CHECK_FAILED
        assert result.stats.final_diff_score == 100
        assert result.stats.final_score == 0
        assert not result.details.issue_examples.empty
        columns = set(result.details.issue_examples['column_name'])
        assert expected_columns <= columns
