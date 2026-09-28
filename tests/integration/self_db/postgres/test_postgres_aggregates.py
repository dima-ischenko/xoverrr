"""PostgreSQL aggregate checks."""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker
from xoverrr.models import DataReference


class TestPostgresAggregates:
    @pytest.fixture(autouse=True)
    def setup_agg_data(self, postgres_engine, table_helper):
        source_table = 'test_pg_aggregates_src'
        target_table = 'test_pg_aggregates_trg'

        table_helper.create_table(
            engine=postgres_engine,
            table_name=source_table,
            create_sql=f"""
                CREATE TABLE {source_table} (
                    id          INTEGER PRIMARY KEY,
                    amount      numeric,
                    qty         INTEGER,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {source_table} (id, amount, qty, created_at, updated_at) VALUES
                (1, 10, 1, '2024-01-01', '2024-01-01 10:00:00'),
                (2, 20, 2, '2024-01-02', '2024-01-02 11:00:00'),
                (3, 999, 50, '2024-01-03', '2024-01-03 12:00:00')
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=target_table,
            create_sql=f"""
                CREATE TABLE {target_table} (
                    id          INTEGER PRIMARY KEY,
                    amount      numeric,
                    qty         INTEGER,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {target_table} (id, amount, qty, created_at, updated_at) VALUES
                (1, 10, 1, '2024-01-01', '2024-01-01 10:00:00'),
                (2, 20, 2, '2024-01-02', '2024-01-02 11:00:00'),
                (3, 30, 3, '2024-01-03', '2024-01-03 12:00:00')
            """,
        )
        yield

    def test_aggregates_match(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        source_query = """
            SELECT id, amount, created_at
            FROM test.test_pg_aggregates_src
            WHERE created_at >= cast(:start_date as date)
              AND created_at < cast(:end_date as date)
              AND id < 3
        """
        target_query = """
            SELECT id, amount, created_at
            FROM test.test_pg_aggregates_trg
            WHERE created_at >= cast(:start_date as date)
              AND created_at < cast(:end_date as date)
              AND id < 3
        """
        params = {'start_date': '2024-01-01', 'end_date': '2024-01-04'}

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

    def test_multiple_max_and_sum_match(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        query = """
            SELECT amount, qty, created_at, updated_at
            FROM {table}
            WHERE id < 3
        """
        result = checker.check_aggregates(
            source=query.format(table='test.test_pg_aggregates_src'),
            target=query.format(table='test.test_pg_aggregates_trg'),
            max_columns=['created_at', 'updated_at', 'qty'],
            sum_columns=['amount', 'qty'],
            include_count=True,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0
        assert result.details.issue_examples.empty

    def test_multiple_max_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source="""
                SELECT qty, created_at, updated_at
                FROM test.test_pg_aggregates_src
            """,
            target="""
                SELECT qty, created_at, updated_at
                FROM test.test_pg_aggregates_trg
                WHERE id < 3
            """,
            max_columns=['created_at', 'updated_at', 'qty'],
        )

        self._assert_failed(
            result, expected_columns={'max_created_at', 'max_updated_at', 'max_qty'}
        )

    def test_multiple_sum_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source='SELECT amount, qty FROM test.test_pg_aggregates_src',
            target='SELECT amount, qty FROM test.test_pg_aggregates_trg',
            sum_columns=['amount', 'qty'],
        )

        self._assert_failed(result, expected_columns={'sum_amount', 'sum_qty'})

    def test_sum_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source='SELECT amount FROM test.test_pg_aggregates_src',
            target='SELECT amount FROM test.test_pg_aggregates_trg',
            sum_columns=['amount'],
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'sum_amount'})

    def test_max_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source='SELECT created_at FROM test.test_pg_aggregates_src',
            target="""
                SELECT created_at
                FROM test.test_pg_aggregates_trg
                WHERE created_at < DATE '2024-01-03'
            """,
            max_columns=['created_at'],
        )

        self._assert_failed(result, expected_columns={'max_created_at'})

    def test_count_only_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source='SELECT id FROM test.test_pg_aggregates_src',
            target='SELECT id FROM test.test_pg_aggregates_trg WHERE id < 3',
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'cnt'})

    def test_max_and_sum_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source="""
                SELECT amount, created_at
                FROM test.test_pg_aggregates_src
            """,
            target="""
                SELECT amount, created_at
                FROM test.test_pg_aggregates_trg
                WHERE id < 3
            """,
            max_columns=['created_at'],
            sum_columns=['amount'],
        )

        self._assert_failed(result, expected_columns={'max_created_at', 'sum_amount'})

    def test_aggregates_table_vs_table_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source=DataReference('test_pg_aggregates_src', schema='test'),
            target=DataReference('test_pg_aggregates_trg', schema='test'),
            sum_columns=['amount'],
            include_count=True,
        )
        self._assert_failed(result, expected_columns={'sum_amount'})
        assert result.source_table == 'test.test_pg_aggregates_src'
        assert result.target_table == 'test.test_pg_aggregates_trg'

    def test_aggregates_query_vs_table_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source='SELECT amount FROM test.test_pg_aggregates_src WHERE id < 3',
            target=DataReference('test_pg_aggregates_trg', schema='test'),
            sum_columns=['amount'],
        )
        self._assert_failed(result, expected_columns={'sum_amount'})
        assert result.source_table is None
        assert result.target_table == 'test.test_pg_aggregates_trg'

    def test_aggregates_table_vs_query_mismatch(self, postgres_engine):
        checker = DataQualityChecker(postgres_engine, postgres_engine, timezone='UTC')
        result = checker.check_aggregates(
            source=DataReference('test_pg_aggregates_src', schema='test'),
            target='SELECT amount FROM test.test_pg_aggregates_trg WHERE id < 3',
            sum_columns=['amount'],
        )
        self._assert_failed(result, expected_columns={'sum_amount'})

    @staticmethod
    def _assert_failed(result, expected_columns):
        assert result.status == CHECK_FAILED
        assert result.stats.final_diff_score == 100
        assert result.stats.final_score == 0
        assert not result.details.issue_examples.empty
        columns = set(result.details.issue_examples['column_name'])
        assert expected_columns <= columns
