"""Custom-query aggregate check between Oracle and PostgreSQL."""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker


class TestOraPgCustomQueryAgg:
    @pytest.fixture(autouse=True)
    def setup_agg_data(self, oracle_engine, postgres_engine, table_helper):
        table_name = 'test_ora_pg_custom_query_agg'

        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    amount      number,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, amount, created_at) VALUES
                (1, 10, date'2024-01-01'),
                (2, 20, date'2024-01-02'),
                (3, 30, date'2024-01-03')
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    amount      numeric,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, amount, created_at) VALUES
                (1, 10, '2024-01-01'),
                (2, 20, '2024-01-02'),
                (3, 30, '2024-01-03')
            """,
        )
        yield

    def test_aggregates_match(self, oracle_engine, postgres_engine):
        checker = DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )
        result = checker.check_custom_queries_agg(
            source_query="""
                SELECT amount, created_at
                FROM test.test_ora_pg_custom_query_agg
                WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
                  AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
            """,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-03'},
            target_query="""
                SELECT amount, created_at
                FROM test.test_ora_pg_custom_query_agg
                WHERE created_at >= date_trunc('day', cast(:start_date as date))
                  AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 day'
            """,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-03'},
            max_columns=['created_at'],
            sum_columns=['amount'],
            include_count=True,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0

    def test_sum_and_count_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_custom_queries_agg(
            source_query="""
                SELECT amount
                FROM test.test_ora_pg_custom_query_agg
            """,
            target_query="""
                SELECT amount
                FROM test.test_ora_pg_custom_query_agg
                WHERE id < 3
            """,
            sum_columns=['amount'],
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'sum_amount', 'cnt'})

    def test_max_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_custom_queries_agg(
            source_query="""
                SELECT created_at
                FROM test.test_ora_pg_custom_query_agg
            """,
            target_query="""
                SELECT created_at
                FROM test.test_ora_pg_custom_query_agg
                WHERE created_at < DATE '2024-01-03'
            """,
            max_columns=['created_at'],
        )

        self._assert_failed(result, expected_columns={'max_created_at'})

    def test_count_only_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_custom_queries_agg(
            source_query="""
                SELECT id
                FROM test.test_ora_pg_custom_query_agg
            """,
            target_query="""
                SELECT id
                FROM test.test_ora_pg_custom_query_agg
                WHERE id < 3
            """,
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'cnt'})

    def test_max_and_sum_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        result = checker.check_custom_queries_agg(
            source_query="""
                SELECT amount, created_at
                FROM test.test_ora_pg_custom_query_agg
            """,
            target_query="""
                SELECT amount, created_at
                FROM test.test_ora_pg_custom_query_agg
                WHERE id < 3
            """,
            max_columns=['created_at'],
            sum_columns=['amount'],
        )

        self._assert_failed(result, expected_columns={'max_created_at', 'sum_amount'})

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
