"""ClickHouse custom-query aggregate checks."""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker


class TestClickHouseCustomQueryAgg:
    @pytest.fixture(autouse=True)
    def setup_agg_data(self, clickhouse_engine, table_helper):
        source_table = 'test_ch_custom_query_agg_src'
        target_table = 'test_ch_custom_query_agg_trg'

        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=source_table,
            create_sql=f"""
                CREATE TABLE {source_table} (
                    id          UInt32,
                    amount      Float64,
                    created_at  Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {source_table} (id, amount, created_at) VALUES
                (1, 10, '2024-01-01'),
                (2, 20, '2024-01-02'),
                (3, 999, '2024-01-03')
            """,
        )
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=target_table,
            create_sql=f"""
                CREATE TABLE {target_table} (
                    id          UInt32,
                    amount      Float64,
                    created_at  Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {target_table} (id, amount, created_at) VALUES
                (1, 10, '2024-01-01'),
                (2, 20, '2024-01-02'),
                (3, 30, '2024-01-03')
            """,
        )
        yield

    def test_aggregates_match(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        source_query = """
            SELECT id, amount, created_at
            FROM test_ch_custom_query_agg_src
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date)
              AND id < 3
        """
        target_query = """
            SELECT id, amount, created_at
            FROM test_ch_custom_query_agg_trg
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date)
              AND id < 3
        """
        params = {'start_date': '2024-01-01', 'end_date': '2024-01-04'}

        result = checker.check_custom_queries_agg(
            source_query=source_query,
            source_params=params,
            target_query=target_query,
            target_params=params,
            max_columns=['created_at'],
            sum_columns=['amount'],
            include_count=True,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0
        assert result.details.issue_examples.empty

    def test_sum_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_custom_queries_agg(
            source_query='SELECT amount FROM test_ch_custom_query_agg_src',
            target_query='SELECT amount FROM test_ch_custom_query_agg_trg',
            sum_columns=['amount'],
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'sum_amount'})

    def test_max_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_custom_queries_agg(
            source_query='SELECT created_at FROM test_ch_custom_query_agg_src',
            target_query="""
                SELECT created_at
                FROM test_ch_custom_query_agg_trg
                WHERE created_at < toDate('2024-01-03')
            """,
            max_columns=['created_at'],
        )

        self._assert_failed(result, expected_columns={'max_created_at'})

    def test_count_only_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_custom_queries_agg(
            source_query='SELECT id FROM test_ch_custom_query_agg_src',
            target_query='SELECT id FROM test_ch_custom_query_agg_trg WHERE id < 3',
            include_count=True,
        )

        self._assert_failed(result, expected_columns={'cnt'})

    def test_max_and_sum_mismatch(self, clickhouse_engine):
        checker = DataQualityChecker(
            clickhouse_engine, clickhouse_engine, timezone='UTC'
        )
        result = checker.check_custom_queries_agg(
            source_query="""
                SELECT amount, created_at
                FROM test_ch_custom_query_agg_src
            """,
            target_query="""
                SELECT amount, created_at
                FROM test_ch_custom_query_agg_trg
                WHERE id < 3
            """,
            max_columns=['created_at'],
            sum_columns=['amount'],
        )

        self._assert_failed(result, expected_columns={'max_created_at', 'sum_amount'})

    @staticmethod
    def _assert_failed(result, expected_columns):
        assert result.status == CHECK_FAILED
        assert result.stats.final_diff_score == 100
        assert result.stats.final_score == 0
        assert not result.details.issue_examples.empty
        columns = set(result.details.issue_examples['column_name'])
        assert expected_columns <= columns
