"""ClickHouse sniff_query integration tests."""

import pytest

from xoverrr.constants import (
    COMPARISON_FAILED,
    COMPARISON_SUCCESS,
    FLAG_VALUE_NO,
    FLAG_VALUE_YES,
    XSNIFF_PASSED_COLUMN,
)
from xoverrr.core import DataQualityComparator


TABLE_NAME = 'test_sniff_query_data'


@pytest.fixture
def comparator(clickhouse_engine):
    return DataQualityComparator(
        source_engine=clickhouse_engine,
        timezone='UTC',
    )


@pytest.fixture
def setup_sniff_data(clickhouse_engine, table_helper):
    table_helper.create_table(
        engine=clickhouse_engine,
        table_name=TABLE_NAME,
        create_sql=f"""
            CREATE TABLE {TABLE_NAME} (
                id UInt32,
                amount Decimal(10, 2)
            )
            ENGINE = MergeTree()
            ORDER BY id
        """,
        insert_sql=f"""
            INSERT INTO {TABLE_NAME} (id, amount) VALUES
            (1, 10.00),
            (2, 20.00),
            (3, 30.00)
        """,
    )
    yield


@pytest.fixture
def setup_sniff_data_with_issue(clickhouse_engine, table_helper):
    table_helper.create_table(
        engine=clickhouse_engine,
        table_name=TABLE_NAME,
        create_sql=f"""
            CREATE TABLE {TABLE_NAME} (
                id UInt32,
                amount Decimal(10, 2)
            )
            ENGINE = MergeTree()
            ORDER BY id
        """,
        insert_sql=f"""
            INSERT INTO {TABLE_NAME} (id, amount) VALUES
            (1, 10.00),
            (2, -5.00),
            (3, 30.00)
        """,
    )
    yield


class TestClickHouseSniffQuery:
    def test_row_level_pass(self, comparator, setup_sniff_data):
        status, _, stats, _ = comparator.sniff_query(
            source_query=f"""
                SELECT
                    id,
                    amount,
                    if(amount < 0, '{FLAG_VALUE_NO}', '{FLAG_VALUE_YES}') AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
            """,
            tolerance_pct=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0

    def test_row_level_fail(self, comparator, setup_sniff_data_with_issue):
        status, _, stats, _ = comparator.sniff_query(
            source_query=f"""
                SELECT
                    id,
                    amount,
                    if(amount < 0, '{FLAG_VALUE_NO}', '{FLAG_VALUE_YES}') AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
            """,
            tolerance_pct=0.0,
        )

        assert status == COMPARISON_FAILED
        assert stats.final_score < 100.0

    def test_pass_fail_pass(self, comparator, setup_sniff_data):
        status, _, stats, _ = comparator.sniff_query(
            source_query=f"""
                SELECT if(countIf(amount < 0) > 0, '{FLAG_VALUE_NO}', '{FLAG_VALUE_YES}')
                    AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
            """,
            tolerance_pct=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0

    def test_pass_fail_fail(self, comparator, setup_sniff_data_with_issue):
        status, _, stats, _ = comparator.sniff_query(
            source_query=f"""
                SELECT if(countIf(amount < 0) > 0, '{FLAG_VALUE_NO}', '{FLAG_VALUE_YES}')
                    AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
            """,
            tolerance_pct=0.0,
        )

        assert status == COMPARISON_FAILED
        assert stats.final_score == 0.0

    def test_issues_only_filter_pass(self, comparator, setup_sniff_data):
        status, _, stats, details = comparator.sniff_query(
            source_query=f"""
                SELECT id, amount, '{FLAG_VALUE_NO}' AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
                WHERE amount < 0
            """,
            tolerance_pct=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.total_source_rows == 0
        assert stats.final_score == 100.0
        assert details.issue_row_examples.empty

    def test_issues_only_filter_fail(self, comparator, setup_sniff_data_with_issue):
        status, _, stats, details = comparator.sniff_query(
            source_query=f"""
                SELECT id, amount, '{FLAG_VALUE_NO}' AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
                WHERE amount < 0
            """,
            tolerance_pct=0.0,
        )

        assert status == COMPARISON_FAILED
        assert stats.total_source_rows == 1
        assert stats.passed_rows == 0
        assert stats.issue_rows_pct == 100.0
        assert stats.final_score == 0.0
        assert len(details.issue_row_examples) == 1
        assert int(details.issue_row_examples.iloc[0]['id']) == 2
