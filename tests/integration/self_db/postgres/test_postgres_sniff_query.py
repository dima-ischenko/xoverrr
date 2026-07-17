"""PostgreSQL sniff_query integration tests."""

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
def comparator(postgres_engine):
    return DataQualityComparator(
        source_engine=postgres_engine,
        timezone='UTC',
    )


@pytest.fixture
def setup_sniff_data(postgres_engine, table_helper):
    table_helper.create_table(
        engine=postgres_engine,
        table_name=TABLE_NAME,
        create_sql=f"""
            CREATE TABLE {TABLE_NAME} (
                id INTEGER PRIMARY KEY,
                amount NUMERIC(10, 2) NOT NULL
            )
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
def setup_sniff_data_with_issue(postgres_engine, table_helper):
    table_helper.create_table(
        engine=postgres_engine,
        table_name=TABLE_NAME,
        create_sql=f"""
            CREATE TABLE {TABLE_NAME} (
                id INTEGER PRIMARY KEY,
                amount NUMERIC(10, 2) NOT NULL
            )
        """,
        insert_sql=f"""
            INSERT INTO {TABLE_NAME} (id, amount) VALUES
            (1, 10.00),
            (2, -5.00),
            (3, 30.00)
        """,
    )
    yield


class TestPostgresSniffQuery:
    def test_row_level_pass(self, comparator, setup_sniff_data):
        status, _, stats, _ = comparator.sniff_query(
            source_query=f"""
                SELECT id, amount,
                    CASE WHEN amount < 0 THEN '{FLAG_VALUE_NO}' ELSE '{FLAG_VALUE_YES}' END
                    AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
            """,
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0

    def test_row_level_fail(self, comparator, setup_sniff_data_with_issue):
        status, _, stats, _ = comparator.sniff_query(
            source_query=f"""
                SELECT id, amount,
                    CASE WHEN amount < 0 THEN '{FLAG_VALUE_NO}' ELSE '{FLAG_VALUE_YES}' END
                    AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
            """,
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_FAILED
        assert stats.final_score < 100.0

    def test_pass_fail_pass(self, comparator, setup_sniff_data):
        status, _, stats, _ = comparator.sniff_query(
            source_query=f"""
                SELECT CASE
                    WHEN SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) > 0
                    THEN '{FLAG_VALUE_NO}' ELSE '{FLAG_VALUE_YES}' END AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
            """,
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0

    def test_pass_fail_fail(self, comparator, setup_sniff_data_with_issue):
        status, _, stats, _ = comparator.sniff_query(
            source_query=f"""
                SELECT CASE
                    WHEN SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) > 0
                    THEN '{FLAG_VALUE_NO}' ELSE '{FLAG_VALUE_YES}' END AS {XSNIFF_PASSED_COLUMN}
                FROM {TABLE_NAME}
            """,
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_FAILED
        assert stats.final_score == 0.0
