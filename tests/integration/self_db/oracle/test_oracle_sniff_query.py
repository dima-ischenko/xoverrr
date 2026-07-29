"""Oracle check_sniff_query integration tests."""

import pytest

from xoverrr.constants import (
    CHECK_FAILED,
    CHECK_SUCCESS,
    FLAG_VALUE_NO,
    FLAG_VALUE_YES,
    XSNIFF_PASSED_COLUMN,
)
from xoverrr.core import DataQualityChecker


TABLE_NAME = 'test_sniff_query_data'


@pytest.fixture
def checker(oracle_engine):
    return DataQualityChecker(
        source_engine=oracle_engine,
        timezone='UTC',
    )


@pytest.fixture
def setup_sniff_data(oracle_engine, table_helper):
    table_helper.create_table(
        engine=oracle_engine,
        table_name=TABLE_NAME,
        create_sql=f"""
            CREATE TABLE {TABLE_NAME} (
                id NUMBER PRIMARY KEY,
                amount NUMBER(10, 2) NOT NULL
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
def setup_sniff_data_with_issue(oracle_engine, table_helper):
    table_helper.create_table(
        engine=oracle_engine,
        table_name=TABLE_NAME,
        create_sql=f"""
            CREATE TABLE {TABLE_NAME} (
                id NUMBER PRIMARY KEY,
                amount NUMBER(10, 2) NOT NULL
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


class TestOracleSniffQuery:
    def test_row_level_pass(self, checker, setup_sniff_data):
        status, _, stats, _ = checker.check_sniff_query(
            source_query=f"""
                SELECT id, amount,
                    CASE WHEN amount < 0 THEN '{FLAG_VALUE_NO}' ELSE '{FLAG_VALUE_YES}' END
                    AS {XSNIFF_PASSED_COLUMN}
                FROM test.{TABLE_NAME}
            """,
            tolerance_pct=0.0,
        )

        assert status == CHECK_SUCCESS
        assert stats.final_score == 100.0

    def test_row_level_fail(self, checker, setup_sniff_data_with_issue):
        status, _, stats, _ = checker.check_sniff_query(
            source_query=f"""
                SELECT id, amount,
                    CASE WHEN amount < 0 THEN '{FLAG_VALUE_NO}' ELSE '{FLAG_VALUE_YES}' END
                    AS {XSNIFF_PASSED_COLUMN}
                FROM test.{TABLE_NAME}
            """,
            tolerance_pct=0.0,
        )

        assert status == CHECK_FAILED
        assert stats.final_score < 100.0

    def test_pass_fail_pass(self, checker, setup_sniff_data):
        status, _, stats, _ = checker.check_sniff_query(
            source_query=f"""
                SELECT CASE
                    WHEN SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) > 0
                    THEN '{FLAG_VALUE_NO}' ELSE '{FLAG_VALUE_YES}' END AS {XSNIFF_PASSED_COLUMN}
                FROM test.{TABLE_NAME}
            """,
            tolerance_pct=0.0,
        )

        assert status == CHECK_SUCCESS
        assert stats.final_score == 100.0

    def test_pass_fail_fail(self, checker, setup_sniff_data_with_issue):
        status, _, stats, _ = checker.check_sniff_query(
            source_query=f"""
                SELECT CASE
                    WHEN SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) > 0
                    THEN '{FLAG_VALUE_NO}' ELSE '{FLAG_VALUE_YES}' END AS {XSNIFF_PASSED_COLUMN}
                FROM test.{TABLE_NAME}
            """,
            tolerance_pct=0.0,
        )

        assert status == CHECK_FAILED
        assert stats.final_score == 0.0

    def test_issues_only_filter_pass(self, checker, setup_sniff_data):
        status, _, stats, details = checker.check_sniff_query(
            source_query=f"""
                SELECT id, amount, '{FLAG_VALUE_NO}' AS {XSNIFF_PASSED_COLUMN}
                FROM test.{TABLE_NAME}
                WHERE amount < 0
            """,
            tolerance_pct=0.0,
        )

        assert status == CHECK_SUCCESS
        assert stats.total_source_rows == 0
        assert stats.final_score == 100.0
        assert details.issue_row_examples.empty

    def test_issues_only_filter_fail(self, checker, setup_sniff_data_with_issue):
        status, _, stats, details = checker.check_sniff_query(
            source_query=f"""
                SELECT id, amount, '{FLAG_VALUE_NO}' AS {XSNIFF_PASSED_COLUMN}
                FROM test.{TABLE_NAME}
                WHERE amount < 0
            """,
            tolerance_pct=0.0,
        )

        assert status == CHECK_FAILED
        assert stats.total_source_rows == 1
        assert stats.passed_rows == 0
        assert stats.issue_rows_pct == 100.0
        assert stats.final_score == 0.0
        assert len(details.issue_row_examples) == 1
        assert int(details.issue_row_examples.iloc[0]['id']) == 2
