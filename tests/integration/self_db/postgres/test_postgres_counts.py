"""PostgreSQL whole-table count checks without date_column."""

import pytest
from sqlalchemy import text

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS, COUNTS_TOTAL_DT
from xoverrr.core import DataQualityChecker, DataReference

RESULTS_TABLE = 'test_counts_without_date_results'


class TestPostgresCountsWithoutDateColumn:
    @pytest.fixture(autouse=True)
    def setup_count_data(self, postgres_engine, table_helper):
        table_helper.create_table(
            engine=postgres_engine,
            table_name='test_counts_source',
            create_sql="""
                CREATE TABLE test_counts_source (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL
                )
            """,
            insert_sql="""
                INSERT INTO test_counts_source (id, name) VALUES
                (1, 'Alice'),
                (2, 'Bob'),
                (3, 'Charlie'),
                (4, 'Diana')
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name='test_counts_target_same',
            create_sql="""
                CREATE TABLE test_counts_target_same (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL
                )
            """,
            insert_sql="""
                INSERT INTO test_counts_target_same (id, name) VALUES
                (1, 'Alice'),
                (2, 'Bobby'),
                (3, 'Charlie'),
                (4, 'Dina')
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name='test_counts_target_short',
            create_sql="""
                CREATE TABLE test_counts_target_short (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL
                )
            """,
            insert_sql="""
                INSERT INTO test_counts_target_short (id, name) VALUES
                (1, 'Alice'),
                (2, 'Bob'),
                (3, 'Charlie')
            """,
        )
        table_helper.drop_table(postgres_engine, RESULTS_TABLE)
        yield

    def test_counts_without_date_column_identical(self, postgres_engine):
        checker = DataQualityChecker(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone='UTC',
        )

        result = checker.check_counts(
            source_table=DataReference('test_counts_source', 'test'),
            target_table=DataReference('test_counts_target_same', 'test'),
            tolerance_pct=0.0,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0.0
        assert result.stats.final_score == 100.0
        assert COUNTS_TOTAL_DT in result.report
        assert 'Source total count: 4' in result.report
        assert 'Target total count: 4' in result.report

    def test_counts_without_date_column_mismatch(self, postgres_engine):
        checker = DataQualityChecker(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            results_engine=postgres_engine,
            timezone='UTC',
        )

        result = checker.check_counts(
            source_table=DataReference('test_counts_source', 'test'),
            target_table=DataReference('test_counts_target_short', 'test'),
            tolerance_pct=0.0,
            persist_result=DataReference(RESULTS_TABLE),
        )

        assert result.status == CHECK_FAILED
        assert 'Source total count: 4' in result.report
        assert 'Target total count: 3' in result.report
        assert 'Discrepancies %: 25.00000%' in result.report
        assert result.stats.final_diff_score == pytest.approx(25.0)
        assert result.stats.final_score == pytest.approx(75.0)

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT stats_final_score, stats_final_diff_score
                    FROM {RESULTS_TABLE}
                    """
                )
            ).fetchone()

        assert row[0] == pytest.approx(75.0)
        assert row[1] == pytest.approx(25.0)
