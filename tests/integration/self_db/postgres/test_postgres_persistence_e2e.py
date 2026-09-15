import pytest
from sqlalchemy import text

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


SRC_TABLE = 'test_persist_postgres_src'
TRG_TABLE = 'test_persist_postgres_trg'
RESULTS_TABLE_SAMPLE = 'test_persist_postgres_results'
RESULTS_TABLE_COUNTS = 'test_persist_postgres_results_counts'
RESULTS_TABLE_CUSTOM = 'test_persist_postgres_results_custom'
RESULTS_TABLE_FAILED = 'test_persist_postgres_results_failed'
RESULTS_TABLE_FAILED_COMPOUND = 'test_persist_postgres_results_failed_compound'


class TestPostgresPersistenceE2E:
    @pytest.fixture(autouse=True)
    def setup_postgres_data(self, postgres_engine, table_helper):
        for results_table in (
            RESULTS_TABLE_SAMPLE,
            RESULTS_TABLE_COUNTS,
            RESULTS_TABLE_CUSTOM,
            RESULTS_TABLE_FAILED,
            RESULTS_TABLE_FAILED_COMPOUND,
        ):
            table_helper.drop_table(postgres_engine, results_table)

        create_sql = """
            CREATE TABLE {table_name} (
                id          INTEGER PRIMARY KEY,
                value       TEXT NOT NULL,
                created_at  DATE NOT NULL
            )
        """
        insert_sql = """
            INSERT INTO {table_name} (id, value, created_at) VALUES
            (1, 'A', '2024-01-01'),
            (2, 'B', '2024-01-02'),
            (3, 'C', '2024-01-03')
        """

        table_helper.create_table(
            engine=postgres_engine,
            table_name=SRC_TABLE,
            create_sql=create_sql.format(table_name=SRC_TABLE),
            insert_sql=insert_sql.format(table_name=SRC_TABLE),
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=TRG_TABLE,
            create_sql=create_sql.format(table_name=TRG_TABLE),
            insert_sql=insert_sql.format(table_name=TRG_TABLE),
        )

        yield

    def _build_checker(self, postgres_engine):
        return DataQualityChecker(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            results_engine=postgres_engine,
            timezone='UTC',
        )

    def test_postgres_persistence_sample_e2e(self, postgres_engine):
        result = self._build_checker(postgres_engine).check_samples(
            source_table=DataReference(SRC_TABLE, 'test'),
            target_table=DataReference(TRG_TABLE, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-03'),
            custom_primary_key=['id'],
            tolerance_pct=0.0,
            persist_result=DataReference(RESULTS_TABLE_SAMPLE),
            report_output_format='json',
        )
        status = result.status

        assert status == CHECK_SUCCESS

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT check_type, status, report
                    FROM {RESULTS_TABLE_SAMPLE}
                    """
                )
            ).fetchone()

        assert row[:2] == ('samples', CHECK_SUCCESS)
        assert 'SAMPLES CHECK REPORT' in row[2]

    def test_postgres_persistence_counts_e2e(self, postgres_engine):
        result = self._build_checker(postgres_engine).check_counts(
            source_table=DataReference(SRC_TABLE, 'test'),
            target_table=DataReference(TRG_TABLE, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_pct=0.0,
            persist_result=DataReference(RESULTS_TABLE_COUNTS),
            report_output_format='json',
        )
        status = result.status

        assert status == CHECK_SUCCESS

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT check_type, status, report
                    FROM {RESULTS_TABLE_COUNTS}
                    """
                )
            ).fetchone()

        assert row[:2] == ('counts', CHECK_SUCCESS)
        assert 'COUNTS CHECK REPORT' in row[2]

    def test_postgres_persistence_custom_query_e2e(self, postgres_engine):
        source_query = f"""
            SELECT id, value, created_at
            FROM test.{SRC_TABLE}
            WHERE created_at >= cast(:start_date as date)
              AND created_at < cast(:end_date as date)
        """
        target_query = f"""
            SELECT id, value, created_at
            FROM test.{TRG_TABLE}
            WHERE created_at >= cast(:start_date as date)
              AND created_at < cast(:end_date as date)
        """
        query_params = {'start_date': '2024-01-01', 'end_date': '2024-01-04'}

        result = self._build_checker(postgres_engine).check_custom_queries(
            source_query=source_query,
            source_params=query_params,
            target_query=target_query,
            target_params=query_params,
            custom_primary_key=['id'],
            tolerance_pct=0.0,
            persist_result=DataReference(RESULTS_TABLE_CUSTOM),
            report_output_format='json',
        )
        status = result.status

        assert status == CHECK_SUCCESS

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT check_type, status
                    FROM {RESULTS_TABLE_CUSTOM}
                    """
                )
            ).fetchone()

        assert row[:2] == ('custom_queries', CHECK_SUCCESS)

    def test_postgres_persistence_failed_e2e(self, postgres_engine, table_helper):
        failed_src = 'test_persist_postgres_failed_src'
        failed_trg = 'test_persist_postgres_failed_trg'

        create_sql = """
            CREATE TABLE {table_name} (
                id          INTEGER,
                value       TEXT NOT NULL,
                created_at  DATE NOT NULL
            )
        """
        table_helper.create_table(
            engine=postgres_engine,
            table_name=failed_src,
            create_sql=create_sql.format(table_name=failed_src),
            insert_sql=f"""
                INSERT INTO {failed_src} (id, value, created_at) VALUES
                (1, 'A', '2024-01-01'),
                (2, 'B', '2024-01-02'),
                (2, 'B-dup', '2024-01-02'),
                (4, 'D', '2024-01-03'),
                (5, 'E', '2024-01-03'),
                (6, 'F', '2024-01-03')
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=failed_trg,
            create_sql=create_sql.format(table_name=failed_trg),
            insert_sql=f"""
                INSERT INTO {failed_trg} (id, value, created_at) VALUES
                (1, 'X', '2024-01-01'),
                (2, 'B', '2024-01-02'),
                (3, 'C', '2024-01-03'),
                (3, 'C-dup', '2024-01-03')
            """,
        )

        result = self._build_checker(postgres_engine).check_samples(
            source_table=DataReference(failed_src, 'test'),
            target_table=DataReference(failed_trg, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            custom_primary_key=['id'],
            tolerance_pct=0.0,
            persist_result=DataReference(RESULTS_TABLE_FAILED),
            report_output_format='text',
        )
        status = result.status
        report = result.report

        assert status == CHECK_FAILED

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT check_type, status, report
                    FROM {RESULTS_TABLE_FAILED}
                    """
                )
            ).fetchone()

        assert row[:2] == ('samples', CHECK_FAILED)
        assert report == row[2]

    def test_postgres_persistence_failed_compound_pk_e2e(
        self, postgres_engine, table_helper
    ):
        failed_src = 'test_persist_postgres_failed_compound_src'
        failed_trg = 'test_persist_postgres_failed_compound_trg'

        create_sql = """
            CREATE TABLE {table_name} (
                user_id     INTEGER,
                session_id  TEXT NOT NULL,
                value       TEXT NOT NULL,
                created_at  DATE NOT NULL
            )
        """
        table_helper.create_table(
            engine=postgres_engine,
            table_name=failed_src,
            create_sql=create_sql.format(table_name=failed_src),
            insert_sql=f"""
                INSERT INTO {failed_src} (user_id, session_id, value, created_at) VALUES
                (1, 'A', 'alpha', '2024-01-01'),
                (1, 'B', 'beta',  '2024-01-01'),
                (2, 'A', 'gamma', '2024-01-02'),
                (3, 'A', 'delta', '2024-01-02'),
                (4, 'A', 'eps',   '2024-01-03'),
                (5, 'A', 'zeta',  '2024-01-03')
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=failed_trg,
            create_sql=create_sql.format(table_name=failed_trg),
            insert_sql=f"""
                INSERT INTO {failed_trg} (user_id, session_id, value, created_at) VALUES
                (1, 'A', 'alpha-x', '2024-01-01'),
                (2, 'A', 'gamma-x', '2024-01-02'),
                (3, 'A', 'delta',   '2024-01-02'),
                (5, 'B', 'eta',     '2024-01-03'),
                (6, 'A', 'theta',   '2024-01-03')
            """,
        )

        result = self._build_checker(postgres_engine).check_samples(
            source_table=DataReference(failed_src, 'test'),
            target_table=DataReference(failed_trg, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            custom_primary_key=['user_id', 'session_id'],
            tolerance_pct=0.0,
            persist_result=DataReference(RESULTS_TABLE_FAILED_COMPOUND),
            report_output_format='text',
        )
        status = result.status
        report = result.report

        assert status == CHECK_FAILED

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT check_type, status, report
                    FROM {RESULTS_TABLE_FAILED_COMPOUND}
                    """
                )
            ).fetchone()

        assert row[:2] == ('samples', CHECK_FAILED)
        assert report == row[2]
