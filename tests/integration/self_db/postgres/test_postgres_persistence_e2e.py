import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_FAILED, COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


RESULTS_TABLE_SAMPLE = 'test_persist_postgres_results'
RESULTS_TABLE_COUNTS = 'test_persist_postgres_results_counts'
RESULTS_TABLE_CUSTOM = 'test_persist_postgres_results_custom'
RESULTS_TABLE_FAILED = 'test_persist_postgres_results_failed'
RESULTS_TABLE_FAILED_COMPOUND = 'test_persist_postgres_results_failed_compound'


class TestPostgresPersistenceE2E:
    @pytest.fixture(autouse=True)
    def setup_postgres_data(self, postgres_engine, table_helper):
        src_table = 'test_persist_postgres_src'
        trg_table = 'test_persist_postgres_trg'

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
            table_name=src_table,
            create_sql=create_sql.format(table_name=src_table),
            insert_sql=insert_sql.format(table_name=src_table),
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=trg_table,
            create_sql=create_sql.format(table_name=trg_table),
            insert_sql=insert_sql.format(table_name=trg_table),
        )

        yield

    def _build_comparator(self, postgres_engine):
        return DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            results_engine=postgres_engine,
            timezone='UTC',
        )

    def test_postgres_persistence_sample_e2e(self, postgres_engine):
        src_table = 'test_persist_postgres_src'
        trg_table = 'test_persist_postgres_trg'

        status, _, _, _ = self._build_comparator(postgres_engine).compare_sample(
            source_table=DataReference(src_table, 'test'),
            target_table=DataReference(trg_table, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-03'),
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
            persist_result=DataReference(RESULTS_TABLE_SAMPLE),
            report_output_format='json',
        )

        assert status == COMPARISON_SUCCESS

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT comparison_type, status, report
                    FROM {RESULTS_TABLE_SAMPLE}
                    """
                )
            ).fetchone()

        assert row[0] == 'sample' and row[1] == COMPARISON_SUCCESS
        assert 'DATA SAMPLE COMPARISON REPORT' in row[2]

    def test_postgres_persistence_counts_e2e(self, postgres_engine):
        src_table = 'test_persist_postgres_src'
        trg_table = 'test_persist_postgres_trg'

        status, _, _, _ = self._build_comparator(postgres_engine).compare_counts(
            source_table=DataReference(src_table, 'test'),
            target_table=DataReference(trg_table, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_percentage=0.0,
            persist_result=DataReference(RESULTS_TABLE_COUNTS),
            report_output_format='json',
        )

        assert status == COMPARISON_SUCCESS

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT comparison_type, status, report
                    FROM {RESULTS_TABLE_COUNTS}
                    """
                )
            ).fetchone()

        assert row[:2] == ('count', COMPARISON_SUCCESS)
        assert 'COUNT COMPARISON REPORT' in row[2]

    def test_postgres_persistence_custom_query_e2e(self, postgres_engine):
        src_table = 'test_persist_postgres_src'
        trg_table = 'test_persist_postgres_trg'

        source_query = f"""
            SELECT id, value, created_at
            FROM test.{src_table}
            WHERE created_at >= cast(:start_date as date)
              AND created_at < cast(:end_date as date)
        """
        target_query = f"""
            SELECT id, value, created_at
            FROM test.{trg_table}
            WHERE created_at >= cast(:start_date as date)
              AND created_at < cast(:end_date as date)
        """
        query_params = {'start_date': '2024-01-01', 'end_date': '2024-01-04'}

        status, _, _, _ = self._build_comparator(postgres_engine).compare_custom_query(
            source_query=source_query,
            source_params=query_params,
            target_query=target_query,
            target_params=query_params,
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
            persist_result=DataReference(RESULTS_TABLE_CUSTOM),
            report_output_format='json',
        )

        assert status == COMPARISON_SUCCESS

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT comparison_type, status, source_query, target_query
                    FROM {RESULTS_TABLE_CUSTOM}
                    """
                )
            ).fetchone()

        assert row[:2] == ('custom_query', COMPARISON_SUCCESS)
        assert ':start_date' not in row[2] and "'2024-01-01'" in row[2]
        assert ':end_date' not in row[3] and "'2024-01-04'" in row[3]

    def test_postgres_persistence_failed_e2e(self, postgres_engine, table_helper):
        failed_src = 'test_persist_postgres_failed_src'
        failed_trg = 'test_persist_postgres_failed_trg'

        for table_name in (failed_src, failed_trg):
            table_helper.drop_table(postgres_engine, table_name)

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

        status, report, stats, details = self._build_comparator(postgres_engine).compare_sample(
            source_table=DataReference(failed_src, 'test'),
            target_table=DataReference(failed_trg, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
            persist_result=DataReference(RESULTS_TABLE_FAILED),
            report_output_format='text',
        )

        assert status == COMPARISON_FAILED
        assert stats.dup_source_rows > 0
        assert stats.dup_target_rows > 0
        assert stats.only_source_rows > 0
        assert stats.only_target_rows > 0
        assert stats.total_matched_rows < stats.common_pk_rows

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT
                        comparison_type,
                        status,
                        stats_dup_source_rows,
                        stats_only_source_rows,
                        stats_only_target_rows,
                        stats_total_diff_percentage_rows,
                        stats_final_diff_score,
                        stats_final_score,
                        report
                    FROM {RESULTS_TABLE_FAILED}
                    """
                )
            ).fetchone()

        assert row[:2] == ('sample', COMPARISON_FAILED)
        assert row[2] > 0 and row[3] > 0 and row[4] > 0
        assert f'Final discrepancies score: {row[6]:.5f}' in row[8]
        assert report == row[8]

    def test_postgres_persistence_failed_compound_pk_e2e(
        self, postgres_engine, table_helper
    ):
        failed_src = 'test_persist_postgres_failed_compound_src'
        failed_trg = 'test_persist_postgres_failed_compound_trg'

        for table_name in (failed_src, failed_trg):
            table_helper.drop_table(postgres_engine, table_name)

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

        status, report, stats, details = self._build_comparator(postgres_engine).compare_sample(
            source_table=DataReference(failed_src, 'test'),
            target_table=DataReference(failed_trg, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            custom_primary_key=['user_id', 'session_id'],
            tolerance_percentage=0.0,
            persist_result=DataReference(RESULTS_TABLE_FAILED_COMPOUND),
            report_output_format='text',
        )

        assert status == COMPARISON_FAILED
        assert stats.only_source_rows >= 2
        assert stats.only_target_rows >= 2
        assert stats.common_pk_rows == 3
        assert stats.total_matched_rows < stats.common_pk_rows
        assert len(details.source_only_keys_examples) >= 2
        assert len(details.target_only_keys_examples) >= 2

        with postgres_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT
                        comparison_type,
                        status,
                        stats_only_source_rows,
                        stats_only_target_rows,
                        stats_final_diff_score,
                        report
                    FROM {RESULTS_TABLE_FAILED_COMPOUND}
                    """
                )
            ).fetchone()

        assert row[:2] == ('sample', COMPARISON_FAILED)
        assert row[2] >= 2 and row[3] >= 2
        assert f'Source only rows %: {stats.source_only_percentage_rows:.5f}' in row[5]
        assert f'Target only rows %: {stats.target_only_percentage_rows:.5f}' in row[5]
        assert f'Final discrepancies score: {row[4]:.5f}' in row[5]
        assert report == row[5]

