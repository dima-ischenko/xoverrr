import pytest
from sqlalchemy import text

from xoverrr.constants import CHECK_SUCCESS, CHECK_TYPE_COUNTS_GROUP_BY_DATE
from xoverrr.core import DataQualityChecker, DataReference

SRC_TABLE = 'test_persist_oracle_src'
TRG_TABLE = 'test_persist_oracle_trg'
RESULTS_TABLE_SAMPLE = 'test_persist_oracle_results'
RESULTS_TABLE_COUNTS = 'test_persist_oracle_results_counts'
RESULTS_TABLE_CUSTOM = 'test_persist_oracle_custom_results'


class TestOraclePersistenceE2E:
    @pytest.fixture(autouse=True)
    def setup_oracle_data(self, oracle_engine, table_helper):
        for results_table in (
            RESULTS_TABLE_SAMPLE,
            RESULTS_TABLE_COUNTS,
            RESULTS_TABLE_CUSTOM,
        ):
            table_helper.drop_table(oracle_engine, results_table)

        create_sql = """
            CREATE TABLE {table_name} (
                id          NUMBER PRIMARY KEY,
                value       VARCHAR2(100) NOT NULL,
                created_at  DATE NOT NULL
            )
        """
        insert_sql = """
            INSERT INTO {table_name} (id, value, created_at) VALUES
            (1, 'A', DATE '2024-01-01'),
            (2, 'B', DATE '2024-01-02'),
            (3, 'C', DATE '2024-01-03')
        """

        table_helper.create_table(
            engine=oracle_engine,
            table_name=SRC_TABLE,
            create_sql=create_sql.format(table_name=SRC_TABLE),
            insert_sql=insert_sql.format(table_name=SRC_TABLE),
        )
        table_helper.create_table(
            engine=oracle_engine,
            table_name=TRG_TABLE,
            create_sql=create_sql.format(table_name=TRG_TABLE),
            insert_sql=insert_sql.format(table_name=TRG_TABLE),
        )

        yield

    def _build_checker(self, oracle_engine):
        return DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            results_engine=oracle_engine,
            timezone='UTC',
        )

    def test_oracle_persistence_sample_e2e(self, oracle_engine):
        result = self._build_checker(oracle_engine).check_samples(
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

        with oracle_engine.begin() as conn:
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

    def test_oracle_persistence_counts_e2e(self, oracle_engine):
        result = self._build_checker(oracle_engine).check_counts_group_by_date(
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

        with oracle_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT check_type, status, report
                    FROM {RESULTS_TABLE_COUNTS}
                    """
                )
            ).fetchone()

        assert row[:2] == (CHECK_TYPE_COUNTS_GROUP_BY_DATE, CHECK_SUCCESS)
        assert 'COUNTS GROUP BY DATE CHECK REPORT' in row[2]

    def test_oracle_persistence_custom_query_e2e(self, oracle_engine):
        source_query = f"""
            SELECT id, value, created_at
            FROM test.{SRC_TABLE}
            WHERE created_at >= date'2024-01-01'
              AND created_at < date'2024-01-04'
              -- {'A' * 5000}
        """
        target_query = f"""
            SELECT id, value, created_at
            FROM test.{TRG_TABLE}
            WHERE created_at >= date'2024-01-01'
              AND created_at < date'2024-01-04'
              -- {'A' * 5000}
        """

        result = self._build_checker(oracle_engine).check_custom_queries(
            source_query=source_query,
            source_params=None,
            target_query=target_query,
            target_params=None,
            custom_primary_key=['id'],
            tolerance_pct=0.0,
            persist_result=DataReference(RESULTS_TABLE_CUSTOM),
            report_output_format='json',
        )
        status = result.status

        assert status == CHECK_SUCCESS

        with oracle_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT check_type, status
                    FROM {RESULTS_TABLE_CUSTOM}
                    """
                )
            ).fetchone()

        assert row[:2] == ('custom_queries', CHECK_SUCCESS)
