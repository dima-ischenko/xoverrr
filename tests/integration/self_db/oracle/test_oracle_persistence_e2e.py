import pytest
from sqlalchemy import text

from xoverrr.constants import CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


class TestOraclePersistenceE2E:
    @pytest.fixture(autouse=True)
    def setup_oracle_data(self, oracle_engine, table_helper):
        src_table = 'test_persist_oracle_src'
        trg_table = 'test_persist_oracle_trg'
        results_table = 'test_persist_oracle_results'
        

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
            table_name=src_table,
            create_sql=create_sql.format(table_name=src_table),
            insert_sql=insert_sql.format(table_name=src_table),
        )
        table_helper.create_table(
            engine=oracle_engine,
            table_name=trg_table,
            create_sql=create_sql.format(table_name=trg_table),
            insert_sql=insert_sql.format(table_name=trg_table),
        )

        yield

    def test_oracle_persistence_e2e(self, oracle_engine):
        src_table = 'test_persist_oracle_src'
        trg_table = 'test_persist_oracle_trg'
        results_table = 'test_persist_oracle_results'

        checker = DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            results_engine=oracle_engine,
            timezone='UTC',
        )

        status, report, stats, details = checker.check_samples(
            source_table=DataReference(src_table, 'test'),
            target_table=DataReference(trg_table, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-03'),
            custom_primary_key=['id'],
            tolerance_pct=0.0,
            persist_result=DataReference(results_table),
            check_tags={'adapter': 'oracle', 'kind': 'self_db'},
            report_output_format='json',
        )

        assert status == CHECK_SUCCESS
        assert stats.final_diff_score == 0.0
        assert details is not None
        assert '"check_type": "samples"' in report

        with oracle_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT
                        status,
                        report,
                        stats_total_source_rows,
                        stats_total_target_rows,
                        stats_final_score,
                        details_evaluated_columns_json
                    FROM {results_table}
                    """
                )
            ).fetchone()

            column_types = {
                name: data_type
                for name, data_type in conn.execute(
                    text(
                        """
                        SELECT column_name, data_type
                        FROM user_tab_columns
                        WHERE table_name = UPPER(:table_name)
                        """
                    ),
                    {'table_name': results_table},
                )
            }

        assert row is not None
        assert row[0] == CHECK_SUCCESS
        assert row[1] is not None and 'SAMPLES CHECK REPORT' in row[1]
        assert int(row[2]) == 3
        assert int(row[3]) == 3
        assert float(row[4]) == pytest.approx(100.0, rel=1e-6)
        assert row[5] is not None and 'value' in row[5]
        assert column_types.get('REPORT') == 'CLOB'
        assert column_types.get('SOURCE_QUERY') == 'VARCHAR2'
        assert column_types.get('CHECK_TAGS_JSON') == 'VARCHAR2'
        assert column_types.get('DETAILS_EVALUATED_COLUMNS_JSON') == 'VARCHAR2'

    def test_oracle_persistence_custom_query_e2e(self, oracle_engine):
        src_table = 'test_persist_oracle_src'
        trg_table = 'test_persist_oracle_trg'
        results_table = 'test_persist_oracle_custom_results'

        checker = DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            results_engine=oracle_engine,
            timezone='UTC',
        )

        source_query = f"""
            SELECT id, value, created_at
            FROM test.{src_table}
            WHERE created_at >= date'2024-01-01'
              AND created_at < date'2024-01-04'
              -- {'A' * 5000}
        """
        target_query = f"""
            SELECT id, value, created_at
            FROM test.{trg_table}
            WHERE created_at >= date'2024-01-01'
              AND created_at < date'2024-01-04'
              -- {'A' * 5000}
        """
        #query_params = {'start_date': '2024-01-01', 'end_date': '2024-01-04'}

        status, _, _, _ = checker.check_custom_queries(
            source_query=source_query,
            source_params=None,
            target_query=target_query,
            target_params=None,
            custom_primary_key=['id'],
            tolerance_pct=0.0,
            persist_result=DataReference(results_table),
            report_output_format='json',
        )

        assert status == CHECK_SUCCESS

        with oracle_engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT check_type, status, source_query, target_query
                    FROM {results_table}
                    """
                )
            ).fetchone()

        assert row[:2] == ('custom_queries', CHECK_SUCCESS)        
