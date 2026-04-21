import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


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

    def test_oracle_persistence_e2e(self, oracle_engine, tmp_path):
        src_table = 'test_persist_oracle_src'
        trg_table = 'test_persist_oracle_trg'
        results_table = 'test_persist_oracle_results'

        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            results_engine=oracle_engine,
            timezone='UTC',
        )

        report_path = tmp_path / 'oracle_comparison_report.json'

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(src_table, 'test'),
            target_table=DataReference(trg_table, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-03'),
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
            persist_result=DataReference(results_table),
            comparison_name='oracle_orders_daily',
            comparison_tags={'adapter': 'oracle', 'kind': 'self_db'},
            report_output_path=str(report_path),
            report_output_format='json',
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        assert details is not None
        assert report_path.exists()

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
                        details_common_attribute_columns_json
                    FROM {results_table}
                    """
                )
            ).fetchone()

        assert row is not None
        assert row[0] == COMPARISON_SUCCESS
        assert row[1] is not None and 'DATA SAMPLE COMPARISON REPORT' in row[1]
        assert int(row[2]) == 3
        assert int(row[3]) == 3
        assert float(row[4]) == pytest.approx(100.0, rel=1e-6)
        assert row[5] is not None and 'value' in row[5]
