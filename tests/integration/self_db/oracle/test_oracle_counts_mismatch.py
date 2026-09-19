"""Oracle count mismatch: source has an extra day."""

import pytest

from xoverrr.constants import CHECK_FAILED
from xoverrr.core import DataQualityChecker, DataReference


class TestOracleCountMismatch:
    @pytest.fixture(autouse=True)
    def setup_mismatch_data(self, oracle_engine, table_helper):
        source_table = 'test_ora_counts_mm_src'
        target_table = 'test_ora_counts_mm_trg'

        table_helper.create_table(
            engine=oracle_engine,
            table_name=source_table,
            create_sql=f"""
                CREATE TABLE {source_table} (
                    id          NUMBER PRIMARY KEY,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {source_table} (id, created_at) VALUES
                (1, DATE '2024-01-01'),
                (2, DATE '2024-01-02'),
                (3, DATE '2024-01-03'),
                (4, DATE '2024-01-04')
            """,
        )
        table_helper.create_table(
            engine=oracle_engine,
            table_name=target_table,
            create_sql=f"""
                CREATE TABLE {target_table} (
                    id          NUMBER PRIMARY KEY,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {target_table} (id, created_at) VALUES
                (1, DATE '2024-01-01'),
                (2, DATE '2024-01-02'),
                (3, DATE '2024-01-03')
            """,
        )
        yield

    def test_total_counts_mismatch(self, oracle_engine):
        checker = DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            timezone='UTC',
        )
        source_ref = DataReference('test_ora_counts_mm_src', 'test')
        target_ref = DataReference('test_ora_counts_mm_trg', 'test')

        result = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_pct=0.0,
        )
        result_chunked = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            chunk_size_days=2,
            tolerance_pct=0.0,
        )

        assert result.status == CHECK_FAILED
        assert result.stats.total_source_rows == 4
        assert result.stats.total_target_rows == 3
        assert result.stats.final_diff_score == pytest.approx(25.0)
        assert result.stats.final_score == pytest.approx(75.0)
        assert result_chunked.status == CHECK_FAILED
        assert result_chunked.stats.total_source_rows == 4
        assert result_chunked.stats.total_target_rows == 3
        assert result_chunked.stats.final_diff_score == result.stats.final_diff_score
        assert 'chunks processed' in result_chunked.report

    def test_counts_group_by_date_mismatch(self, oracle_engine):
        checker = DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            timezone='UTC',
        )
        source_ref = DataReference('test_ora_counts_mm_src', 'test')
        target_ref = DataReference('test_ora_counts_mm_trg', 'test')

        result = checker.check_counts_group_by_date(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_pct=0.0,
        )
        result_chunked = checker.check_counts_group_by_date(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-04'),
            chunk_size_days=2,
            tolerance_pct=0.0,
        )

        assert result.status == CHECK_FAILED
        assert result.stats.final_diff_score == pytest.approx(25.0)
        assert result.stats.final_score == pytest.approx(75.0)
        assert 'Source total count: 4' in result.report
        assert 'Target total count: 3' in result.report
        assert result.details is not None
        assert result_chunked.status == CHECK_FAILED
        assert result_chunked.stats.final_diff_score == result.stats.final_diff_score
        assert 'Source total count: 4' in result_chunked.report
        assert 'Target total count: 3' in result_chunked.report
