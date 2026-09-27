"""Test hash sampling between Oracle and PostgreSQL."""

import pytest

from xoverrr.constants import CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference


class TestOraclePostgresHashSampling:
    """Hash sampling between Oracle and PostgreSQL."""

    @pytest.fixture(autouse=True)
    def setup_hash_data(self, oracle_engine, postgres_engine, table_helper):
        table_name = 'test_ora_pg_hash'

        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    event_date DATE NOT NULL,
                    value VARCHAR2(50) NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, value)
                SELECT LEVEL,
                       DATE '2024-01-01' + MOD(LEVEL, 30),
                       'value_' || LEVEL
                FROM dual
                CONNECT BY LEVEL <= 1000
            """,
        )

        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    event_date DATE NOT NULL,
                    value TEXT NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, value)
                SELECT id,
                       DATE '2024-01-01' + (id % 30)::integer,
                       'value_' || id
                FROM generate_series(1, 1000) AS rows(id)
            """,
        )

        yield

    @pytest.mark.parametrize(
        ('date_column', 'date_chunk', 'date_range'),
        [(None, None, None), ('event_date', 1, ('2024-01-01', '2024-01-10'))],
    )
    def test_sample_hash_pct(
        self, oracle_engine, postgres_engine, date_column, date_chunk, date_range
    ):
        checker = DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        result = checker.check_samples(
            source_table=DataReference('test_ora_pg_hash', 'test'),
            target_table=DataReference('test_ora_pg_hash', 'test'),
            date_column=date_column,
            chunk_size_days=date_chunk,
            date_range = date_range,
            custom_primary_key= ['id'],
            hash_pct=1,
        )
        print(result.report)
        assert result.status == CHECK_SUCCESS
        assert result.stats.total_source_rows == result.stats.total_target_rows
        assert 0 < result.stats.total_source_rows < 1000
        assert result.stats.final_diff_score == 0.0
