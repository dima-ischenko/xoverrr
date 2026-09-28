"""Hash sampling must keep the same keys on Oracle and PostgreSQL."""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference

ROW_COUNT = 100_000
HASH_PCT = 2


class TestOraPgHashSample:
    @pytest.fixture(autouse=True)
    def setup_hash_data(self, oracle_engine, postgres_engine, table_helper):
        table_name = 'test_ora_pg_hash_sample'
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    code        VARCHAR2(20) NOT NULL,
                    org_id      INTEGER NOT NULL,
                    item_id     INTEGER NOT NULL,
                    amount      INTEGER NOT NULL,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name}
                    (id, code, org_id, item_id, amount, created_at)
                SELECT
                    LEVEL,
                    'c-' || LEVEL,
                    TRUNC((LEVEL - 1) / 5) + 1,
                    MOD(LEVEL - 1, 5) + 1,
                    LEVEL * 10,
                    DATE '2024-01-01' + (LEVEL - 1)
                FROM dual
                CONNECT BY LEVEL <= {ROW_COUNT}
            """,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    code        TEXT NOT NULL,
                    org_id      INTEGER NOT NULL,
                    item_id     INTEGER NOT NULL,
                    amount      INTEGER NOT NULL,
                    created_at  DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name}
                    (id, code, org_id, item_id, amount, created_at)
                SELECT
                    g,
                    'c-' || g,
                    ((g - 1) / 5) + 1,
                    ((g - 1) % 5) + 1,
                    g * 10,
                    DATE '2024-01-01' + (g - 1)
                FROM generate_series(1, {ROW_COUNT}) AS g
            """,
        )
        yield

    def _checker(self, oracle_engine, postgres_engine):
        return DataQualityChecker(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='UTC',
            default_exclude_recent_hours=None,
        )

    def _refs(self):
        return (
            DataReference('test_ora_pg_hash_sample', 'test'),
            DataReference('test_ora_pg_hash_sample', 'test'),
        )

    def _assert_sampled(self, result):
        sampled = result.stats.total_source_rows
        assert sampled == result.stats.total_target_rows
        lo = ROW_COUNT * HASH_PCT // 400
        hi = min(ROW_COUNT - 1, ROW_COUNT * HASH_PCT // 25)
        assert lo <= sampled <= hi

    @pytest.mark.parametrize(
        'key',
        [
            ['id'],
            ['code'],
            ['created_at'],
            ['org_id', 'item_id'],
        ],
    )
    def test_samples_match_same_keys(self, oracle_engine, postgres_engine, key):
        source, target = self._refs()
        result = self._checker(oracle_engine, postgres_engine).check_samples(
            source_table=source,
            target_table=target,
            custom_primary_key=key,
            hash_pct=HASH_PCT,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0
        self._assert_sampled(result)

    def test_samples_mismatch(self, oracle_engine, postgres_engine):
        result = self._checker(oracle_engine, postgres_engine).check_custom_queries(
            source_query="""
                SELECT id, amount FROM test.test_ora_pg_hash_sample
            """,
            target_query="""
                SELECT id, amount + 1 AS amount FROM test.test_ora_pg_hash_sample
            """,
            custom_primary_key=['id'],
            hash_pct=HASH_PCT,
        )

        assert result.status == CHECK_FAILED
        assert 'amount' in set(result.details.issue_examples['column_name'])
        self._assert_sampled(result)

    def test_aggregates_match_and_mismatch(self, oracle_engine, postgres_engine):
        checker = self._checker(oracle_engine, postgres_engine)
        match = checker.check_aggregates(
            source="""
                SELECT id, amount, created_at FROM test.test_ora_pg_hash_sample
            """,
            target="""
                SELECT id, amount, created_at FROM test.test_ora_pg_hash_sample
            """,
            max_columns=['created_at'],
            sum_columns=['amount'],
            include_count=True,
            hash_columns=['id'],
            hash_pct=HASH_PCT,
        )
        mismatch = checker.check_aggregates(
            source="""
                SELECT id, code, amount FROM test.test_ora_pg_hash_sample
            """,
            target="""
                SELECT id, code, amount + 5 AS amount
                FROM test.test_ora_pg_hash_sample
            """,
            sum_columns=['amount'],
            hash_columns=['id', 'code'],
            hash_pct=HASH_PCT,
        )

        assert match.status == CHECK_SUCCESS
        assert match.stats.final_score == 100
        assert mismatch.status == CHECK_FAILED
        assert mismatch.stats.final_score == 0
        assert 'sum_amount' in set(mismatch.details.issue_examples['column_name'])
