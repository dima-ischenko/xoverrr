"""PostgreSQL hash-sample checks: types, composite keys, positive and negative."""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference

ROW_COUNT = 100_000
HASH_PCT = 2


class TestPostgresHashSample:
    @pytest.fixture(autouse=True)
    def setup_hash_data(self, postgres_engine, table_helper):
        source_table = 'test_pg_hash_sample_src'
        target_table = 'test_pg_hash_sample_trg'
        columns = """
            id          INTEGER PRIMARY KEY,
            code        TEXT NOT NULL,
            org_id      INTEGER NOT NULL,
            item_id     INTEGER NOT NULL,
            amount      INTEGER NOT NULL,
            created_at  DATE NOT NULL,
            updated_at  TIMESTAMP NOT NULL,
            is_active   BOOLEAN NOT NULL
        """
        source_insert = f"""
            INSERT INTO {source_table}
                (id, code, org_id, item_id, amount, created_at, updated_at, is_active)
            SELECT
                g,
                'c-' || g,
                ((g - 1) / 5) + 1,
                ((g - 1) % 5) + 1,
                g * 10,
                DATE '2024-01-01' + (g - 1),
                TIMESTAMP '2024-01-01 10:00:00' + (g - 1) * INTERVAL '1 hour',
                (g % 2 = 0)
            FROM generate_series(1, {ROW_COUNT}) AS g
        """
        target_insert = source_insert.replace(source_table, target_table)
        table_helper.create_table(
            engine=postgres_engine,
            table_name=source_table,
            create_sql=f'CREATE TABLE {source_table} ({columns})',
            insert_sql=source_insert,
        )
        table_helper.create_table(
            engine=postgres_engine,
            table_name=target_table,
            create_sql=f'CREATE TABLE {target_table} ({columns})',
            insert_sql=target_insert,
        )
        yield

    def _checker(self, engine):
        return DataQualityChecker(
            engine, engine, timezone='UTC', default_exclude_recent_hours=None
        )

    def _refs(self):
        return (
            DataReference('test_pg_hash_sample_src', 'test'),
            DataReference('test_pg_hash_sample_trg', 'test'),
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
            ['updated_at'],
            ['org_id', 'item_id'],
            ['is_active', 'id'],
        ],
    )
    def test_samples_match_for_key_types(self, postgres_engine, key):
        source, target = self._refs()
        result = self._checker(postgres_engine).check_samples(
            source_table=source,
            target_table=target,
            custom_primary_key=key,
            hash_pct=HASH_PCT,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0
        self._assert_sampled(result)

    def test_samples_mismatch(self, postgres_engine, table_helper):
        table_helper.create_table(
            engine=postgres_engine,
            table_name='test_pg_hash_sample_bad',
            create_sql="""
                CREATE TABLE test_pg_hash_sample_bad (
                    id INTEGER PRIMARY KEY,
                    amount INTEGER NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO test_pg_hash_sample_bad (id, amount)
                SELECT g, g * 10 + 1 FROM generate_series(1, {ROW_COUNT}) AS g
            """,
        )
        result = self._checker(postgres_engine).check_samples(
            source_table=DataReference('test_pg_hash_sample_src', 'test'),
            target_table=DataReference('test_pg_hash_sample_bad', 'test'),
            custom_primary_key=['id'],
            include_columns=['id', 'amount'],
            hash_pct=HASH_PCT,
        )

        assert result.status == CHECK_FAILED
        assert result.stats.final_score == pytest.approx(
            100 - result.stats.final_diff_score
        )
        self._assert_sampled(result)
        assert 'amount' in set(result.details.issue_examples['column_name'])

    def test_custom_queries_match_and_mismatch(self, postgres_engine):
        checker = self._checker(postgres_engine)
        match = checker.check_custom_queries(
            source_query="""
                SELECT id, code, amount FROM test.test_pg_hash_sample_src
            """,
            target_query="""
                SELECT id, code, amount FROM test.test_pg_hash_sample_trg
            """,
            custom_primary_key=['id', 'code'],
            hash_pct=HASH_PCT,
        )
        mismatch = checker.check_custom_queries(
            source_query="""
                SELECT id, amount FROM test.test_pg_hash_sample_src
            """,
            target_query="""
                SELECT id, amount + 1 AS amount FROM test.test_pg_hash_sample_trg
            """,
            custom_primary_key=['id'],
            hash_pct=HASH_PCT,
        )

        assert match.status == CHECK_SUCCESS
        assert match.stats.final_diff_score == 0
        self._assert_sampled(match)
        assert mismatch.status == CHECK_FAILED
        assert 'amount' in set(mismatch.details.issue_examples['column_name'])

    def test_aggregates_match_and_mismatch(self, postgres_engine):
        checker = self._checker(postgres_engine)
        query = 'SELECT id, amount, created_at FROM test.test_pg_hash_sample_src'
        match = checker.check_aggregates(
            source=query,
            target=query.replace('_src', '_trg'),
            max_columns=['created_at'],
            sum_columns=['amount'],
            include_count=True,
            hash_columns=['id'],
            hash_pct=HASH_PCT,
        )
        mismatch = checker.check_aggregates(
            source=query,
            target="""
                SELECT id, amount + 5 AS amount, created_at
                FROM test.test_pg_hash_sample_trg
            """,
            max_columns=['created_at'],
            sum_columns=['amount'],
            hash_columns=['id', 'created_at'],
            hash_pct=HASH_PCT,
        )

        assert match.status == CHECK_SUCCESS
        assert match.stats.final_score == 100
        assert mismatch.status == CHECK_FAILED
        assert mismatch.stats.final_score == 0
        assert 'sum_amount' in set(mismatch.details.issue_examples['column_name'])
