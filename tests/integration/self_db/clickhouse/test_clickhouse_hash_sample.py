"""ClickHouse hash-sample checks: types, composite keys, positive and negative."""

import pytest

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference

ROW_COUNT = 100_000
HASH_PCT = 2


class TestClickHouseHashSample:
    @pytest.fixture(autouse=True)
    def setup_hash_data(self, clickhouse_engine, table_helper):
        source_table = 'test_ch_hash_sample_src'
        target_table = 'test_ch_hash_sample_trg'
        columns = """
            id          UInt32,
            code        String,
            org_id      UInt32,
            item_id     UInt32,
            amount      Int32,
            created_at  Date32,
            updated_at  DateTime,
            is_active   UInt8
        """
        insert = f"""
            INSERT INTO {{table}}
                (id, code, org_id, item_id, amount, created_at, updated_at, is_active)
            SELECT
                number + 1,
                concat('c-', toString(number + 1)),
                intDiv(number, 5) + 1,
                (number % 5) + 1,
                (number + 1) * 10,
                toDate32('2024-01-01') + number,
                toDateTime('2024-01-01 10:00:00') + toIntervalHour(number),
                (number + 1) % 2
            FROM numbers({ROW_COUNT})
        """
        for table_name in (source_table, target_table):
            table_helper.create_table(
                engine=clickhouse_engine,
                table_name=table_name,
                create_sql=f"""
                    CREATE TABLE {table_name} ({columns})
                    ENGINE = MergeTree()
                    ORDER BY id
                """,
                insert_sql=insert.format(table=table_name),
            )
        yield

    def _checker(self, engine):
        return DataQualityChecker(
            engine, engine, timezone='UTC', default_exclude_recent_hours=None
        )

    def _refs(self):
        return (
            DataReference('test_ch_hash_sample_src', 'test'),
            DataReference('test_ch_hash_sample_trg', 'test'),
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
    def test_samples_match_for_key_types(self, clickhouse_engine, key):
        source, target = self._refs()
        result = self._checker(clickhouse_engine).check_samples(
            source_table=source,
            target_table=target,
            custom_primary_key=key,
            hash_pct=HASH_PCT,
        )

        assert result.status == CHECK_SUCCESS
        assert result.stats.final_diff_score == 0
        self._assert_sampled(result)

    def test_samples_mismatch(self, clickhouse_engine):
        result = self._checker(clickhouse_engine).check_custom_queries(
            source_query='SELECT id, amount FROM test_ch_hash_sample_src',
            target_query="""
                SELECT id, amount + 1 AS amount FROM test_ch_hash_sample_trg
            """,
            custom_primary_key=['id'],
            hash_pct=HASH_PCT,
        )

        assert result.status == CHECK_FAILED
        assert 'amount' in set(result.details.issue_examples['column_name'])
        self._assert_sampled(result)

    def test_custom_queries_agg_match_and_mismatch(self, clickhouse_engine):
        checker = self._checker(clickhouse_engine)
        match = checker.check_custom_queries_agg(
            source_query='SELECT id, amount, created_at FROM test_ch_hash_sample_src',
            target_query='SELECT id, amount, created_at FROM test_ch_hash_sample_trg',
            max_columns=['created_at'],
            sum_columns=['amount'],
            include_count=True,
            hash_columns=['id'],
            hash_pct=HASH_PCT,
        )
        mismatch = checker.check_custom_queries_agg(
            source_query='SELECT id, amount FROM test_ch_hash_sample_src',
            target_query='SELECT id, amount + 5 AS amount FROM test_ch_hash_sample_trg',
            sum_columns=['amount'],
            hash_columns=['id'],
            hash_pct=HASH_PCT,
        )

        assert match.status == CHECK_SUCCESS
        assert match.stats.final_score == 100
        assert mismatch.status == CHECK_FAILED
        assert mismatch.stats.final_score == 0
        assert 'sum_amount' in set(mismatch.details.issue_examples['column_name'])
