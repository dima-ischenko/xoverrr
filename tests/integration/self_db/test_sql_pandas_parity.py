"""Dual-run parity: pandas DataQualityChecker vs InDatabaseChecker stats."""

from dataclasses import fields

import pytest

from xoverrr.core import DataQualityChecker, DataReference, InDatabaseChecker
from xoverrr.utils import CheckStats


def assert_check_stats_equal(left: CheckStats, right: CheckStats) -> None:
    for item in fields(CheckStats):
        lv = getattr(left, item.name)
        rv = getattr(right, item.name)
        if isinstance(lv, float):
            assert lv == pytest.approx(rv, rel=1e-9, abs=1e-9), item.name
        else:
            assert lv == rv, item.name


def assert_issue_breakdown_equal(left, right) -> None:
    def as_map(details):
        if details.issue_breakdown is None or details.issue_breakdown.empty:
            return {}
        return {
            str(row.column_name): int(row.issue_count)
            for row in details.issue_breakdown.itertuples(index=False)
        }

    assert as_map(left) == as_map(right)


def _run_both(engine, source_ref, target_ref, **kwargs):
    pandas_checker = DataQualityChecker(engine, engine, timezone='UTC')
    sql_checker = InDatabaseChecker(engine, timezone='UTC')
    pandas_result = pandas_checker.check_samples(
        source_table=source_ref, target_table=target_ref, **kwargs
    )
    sql_result = sql_checker.check_samples(
        source_table=source_ref, target_table=target_ref, **kwargs
    )
    return pandas_result, sql_result


def test_postgres_sql_pandas_stats_parity(postgres_engine, table_helper):
    src = 'test_parity_src'
    trg = 'test_parity_trg'
    table_helper.create_table(
        engine=postgres_engine,
        table_name=src,
        create_sql=f"""
            CREATE TABLE {src} (
                id INTEGER NOT NULL,
                name TEXT,
                amount NUMERIC,
                created_at DATE NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """,
        insert_sql=f"""
            INSERT INTO {src} (id, name, amount, created_at, updated_at) VALUES
            (1, 'Alice', 1.0, '2024-01-01', '2024-01-01 10:00:00'),
            (1, 'Alice', 1.0, '2024-01-01', '2024-01-01 10:00:00'),
            (2, 'Bob', 2, '2024-01-02', '2024-01-02 10:00:00'),
            (3, 'Charlie', 3, '2024-01-03', '2024-01-03 10:00:00'),
            (4, 'Dave', 4, '2024-01-04', '2024-01-04 10:00:00')
        """,
    )
    table_helper.create_table(
        engine=postgres_engine,
        table_name=trg,
        create_sql=f"""
            CREATE TABLE {trg} (
                id INTEGER NOT NULL,
                name TEXT,
                amount NUMERIC,
                created_at DATE NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """,
        insert_sql=f"""
            INSERT INTO {trg} (id, name, amount, created_at, updated_at) VALUES
            (1, 'Alice', 1, '2024-01-01', '2024-01-01 10:00:00'),
            (2, 'Bobby', 2.0, '2024-01-02', '2024-01-02 10:00:00'),
            (3, 'Charlie', 3, '2024-01-03', '2024-01-03 10:00:00'),
            (4, 'Dave', 4, '2024-01-04', '2024-01-04 10:00:00'),
            (5, 'Eve', 5, '2024-01-05', '2024-01-05 10:00:00')
        """,
    )

    pandas_result, sql_result = _run_both(
        postgres_engine,
        DataReference(src, 'test'),
        DataReference(trg, 'test'),
        date_column='created_at',
        update_column='updated_at',
        date_range=('2024-01-01', '2024-01-05'),
        custom_primary_key=['id'],
        tolerance_pct=100.0,
        max_examples=3,
    )
    assert_check_stats_equal(pandas_result.stats, sql_result.stats)
    assert_issue_breakdown_equal(pandas_result.details, sql_result.details)
    assert pandas_result.status == sql_result.status


def test_oracle_sql_pandas_stats_parity(oracle_engine, table_helper):
    src = 'test_parity_src'
    trg = 'test_parity_trg'
    table_helper.create_table(
        engine=oracle_engine,
        table_name=src,
        create_sql=f"""
            CREATE TABLE {src} (
                id NUMBER NOT NULL,
                name VARCHAR2(100),
                amount NUMBER,
                created_at DATE NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """,
        insert_sql=f"""
            INSERT INTO {src} (id, name, amount, created_at, updated_at)
            SELECT 1, 'Alice', 1, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00' FROM dual UNION ALL
            SELECT 1, 'Alice', 1, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00' FROM dual UNION ALL
            SELECT 2, 'Bob', 2, DATE '2024-01-02', TIMESTAMP '2024-01-02 10:00:00' FROM dual UNION ALL
            SELECT 3, 'Charlie', 3, DATE '2024-01-03', TIMESTAMP '2024-01-03 10:00:00' FROM dual
        """,
    )
    table_helper.create_table(
        engine=oracle_engine,
        table_name=trg,
        create_sql=f"""
            CREATE TABLE {trg} (
                id NUMBER NOT NULL,
                name VARCHAR2(100),
                amount NUMBER,
                created_at DATE NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """,
        insert_sql=f"""
            INSERT INTO {trg} (id, name, amount, created_at, updated_at)
            SELECT 1, 'Alice', 1, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00' FROM dual UNION ALL
            SELECT 2, 'Bobby', 2, DATE '2024-01-02', TIMESTAMP '2024-01-02 10:00:00' FROM dual UNION ALL
            SELECT 3, 'Charlie', 3, DATE '2024-01-03', TIMESTAMP '2024-01-03 10:00:00' FROM dual UNION ALL
            SELECT 5, 'Eve', 5, DATE '2024-01-05', TIMESTAMP '2024-01-05 10:00:00' FROM dual
        """,
    )

    pandas_result, sql_result = _run_both(
        oracle_engine,
        DataReference(src, 'test'),
        DataReference(trg, 'test'),
        date_column='created_at',
        update_column='updated_at',
        date_range=('2024-01-01', '2024-01-05'),
        custom_primary_key=['id'],
        tolerance_pct=100.0,
        max_examples=3,
    )
    assert_check_stats_equal(pandas_result.stats, sql_result.stats)
    assert_issue_breakdown_equal(pandas_result.details, sql_result.details)


def test_clickhouse_sql_pandas_stats_parity(clickhouse_engine, table_helper):
    src = 'test_parity_src'
    trg = 'test_parity_trg'
    table_helper.create_table(
        engine=clickhouse_engine,
        table_name=src,
        create_sql=f"""
            CREATE TABLE {src} (
                id UInt32,
                name Nullable(String),
                amount Float64,
                created_at Date,
                updated_at DateTime
            )
            ENGINE = MergeTree()
            ORDER BY id
        """,
        insert_sql=f"""
            INSERT INTO {src} VALUES
            (1, 'Alice', 1.0, '2024-01-01', '2024-01-01 10:00:00'),
            (1, 'Alice', 1.0, '2024-01-01', '2024-01-01 10:00:00'),
            (2, 'Bob', 2, '2024-01-02', '2024-01-02 10:00:00'),
            (3, 'Charlie', 3, '2024-01-03', '2024-01-03 10:00:00'),
            (4, 'Dave', 4, '2024-01-04', '2024-01-04 10:00:00')
        """,
    )
    table_helper.create_table(
        engine=clickhouse_engine,
        table_name=trg,
        create_sql=f"""
            CREATE TABLE {trg} (
                id UInt32,
                name Nullable(String),
                amount Float64,
                created_at Date,
                updated_at DateTime
            )
            ENGINE = MergeTree()
            ORDER BY id
        """,
        insert_sql=f"""
            INSERT INTO {trg} VALUES
            (1, 'Alice', 1, '2024-01-01', '2024-01-01 10:00:00'),
            (2, 'Bobby', 2.0, '2024-01-02', '2024-01-02 10:00:00'),
            (3, 'Charlie', 3, '2024-01-03', '2024-01-03 10:00:00'),
            (4, 'Dave', 4, '2024-01-04', '2024-01-04 10:00:00'),
            (5, 'Eve', 5, '2024-01-05', '2024-01-05 10:00:00')
        """,
    )

    pandas_result, sql_result = _run_both(
        clickhouse_engine,
        DataReference(src, 'test'),
        DataReference(trg, 'test'),
        date_column='created_at',
        update_column='updated_at',
        date_range=('2024-01-01', '2024-01-05'),
        custom_primary_key=['id'],
        tolerance_pct=100.0,
        max_examples=3,
    )
    assert_check_stats_equal(pandas_result.stats, sql_result.stats)
    assert_issue_breakdown_equal(pandas_result.details, sql_result.details)
