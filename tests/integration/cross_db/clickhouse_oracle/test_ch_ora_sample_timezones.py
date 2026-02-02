"""
Test for bug fix: Mixed timezone offsets in timestamptz columns should be handled correctly.
Oracle ↔ ClickHouse comparisons must handle timezone conversions properly.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestClickHouseOracleMixedTimezoneOffsets:
    """Test for mixed timezone offsets in timestamptz columns bug fix - Oracle ↔ ClickHouse"""
    
    @pytest.fixture(autouse=True)
    def setup_mixed_timezone_data(self, oracle_engine, clickhouse_engine, table_helper):
        """Setup test data with mixed timezone offsets in timestamptz columns"""
        
        table_name = "test_mixed_timezones_ch_ora"
        
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_name String,
                    created_on Nullable(DateTime64(6, 'UTC')),
                    updated_on Nullable(DateTime64(6, 'UTC')),
                    record_date Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (1, 'Event in +05', '2024-01-01 05:00:00.000000', '2024-01-01 06:00:00.000000', '2024-01-01'),
                (2, 'Event in +06', '2024-01-02 04:00:00.000000', '2024-01-02 05:00:00.000000', '2024-01-02'),
                (3, 'Event in +00', '2024-01-03 10:00:00.000000', '2024-01-03 11:00:00.000000', '2024-01-03'),
                (4, 'Event in -08', '2024-01-04 18:00:00.000000', '2024-01-04 19:00:00.000000', '2024-01-04'),
                (5, 'Event with NULL', Null, Null, '2024-01-05'),
                (6, 'Event crossing midnight UTC', '2024-01-06 18:30:00.000000', '2024-01-06 19:30:00.000000', '2024-01-06'),
                (7, 'Event with future date', '3023-04-04 00:00:00.000000', '3023-04-04 01:00:00.000000', '3023-04-04')
            """
        )

        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    event_name VARCHAR2(100),
                    created_on TIMESTAMP WITH TIME ZONE,
                    updated_on TIMESTAMP WITH TIME ZONE,
                    record_date DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (1, 'Event in +05', TIMESTAMP '2024-01-01 10:00:00 +05:00', 
                 TIMESTAMP '2024-01-01 11:00:00 +05:00', DATE '2024-01-01'),
                (2, 'Event in +06', TIMESTAMP '2024-01-02 10:00:00 +06:00', 
                 TIMESTAMP '2024-01-02 11:00:00 +06:00', DATE '2024-01-02'),
                (3, 'Event in +00', TIMESTAMP '2024-01-03 10:00:00 +00:00', 
                 TIMESTAMP '2024-01-03 11:00:00 +00:00', DATE '2024-01-03'),
                (4, 'Event in -08', TIMESTAMP '2024-01-04 10:00:00 -08:00', 
                 TIMESTAMP '2024-01-04 11:00:00 -08:00', DATE '2024-01-04'),
                (5, 'Event with NULL', NULL, NULL, DATE '2024-01-05'),
                (6, 'Event crossing midnight UTC', TIMESTAMP '2024-01-06 23:30:00 +05:00', 
                 TIMESTAMP '2024-01-07 00:30:00 +05:00', DATE '2024-01-06'),
                (7, 'Event with future date', TIMESTAMP '3023-04-04 00:00:00 +00:00', 
                 TIMESTAMP '3023-04-04 01:00:00 +00:00', DATE '3023-04-04')
            """
        )
        
        yield

    def test_comparison_with_utc_only(self, oracle_engine, clickhouse_engine):
        """
        Test Oracle ↔ ClickHouse comparison MUST use UTC.
        Oracle has tz-aware columns, ClickHouse stores UTC.
        """
        pytest.skip("issue #33")
        table_name = "test_mixed_timezones_ch_ora"
        
        # Only UTC is valid for this comparison
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone="UTC",  # MUST be UTC
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            update_column="updated_on",
            date_range=("2024-01-01", "2024-01-08"),
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS, "Failed with UTC timezone"
        assert stats.final_diff_score == 0.0, f"Non-zero diff with UTC timezone"
        print(f"Oracle   ClickHouse with UTC passed: {stats.final_score:.2f}%")

    def test_clickhouse_to_oracle_with_utc(self, clickhouse_engine, oracle_engine):
        """
        Test ClickHouse   Oracle comparison must use UTC.
        """
        pytest.skip("issue #33")
        table_name = "test_mixed_timezones_ch_ora"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone="UTC",  # Must be UTC
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            update_column="updated_on",
            date_range=("2024-01-01", "2024-01-08"),
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"ClickHouse   Oracle with UTC passed: {stats.final_score:.2f}%")

    def test_oracle_tz_naive_comparison(self, oracle_engine, clickhouse_engine, table_helper):
        """
        Test comparison with Oracle tz-naive TIMESTAMP columns.
        Can use any timezone since both are tz-naive.
        """
        table_name = "test_ch_ora_tz_naive"
        
        # Oracle with tz-naive TIMESTAMP
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    event_name VARCHAR2(100),
                    created_on TIMESTAMP,  -- tz-naive
                    record_date DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_name, created_on, record_date) VALUES
                (1, 'Event 1', TIMESTAMP '2024-01-01 10:00:00', DATE '2024-01-01'),
                (2, 'Event 2', TIMESTAMP '2024-01-02 11:00:00', DATE '2024-01-02')
            """
        )
        
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_name String,
                    created_on DateTime('UTC'),
                    record_date Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_name, created_on, record_date) VALUES
                (1, 'Event 1', '2024-01-01 10:00:00', '2024-01-01'),
                (2, 'Event 2', '2024-01-02 11:00:00', '2024-01-02')
            """
        )

        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS