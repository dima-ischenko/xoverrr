"""
Test for bug fix: Mixed timezone offsets in timestamptz columns should be handled correctly.
Oracle ↔ ClickHouse comparisons must handle timezone conversions properly.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestOracleClickHouseMixedTimezoneOffsets:
    """Test for mixed timezone offsets in timestamptz columns bug fix - Oracle ↔ ClickHouse"""
    
    @pytest.fixture(autouse=True)
    def setup_mixed_timezone_data(self, oracle_engine, clickhouse_engine, table_helper):
        """Setup test data with mixed timezone offsets in timestamptz columns"""
        
        table_name = "test_mixed_timezones_ora_ch"
        
        # Oracle setup - TIMESTAMP WITH TIME ZONE
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
        
        # ClickHouse setup (ClickHouse stores timestamps in UTC)
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_name String,
                    created_on DateTime64(6, 'UTC'),
                    updated_on DateTime64(6, 'UTC'),
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
                (5, 'Event with NULL', '1970-01-01 00:00:00.000000', '1970-01-01 00:00:00.000000', '2024-01-05'),
                (6, 'Event crossing midnight UTC', '2024-01-06 18:30:00.000000', '2024-01-06 19:30:00.000000', '2024-01-06'),
                (7, 'Event with future date', '3023-04-04 00:00:00.000000', '3023-04-04 01:00:00.000000', '3023-04-04')
            """
        )
        
        yield

    def test_cross_db_comparison_with_utc_only(self, oracle_engine, clickhouse_engine):
        """
        Test Oracle ↔ ClickHouse comparison MUST use UTC.
        Oracle has tz-aware columns, ClickHouse stores UTC.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
        # Only UTC is valid for this comparison
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=clickhouse_engine,
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
        
        assert status == COMPARISON_SUCCESS, "Failed with UTC timezone"
        assert stats.final_diff_score == 0.0, f"Non-zero diff with UTC timezone"
        print(f"Oracle → ClickHouse with UTC passed: {stats.final_score:.2f}%")

    def test_clickhouse_to_oracle_with_utc(self, clickhouse_engine, oracle_engine):
        """
        Test ClickHouse → Oracle comparison must use UTC.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
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
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"ClickHouse → Oracle with UTC passed: {stats.final_score:.2f}%")

    def test_oracle_tz_naive_comparison(self, oracle_engine, clickhouse_engine, table_helper):
        """
        Test comparison with Oracle tz-naive TIMESTAMP columns.
        Can use any timezone since both are tz-naive.
        """
        table_name = "test_ora_ch_tz_naive"
        
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
        
        # ClickHouse with tz-naive DateTime
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_name String,
                    created_on DateTime,  -- tz-naive
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
        
        # Can use any timezone since both are tz-naive
        test_timezones = ["UTC", "Europe/Athens", "America/Chicago"]
        
        for timezone in test_timezones:
            comparator = DataQualityComparator(
                source_engine=oracle_engine,
                target_engine=clickhouse_engine,
                timezone=timezone,
            )

            status, report, stats, details = comparator.compare_sample(
                source_table=DataReference(table_name, "test"),
                target_table=DataReference(table_name, "test"),
                date_column="record_date",
                date_range=("2024-01-01", "2024-01-03"),
                tolerance_percentage=0.0,
            )
            
            assert status == COMPARISON_SUCCESS, f"Failed with timezone {timezone}"
            print(f"Oracle ↔ ClickHouse tz-naive comparison passed (timezone={timezone}): {stats.final_score:.2f}%")

    def test_date_only_comparisons(self, oracle_engine, clickhouse_engine):
        """
        Test date-only comparisons work correctly.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
        # Can use any timezone for date-only comparisons
        test_timezones = ["UTC", "Europe/Athens", "Asia/Tokyo"]
        
        for timezone in test_timezones:
            comparator = DataQualityComparator(
                source_engine=oracle_engine,
                target_engine=clickhouse_engine,
                timezone=timezone,
            )

            status, report, stats, details = comparator.compare_counts(
                source_table=DataReference(table_name, "test"),
                target_table=DataReference(table_name, "test"),
                date_column="record_date",
                date_range=("2024-01-01", "2024-01-08"),
                tolerance_percentage=0.0,
            )
            
            assert status == COMPARISON_SUCCESS, f"Failed with timezone {timezone}"
            assert stats.final_score == 100.0
            print(f"Oracle → ClickHouse date-only comparison passed (timezone={timezone}): {stats.final_score:.2f}%")

    def test_custom_query_with_timezone_handling(self, oracle_engine, clickhouse_engine):
        """
        Test custom query comparison with proper timezone handling.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=clickhouse_engine,
            timezone="UTC",  # Must be UTC for tz-aware columns
        )

        source_query = """
            SELECT id, event_name, created_on, record_date,
                   case when updated_on > (sysdate - :exclude_recent_hours/24) then 'y' end as xrecently_changed
            FROM test.test_mixed_timezones_ora_ch
            WHERE record_date >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND record_date < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """
        
        # ClickHouse query (stored in UTC)
        target_query = """
            SELECT id, event_name, created_on, record_date,
                   case when updated_on > (now() - INTERVAL 1 HOUR) then 'y' end as xrecently_changed
            FROM test.test_mixed_timezones_ora_ch
            WHERE record_date >= toDate(%(start_date)s)
              AND record_date < toDate(%(end_date)s) + INTERVAL 1 day
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-08'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-08'},
            custom_primary_key=["id"],
            exclude_columns=["xrecently_changed"],
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"Oracle → ClickHouse custom query with UTC passed: {stats.final_score:.2f}%")

    def test_negative_timezone_offsets_with_utc(self, oracle_engine, clickhouse_engine):
        """
        Test negative timezone offsets work correctly with UTC.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
        # Add negative timezone data
        with oracle_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (8, 'Negative offset -07', TIMESTAMP '2024-07-01 10:00:00 -07:00', 
                 TIMESTAMP '2024-07-01 11:00:00 -07:00', DATE '2024-07-01')
            """))
        
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (8, 'Negative offset -07', '2024-07-01 17:00:00.000000', '2024-07-01 18:00:00.000000', '2024-07-01')
            """))
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=clickhouse_engine,
            timezone="UTC",  # Must use UTC
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            update_column="updated_on",
            date_range=("2024-07-01", "2024-07-02"),
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"Oracle → ClickHouse negative timezone offsets with UTC passed: {stats.final_score:.2f}%")