"""
Test for bug fix: Mixed timezone offsets in timestamptz columns should be handled correctly.
When timestamptz columns contain mixed offsets (e.g., +05, +06), conversion with 
pd.to_datetime should use utc=True to avoid conversion errors.
PostgreSQL ↔ ClickHouse specific test.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestPostgresClickHouseMixedTimezoneOffsets:
    """Test for mixed timezone offsets in timestamptz columns bug fix - PostgreSQL ↔ ClickHouse"""
    
    @pytest.fixture(autouse=True)
    def setup_mixed_timezone_data(self, postgres_engine, clickhouse_engine, table_helper):
        """Setup test data with mixed timezone offsets in timestamptz columns"""
        
        table_name = "test_mixed_timezones_pg_ch"
        
        # PostgreSQL setup - contains mixed timezone offsets
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    event_name TEXT,
                    created_on TIMESTAMPTZ NULL,
                    updated_on TIMESTAMPTZ NULL,
                    record_date DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (1, 'Event in +05', '2024-01-01 10:00:00+05', '2024-01-01 11:00:00+05', '2024-01-01'),
                (2, 'Event in +06', '2024-01-02 10:00:00+06', '2024-01-02 11:00:00+06', '2024-01-02'),
                (3, 'Event in +00', '2024-01-03 10:00:00+00', '2024-01-03 11:00:00+00', '2024-01-03'),
                (4, 'Event in -08', '2024-01-04 10:00:00-08', '2024-01-04 11:00:00-08', '2024-01-04'),
                (5, 'Event with NULL', NULL, NULL, '2024-01-05'),
                (6, 'Event crossing midnight UTC', '2024-01-06 23:30:00+05', '2024-01-07 00:30:00+05', '2024-01-06'),
                (7, 'Event with future date', '3023-04-04 00:00:00+00', '3023-04-04 01:00:00+00', '3023-04-04')
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
                    created_on DateTime64(6),
                    updated_on DateTime64(6),
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

    def test_mixed_timezones_various_timezone_settings(self, postgres_engine, clickhouse_engine):
        """
        Test PostgreSQL ↔ ClickHouse comparison with mixed timezone offsets.
        ClickHouse doesn't store timezone info natively, so we store UTC times.
        """
        table_name = "test_mixed_timezones_pg_ch"
        
        # Test with different timezone settings including those with DST
        test_timezones = [
            "UTC",
            "Europe/Athens",
            "America/Los_Angeles",  # Has DST changes
            "Asia/Kolkata",  # +05:30 (non-integer offset)
            "Australia/Sydney",  # +10:00/+11:00 (DST)
            "+03:00",  # Simple offset
            "-05:00"   # Simple offset
        ]
        
        for timezone in test_timezones:
            comparator = DataQualityComparator(
                source_engine=postgres_engine,
                target_engine=clickhouse_engine,
                timezone=timezone,
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
            
            assert status == COMPARISON_SUCCESS, f"Failed with timezone {timezone}"
            assert stats.final_diff_score == 0.0, f"Non-zero diff with timezone {timezone}"
            print(f"PostgreSQL → ClickHouse with mixed timezones passed (timezone={timezone}): {stats.final_score:.2f}%")

    def test_dst_transition_period(self, postgres_engine, clickhouse_engine):
        """
        Test during DST transition period.
        """
        table_name = "test_mixed_timezones_pg_ch"
        
        # Create additional test data for DST transition
        with postgres_engine.begin() as conn:
            # Add data for DST transition period (March in Northern Hemisphere)
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (8, 'Before DST', '2024-03-09 10:00:00-05', '2024-03-09 11:00:00-05', '2024-03-09'),
                (9, 'After DST', '2024-03-10 10:00:00-04', '2024-03-10 11:00:00-04', '2024-03-10')
            """))
        
        # ClickHouse equivalent (stored in UTC)
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (8, 'Before DST', '2024-03-09 15:00:00.000000', '2024-03-09 16:00:00.000000', '2024-03-09'),
                (9, 'After DST', '2024-03-10 14:00:00.000000', '2024-03-10 15:00:00.000000', '2024-03-10')
            """))
        
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone="America/New_York",  # Has DST transition
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            update_column="updated_on",
            date_range=("2024-03-09", "2024-03-11"),
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"PostgreSQL → ClickHouse DST transition test passed: {stats.final_score:.2f}%")

    def test_custom_query_with_timezones(self, postgres_engine, clickhouse_engine):
        """
        Test custom query comparison with timezone-aware data.
        """
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone="Asia/Tokyo",  # +09:00, no DST
        )

        source_query = """
            SELECT id, event_name, created_on, record_date,
                   case when updated_on > (now() - INTERVAL '1 hours') then 'y' end as xrecently_changed
            FROM test.test_mixed_timezones_pg_ch
            WHERE record_date >= date_trunc('day', %(start_date)s::date)
              AND record_date < date_trunc('day', %(end_date)s::date) + interval '1 days'
        """
        
        # ClickHouse query (note: ClickHouse stores in UTC)
        target_query = """
            SELECT id, event_name, created_on, record_date,
                   case when updated_on > (now() - INTERVAL 1 HOUR) then 'y' end as xrecently_changed
            FROM test.test_mixed_timezones_pg_ch
            WHERE record_date >= toDate(%(start_date)s)
              AND record_date < toDate(%(end_date)s) + INTERVAL 1 day
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-08'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-08'},
            custom_primary_key=["id"],
            exclude_columns=["xrecently_changed"],  # Exclude as we don't want to compare this
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"PostgreSQL → ClickHouse custom query with timezones passed: {stats.final_score:.2f}%")

    def test_timezone_boundary_conditions(self, postgres_engine, clickhouse_engine):
        """
        Test edge cases with timezone boundaries.
        """
        table_name = "test_mixed_timezones_pg_ch"
        
        # Add edge case data
        with postgres_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (10, 'Near year boundary', '2023-12-31 23:30:00+00', '2024-01-01 00:30:00+00', '2023-12-31'),
                (11, 'Leap second day', '2023-12-31 23:59:60+00', '2024-01-01 00:00:00+00', '2023-12-31'),
                (12, 'All NULLs', NULL, NULL, '2024-01-10')
            """))
        
        # ClickHouse equivalent
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (10, 'Near year boundary', '2023-12-31 23:30:00.000000', '2024-01-01 00:30:00.000000', '2023-12-31'),
                (11, 'Leap second day', '2023-12-31 23:59:59.999999', '2024-01-01 00:00:00.000000', '2023-12-31'),
                (12, 'All NULLs', '1970-01-01 00:00:00.000000', '1970-01-01 00:00:00.000000', '2024-01-10')
            """))
        
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone="Pacific/Kiritimati",  # +14:00, first timezone to see new day
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            date_range=("2023-12-31", "2024-01-11"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        print(f"PostgreSQL → ClickHouse timezone boundary test passed: {stats.final_score:.2f}%")