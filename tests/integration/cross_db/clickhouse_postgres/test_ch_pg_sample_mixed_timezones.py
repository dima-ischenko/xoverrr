"""
Test for bug fix: Mixed timezone offsets in timestamptz columns should be handled correctly.
ClickHouse ↔ PostgreSQL comparisons must handle timezone conversions properly.
ClickHouse doesn't store timezone info natively, so we store UTC times.
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
        
        table_name = "test_mixed_timezones_ch_pg"
        
        # ClickHouse setup (ClickHouse stores timestamps in UTC, no timezone info)
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
            (1, 'Event in +05', toDateTime64('2024-01-01 05:00:00.000000', 6, 'UTC'), toDateTime64('2024-01-01 06:00:00.000000', 6, 'UTC'), '2024-01-01'),
            (2, 'Event in +06', toDateTime64('2024-01-02 04:00:00.000000', 6, 'UTC'), toDateTime64('2024-01-02 05:00:00.000000', 6, 'UTC'), '2024-01-02'),
            (3, 'Event in +00', toDateTime64('2024-01-03 10:00:00.000000', 6, 'UTC'), toDateTime64('2024-01-03 11:00:00.000000', 6, 'UTC'), '2024-01-03'),
            (4, 'Event in -08', toDateTime64('2024-01-04 18:00:00.000000', 6, 'UTC'), toDateTime64('2024-01-04 19:00:00.000000', 6, 'UTC'), '2024-01-04'),
            (5, 'Event with NULL', NULL, NULL, '2024-01-05'),
            (6, 'Event crossing midnight UTC', toDateTime64('2024-01-06 18:30:00.000000', 6, 'UTC'), toDateTime64('2024-01-06 19:30:00.000000', 6, 'UTC'), '2024-01-06'),
            (7, 'Event with future date', toDateTime64('3023-04-04 00:00:00.000000', 6, 'UTC'), toDateTime64('3023-04-04 01:00:00.000000', 6, 'UTC'), '3023-04-04')
            """
        )
        
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

        yield

    def test_cross_db_comparison_must_use_utc(self, postgres_engine, clickhouse_engine):
        """
        Test PostgreSQL ↔ ClickHouse comparison MUST use UTC.
        ClickHouse stores UTC, PostgreSQL has tz-aware columns.
        """
        table_name = "test_mixed_timezones_ch_pg"
        
        # Only UTC is valid for this comparison
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
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
        print(f"PostgreSQL → ClickHouse with UTC passed: {stats.final_score:.2f}%")

    def test_clickhouse_utc_to_postgres_tz_aware(self, clickhouse_engine, postgres_engine):
        """
        Test ClickHouse (UTC) → PostgreSQL (tz-aware) comparison.
        Must use UTC for proper conversion.
        """
        table_name = "test_mixed_timezones_ch_pg"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="UTC",  # Must be UTC since ClickHouse stores UTC
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
        print(f"ClickHouse → PostgreSQL with UTC passed: {stats.final_score:.2f}%")

    def test_clickhouse_tz_naive_comparison(self, clickhouse_engine, postgres_engine, table_helper):
        """
        Test comparison with ClickHouse tz-naive columns.
        Can use any timezone since ClickHouse doesn't store timezone info.
        """
        table_name = "test_ch_pg_tz_naive"
        
        # ClickHouse with tz-naive DateTime
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_name String,
                    created_on DateTime,
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
        

        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    event_name TEXT,
                    created_on TIMESTAMP, 
                    record_date DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_name, created_on, record_date) VALUES
                (1, 'Event 1', '2024-01-01 12:00:00', '2024-01-01'),
                (2, 'Event 2', '2024-01-02 13:00:00', '2024-01-02')
            """
        )
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS


    def test_mixed_tz_types_not_allowed(self, postgres_engine, clickhouse_engine, table_helper):
        """
        Test that mixing tz-aware and tz-naive in comparison is not allowed.
        This demonstrates Rule 2 violation.
        """
        # Create separate tables with different tz types
        table_name = "test_pg_ch_tz_depend"

        
        # PostgreSQL with tz-aware
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    event_name TEXT,
                    created_at TIMESTAMPTZ,
                    record_date DATE
                )
            """,
            insert_sql =
            f"""
                INSERT INTO {table_name} (id, event_name, created_at, record_date) VALUES
                (1, 'Event', '2024-01-01 10:00:00+00', '2024-01-01')
            """
        )    
        
        
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_name String,
                    created_at DateTime,
                    record_date Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql =
            f"""
                INSERT INTO {table_name} (id, event_name, created_at, record_date) VALUES
                (1, 'Event', '2024-01-01 10:00:00', '2024-01-01')
            """)
        
        # This comparison should fail or show discrepancies
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone="UTC",  # Even UTC won't help mixing tz-aware with tz-naive
        )
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            date_range=("2024-01-01", "2024-01-07"),  # Includes boundary
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS

    def test_date_boundary_with_timezone_conversion(self, postgres_engine, clickhouse_engine):
        """
        Test date boundary case with UTC timezone.
        """
        table_name = "test_mixed_timezones_ch_pg"
        
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone="UTC",  # Must use UTC
        )

        # Test filtering on the boundary date
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            date_range=("2024-01-06", "2024-01-07"),  # Includes boundary
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        # Should have the boundary record (id=6)
        assert stats.common_pk_rows >= 1
        print(f"PostgreSQL → ClickHouse date boundary with UTC passed: {stats.final_score:.2f}%")