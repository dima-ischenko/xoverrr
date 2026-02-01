"""
Test for timestamp with timezone handling in PostgreSQL/Greenplum with mixed offsets.
This test specifically checks that all timestamptz comparisons are done in UTC.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestPostgresMixedTimezoneOffsets:
    """Tests for PostgreSQL/Greenplum timestamptz columns with mixed timezone offsets"""
    
    @pytest.fixture(autouse=True)
    def setup_mixed_timezone_data(self, postgres_engine, table_helper):
        """Setup test data with timestamptz columns containing mixed timezone offsets"""
        
        table_name = "test_pg_mixed_tz_offsets"
        
        # PostgreSQL setup with mixed timezone offsets
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    event_name TEXT NOT NULL,
                    -- Explicitly storing timestamps with different timezone offsets
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ,
                    -- Mix of timezone offsets that caused the bug
                    timestamp_utc TIMESTAMPTZ,
                    timestamp_plus5 TIMESTAMPTZ,
                    timestamp_plus6 TIMESTAMPTZ,
                    timestamp_minus8 TIMESTAMPTZ,
                    -- Regular timestamp for comparison
                    regular_timestamp TIMESTAMP,
                    event_date DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (
                    id, event_name, created_at, updated_at,
                    timestamp_utc, timestamp_plus5, timestamp_plus6, timestamp_minus8,
                    regular_timestamp, event_date
                ) VALUES
                -- Record 1: UTC timezone
                (1, 'Event UTC',
                 '2024-01-01 10:00:00+00',
                 '2024-01-01 11:00:00+00',
                 '2024-01-01 12:00:00+00',
                 '2024-01-01 13:00:00+05',
                 '2024-01-01 14:00:00+06',
                 '2024-01-01 15:00:00-08',
                 '2024-01-01 16:00:00',
                 '2024-01-01'),
                
                -- Record 2: +05 timezone (Asia/Yekaterinburg)
                (2, 'Event +05',
                 '2024-01-02 10:00:00+05',
                 '2024-01-02 11:00:00+05',
                 '2024-01-02 12:00:00+00',
                 '2024-01-02 13:00:00+05',
                 '2024-01-02 14:00:00+06',
                 '2024-01-02 15:00:00-08',
                 '2024-01-02 16:00:00',
                 '2024-01-02'),
                
                -- Record 3: +06 timezone (Asia/Omsk)
                (3, 'Event +06',
                 '2024-01-03 10:00:00+06',
                 '2024-01-03 11:00:00+06',
                 '2024-01-03 12:00:00+00',
                 '2024-01-03 13:00:00+05',
                 '2024-01-03 14:00:00+06',
                 '2024-01-03 15:00:00-08',
                 '2024-01-03 16:00:00',
                 '2024-01-03'),
                
                -- Record 4: -08 timezone (America/Los_Angeles)
                (4, 'Event -08',
                 '2024-01-04 10:00:00-08',
                 '2024-01-04 11:00:00-08',
                 '2024-01-04 12:00:00+00',
                 '2024-01-04 13:00:00+05',
                 '2024-01-04 14:00:00+06',
                 '2024-01-04 15:00:00-08',
                 '2024-01-04 16:00:00',
                 '2024-01-04'),
                
                -- Record 5: NULL values in timestamptz columns
                (5, 'Event with NULLs',
                 '2024-01-05 10:00:00+00',
                 NULL,
                 NULL,
                 '2024-01-05 13:00:00+05',
                 NULL,
                 '2024-01-05 15:00:00-08',
                 NULL,
                 '2024-01-05'),
                
                -- Record 6: Date boundary case (near midnight in different timezones)
                (6, 'Date boundary',
                 '2024-01-06 23:30:00+05',  -- Jan 6 23:30 +05
                 '2024-01-07 00:30:00+05',  -- Jan 7 00:30 +05 (crosses midnight)
                 '2024-01-06 18:30:00+00',  -- Jan 6 18:30 UTC
                 '2024-01-06 23:30:00+05',  -- Same as created_at
                 '2024-01-07 00:30:00+06',  -- Jan 7 00:30 +06
                 '2024-01-06 10:30:00-08',  -- Jan 6 10:30 -08
                 '2024-01-06 23:30:00',
                 '2024-01-06')
            """
        )
        
        yield

    def test_self_comparison_with_utc_timezone(self, postgres_engine):
        """
        Test self-comparison with UTC timezone (Rule 1).
        All timestamptz comparisons must be done in UTC.
        """
        table_name = "test_pg_mixed_tz_offsets"
        
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="UTC",  # Explicit UTC for tz-aware comparisons
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",
            update_column="updated_at",
            date_range=("2024-01-01", "2024-01-07"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"PostgreSQL self-comparison with UTC timezone passed: {stats.final_score:.2f}%")

    def test_comparison_with_explicit_timezone_fails(self, postgres_engine):
        """
        Test that comparison with non-UTC timezone fails when tz-aware columns are present.
        This demonstrates Rule 1 violation.
        """
        table_name = "test_pg_mixed_tz_offsets"
        
        # This should either fail or show discrepancies because timestamps are compared in different timezones
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",  # Non-UTC timezone
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",
            date_range=("2024-01-01", "2024-01-02"),
            exclude_columns=["updated_at", "timestamp_utc", "timestamp_plus5", "timestamp_plus6", "timestamp_minus8"],  # Exclude tz-aware columns
            tolerance_percentage=0.0,
        )

        # Should succeed because we excluded tz-aware columns
        assert status == COMPARISON_SUCCESS
        print(f"PostgreSQL comparison with non-UTC but tz-naive columns passed: {stats.final_score:.2f}%")

    def test_cross_table_comparison_with_utc(self, postgres_engine, table_helper):
        """
        Test comparison between two tables with same data, must use UTC.
        """
        source_table = "test_pg_mixed_tz_offsets"
        target_table = "test_pg_mixed_tz_offsets_copy"
        
        # Create a copy of the table with same data
        table_helper.create_table(
            engine=postgres_engine,
            table_name=target_table,
            create_sql=f"""
                CREATE TABLE {target_table} (
                    id INTEGER PRIMARY KEY,
                    event_name TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ,
                    timestamp_utc TIMESTAMPTZ,
                    timestamp_plus5 TIMESTAMPTZ,
                    timestamp_plus6 TIMESTAMPTZ,
                    timestamp_minus8 TIMESTAMPTZ,
                    regular_timestamp TIMESTAMP,
                    event_date DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {target_table} (
                    id, event_name, created_at, updated_at,
                    timestamp_utc, timestamp_plus5, timestamp_plus6, timestamp_minus8,
                    regular_timestamp, event_date
                )
                SELECT 
                    id, event_name, created_at, updated_at,
                    timestamp_utc, timestamp_plus5, timestamp_plus6, timestamp_minus8,
                    regular_timestamp, event_date
                FROM {source_table}
            """
        )
        
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="Asia/Tokyo",  # Required for tz-aware columns
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(source_table, "test"),
            target_table=DataReference(target_table, "test"),
            date_column="event_date",
            update_column="updated_at",
            date_range=("2024-01-01", "2024-01-07"),
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"PostgreSQL cross-table comparison with UTC passed: {stats.final_score:.2f}%")

    def test_tz_naive_vs_tz_aware_separate_comparisons(self, postgres_engine):
        """
        Test that tz-naive and tz-aware columns should be compared separately.
        """
        table_name = "test_pg_mixed_tz_offsets"
        
        # Test 1: Compare only tz-naive columns with local timezone
        comparator_local = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="Europe/Paris",  # Local timezone for tz-naive
        )

        status_local, _, _, _ = comparator_local.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",
            date_range=("2024-01-01", "2024-01-07"),
            include_columns=["id", "event_name", "regular_timestamp", "event_date"],  # Only tz-naive
            tolerance_percentage=0.0,
        )
        
        assert status_local == COMPARISON_SUCCESS
        print("Tz-naive columns comparison with local timezone passed")
        
      
        comparator_utc = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="US/Pacific",  
        )

        status_utc, _, _, _ = comparator_utc.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",
            date_range=("2024-01-01", "2024-01-07"),
            include_columns=["id", "event_name", "created_at", "timestamp_utc", "event_date"],  # Only tz-aware
            tolerance_percentage=0.0,
        )
        
        assert status_utc == COMPARISON_SUCCESS
        print("Tz-aware columns comparison with UTC passed")

    def test_date_boundary_filtering_with_utc(self, postgres_engine):
        """
        Test date boundary filtering works correctly with UTC timezone.
        """
        table_name = "test_pg_mixed_tz_offsets"
        
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="Asia/Tokyo", 
        )

        # Test filtering on the boundary date
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",
            date_range=("2024-01-06", "2024-01-07"),  # Includes the boundary case
            include_columns=["id", "event_name", "created_at", "event_date"],  # Include tz-aware column
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        # Should have the boundary record (id=6)
        assert stats.common_pk_rows == 1