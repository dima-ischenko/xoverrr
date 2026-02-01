"""
Test for bug fix: Mixed timezone offsets in timestamptz columns should be handled correctly.
When timestamptz columns contain mixed offsets (e.g., +05, +06), conversion with 
pd.to_datetime should use utc=True to avoid conversion errors.
PostgreSQL ↔ Oracle specific test.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestPostgresOracleMixedTimezoneOffsets:
    """Test for mixed timezone offsets in timestamptz columns bug fix - PostgreSQL ↔ Oracle"""
    
    @pytest.fixture(autouse=True)
    def setup_mixed_timezone_data(self, postgres_engine, oracle_engine, table_helper):
        """Setup test data with mixed timezone offsets in timestamptz columns"""
        
        table_name = "test_mixed_timezones_pg_ora"
        
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
        
        # Oracle setup
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

    def test_mixed_timezones_various_timezone_settings(self, postgres_engine, oracle_engine):
        """
        Test PostgreSQL ↔ Oracle comparison with mixed timezone offsets.
        This specifically tests the bug fix for timestamptz columns.
        Test with various timezone settings.
        """
        table_name = "test_mixed_timezones_pg_ora"
        
        # Test with different timezone settings to ensure robustness
        test_timezones = [
            "UTC",
            "Europe/Athens",  # Existing test timezone
            "Asia/Kolkata",   # +05:30
            "America/New_York",  # -05:00/-04:00 (DST changes)
            "Asia/Yekaterinburg",  # +05:00
            "+06:00",  # Offset timezone
            "-08:00",  # Offset timezone
            "Pacific/Auckland",  # +12:00/+13:00 (DST)
            "Europe/Moscow"  # Fixed +03:00
        ]
        
        for timezone in test_timezones:
            comparator = DataQualityComparator(
                source_engine=postgres_engine,
                target_engine=oracle_engine,
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
            print(f"PostgreSQL → Oracle with mixed timezones passed (timezone={timezone}): {stats.final_score:.2f}%")

    def test_midnight_boundary_case(self, postgres_engine, oracle_engine):
        """
        Special test case for records that cross UTC midnight.
        This can be problematic with timezone conversions.
        """
        table_name = "test_mixed_timezones_pg_ora"
        
        # Use timezone with non-integer offset to test edge cases
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=oracle_engine,
            timezone="Asia/Kolkata",  # +05:30
        )

        # Test specific date range that includes the midnight-crossing record
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            update_column="updated_on",
            date_range=("2024-01-06", "2024-01-07"),  # Specifically test the midnight crossing
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"PostgreSQL → Oracle midnight boundary test passed: {stats.final_score:.2f}%")

    def test_future_date_handling(self, postgres_engine, oracle_engine):
        """
        Test handling of future dates (beyond year 2262).
        Requires errors='coerce' parameter in pd.to_datetime.
        """
        table_name = "test_mixed_timezones_pg_ora"
        
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=oracle_engine,
            timezone="UTC",
        )

        # Test with future date
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            update_column="updated_on",
            date_range=("3023-04-01", "3023-04-05"),  # Future date range
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        
        # Should work without errors due to errors='coerce'
        assert status == COMPARISON_SUCCESS
        print(f"PostgreSQL → Oracle future date handling passed: {stats.final_score:.2f}%")

    def test_count_comparison_with_mixed_timezones(self, postgres_engine, oracle_engine):
        """
        Test count-based comparison with mixed timezone data.
        """
        table_name = "test_mixed_timezones_pg_ora"
        
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=oracle_engine,
            timezone="Europe/London",  # Timezone with DST
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            date_range=("2024-01-01", "2024-01-08"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"PostgreSQL → Oracle count comparison with mixed timezones passed: {stats.final_score:.2f}%")