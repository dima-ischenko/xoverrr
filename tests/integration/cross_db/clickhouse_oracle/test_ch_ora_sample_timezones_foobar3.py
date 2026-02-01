"""
Test for bug fix: Mixed timezone offsets in timestamptz columns should be handled correctly.
When timestamptz columns contain mixed offsets (e.g., +05, +06), conversion with 
pd.to_datetime should use utc=True to avoid conversion errors.
Oracle ↔ ClickHouse specific test.
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

    def test_mixed_timezones_various_timezone_settings(self, oracle_engine, clickhouse_engine):
        """
        Test Oracle ↔ ClickHouse comparison with mixed timezone offsets.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
        # Test with different timezone settings
        test_timezones = [
            "UTC",
            "Europe/Athens",
            "America/Chicago",  # Has DST changes
            "Asia/Shanghai",    # +08:00, no DST
            "Pacific/Honolulu", # -10:00, no DST
            "+02:00",           # Simple offset
            "-07:00"            # Simple offset
        ]
        
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
                update_column="updated_on",
                date_range=("2024-01-01", "2024-01-08"),
                exclude_recent_hours=24,
                tolerance_percentage=0.0,
            )
            
            assert status == COMPARISON_SUCCESS, f"Failed with timezone {timezone}"
            assert stats.final_diff_score == 0.0, f"Non-zero diff with timezone {timezone}"
            print(f"Oracle → ClickHouse with mixed timezones passed (timezone={timezone}): {stats.final_score:.2f}%")

    def test_negative_timezone_offsets(self, oracle_engine, clickhouse_engine):
        """
        Test with negative timezone offsets and DST.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
        # Add data specifically for negative timezone testing
        with oracle_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (8, 'Negative offset standard', TIMESTAMP '2024-07-01 10:00:00 -07:00', 
                 TIMESTAMP '2024-07-01 11:00:00 -07:00', DATE '2024-07-01'),
                (9, 'Negative offset DST', TIMESTAMP '2024-07-01 10:00:00 -06:00', 
                 TIMESTAMP '2024-07-01 11:00:00 -06:00', DATE '2024-07-01')
            """))
        
        # ClickHouse equivalent (stored in UTC)
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (8, 'Negative offset standard', '2024-07-01 17:00:00.000000', '2024-07-01 18:00:00.000000', '2024-07-01'),
                (9, 'Negative offset DST', '2024-07-01 16:00:00.000000', '2024-07-01 17:00:00.000000', '2024-07-01')
            """))
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=clickhouse_engine,
            timezone="America/Denver",  # Mountain Time: -07:00/-06:00
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
        print(f"Oracle → ClickHouse negative timezone offsets test passed: {stats.final_score:.2f}%")

    def test_daylight_saving_time_edge_cases(self, oracle_engine, clickhouse_engine):
        """
        Test edge cases around Daylight Saving Time transitions.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
        # Add DST transition edge cases
        with oracle_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (10, 'Exactly DST start', TIMESTAMP '2024-03-10 01:59:59 -05:00', 
                 TIMESTAMP '2024-03-10 03:00:00 -04:00', DATE '2024-03-10'),
                (11, 'Exactly DST end', TIMESTAMP '2024-11-03 01:59:59 -04:00', 
                 TIMESTAMP '2024-11-03 01:00:00 -05:00', DATE '2024-11-03')
            """))
        
        # ClickHouse equivalent
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (10, 'Exactly DST start', '2024-03-10 06:59:59.000000', '2024-03-10 07:00:00.000000', '2024-03-10'),
                (11, 'Exactly DST end', TIMESTAMP '2024-11-03 05:59:59.000000', 
                 '2024-11-03 06:00:00.000000', DATE '2024-11-03')
            """))
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=clickhouse_engine,
            timezone="America/New_York",  # Eastern Time with DST
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            update_column="updated_on",
            date_range=("2024-03-10", "2024-03-11"),  # DST start
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        print(f"Oracle → ClickHouse DST edge case test passed: {stats.final_score:.2f}%")

    def test_date_cutoff_across_timezones(self, oracle_engine, clickhouse_engine):
        """
        Test date cutoff behavior when records span multiple timezones.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
        # Add records that appear on different calendar days in different timezones
        with oracle_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (12, 'Late in +14', TIMESTAMP '2024-01-01 23:30:00 +14:00', 
                 TIMESTAMP '2024-01-01 23:45:00 +14:00', DATE '2024-01-01'),
                (13, 'Early in -12', TIMESTAMP '2024-01-01 00:30:00 -12:00', 
                 TIMESTAMP '2024-01-01 00:45:00 -12:00', DATE '2024-01-01')
            """))
        
        # ClickHouse equivalent
        with clickhouse_engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {table_name} (id, event_name, created_on, updated_on, record_date) VALUES
                (12, 'Late in +14', '2024-01-01 09:30:00.000000', '2024-01-01 09:45:00.000000', '2024-01-01'),
                (13, 'Early in -12', '2024-01-01 12:30:00.000000', '2024-01-01 12:45:00.000000', '2024-01-01')
            """))
        
        # Test with extreme timezones
        test_timezones = [
            "Pacific/Kiritimati",  # +14:00
            "Pacific/Midway",       # -11:00
            "UTC"
        ]
        
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
                date_range=("2024-01-01", "2024-01-02"),
                tolerance_percentage=0.0,
            )
            
            assert status == COMPARISON_SUCCESS, f"Failed with timezone {timezone}"
            print(f"Oracle → ClickHouse date cutoff test passed (timezone={timezone}): {stats.final_score:.2f}%")

    def test_null_handling_in_timezone_columns(self, oracle_engine, clickhouse_engine):
        """
        Test proper handling of NULL values in timezone-aware columns.
        """
        table_name = "test_mixed_timezones_ora_ch"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=clickhouse_engine,
            timezone="Europe/Berlin",  # Central European Time
        )

        # Test specifically the NULL record (ID 5)
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="record_date",
            date_range=("2024-01-05", "2024-01-06"),  # Specifically test the NULL record
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"Oracle → ClickHouse NULL handling in timezone columns passed: {stats.final_score:.2f}%")