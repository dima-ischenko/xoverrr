"""
Test for bug fix: Mixed timezone offsets in timestamptz columns should be handled correctly.
All cross-database comparisons with tz-aware columns must use UTC.
"""

import pytest

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestPostgresOracleMixedTimezoneOffsets:
    """Test for mixed timezone offsets in timestamptz columns bug fix - PostgreSQL ↔ Oracle"""

    @pytest.fixture(autouse=True)
    def setup_mixed_timezone_data(self, postgres_engine, oracle_engine, table_helper):
        """Setup test data with mixed timezone offsets in timestamptz columns"""

        table_name = 'test_mixed_timezones_ora_pg'

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
            """,
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
            """,
        )

        yield

    def test_cross_db_comparison_with_utc_only(self, postgres_engine, oracle_engine):
        """
        Test cross-database comparison MUST use UTC when comparing tz-aware columns.
        This is Rule 1: All tz-aware comparisons must be done in UTC.
        """
        pytest.skip('issue #33')
        table_name = 'test_mixed_timezones_ora_pg'

        # Only UTC is valid for cross-db tz-aware comparisons
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='UTC',  # MUST be UTC for tz-aware columns
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='record_date',
            update_column='updated_on',
            date_range=('2024-01-01', '2024-01-08'),
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )

        print(report)

        assert status == COMPARISON_SUCCESS, (
            'Cross-db tz-aware comparison failed with UTC'
        )
        assert stats.final_diff_score == 0.0, f'Non-zero diff with UTC timezone'
        print(f'cross-db comparison with UTC passed: {stats.final_score:.2f}%')

    def test_cross_db_without_tz_aware_columns(
        self, postgres_engine, oracle_engine, table_helper
    ):
        """
        Test cross-database comparison without tz-aware columns can use any timezone.
        Demonstrates Rule 2: Don't mix tz-aware with tz-naive.
        """
        # Create tables without tz-aware columns for this test
        test_table = 'test_no_tz_columns'

        # PostgreSQL
        table_helper.create_table(
            engine=postgres_engine,
            table_name=test_table,
            create_sql=f"""
                CREATE TABLE {test_table} (
                    id INTEGER PRIMARY KEY,
                    event_name TEXT,
                    record_date DATE,
                    regular_timestamp TIMESTAMP
                )
            """,
            insert_sql=f"""
                INSERT INTO {test_table} (id, event_name, record_date, regular_timestamp) VALUES
                (1, 'Event 1', '2024-01-01', '2024-01-01 10:00:00'),
                (2, 'Event 2', '2024-01-02', '2024-01-02 11:00:00')
            """,
        )

        # Oracle
        table_helper.create_table(
            engine=oracle_engine,
            table_name=test_table,
            create_sql=f"""
                CREATE TABLE {test_table} (
                    id NUMBER PRIMARY KEY,
                    event_name VARCHAR2(100),
                    record_date DATE,
                    regular_timestamp TIMESTAMP
                )
            """,
            insert_sql=f"""
                INSERT INTO {test_table} (id, event_name, record_date, regular_timestamp) VALUES
                (1, 'Event 1', DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00'),
                (2, 'Event 2', DATE '2024-01-02', TIMESTAMP '2024-01-02 11:00:00')
            """,
        )

        # Can use any timezone since no tz-aware columns
        test_timezones = ['UTC', 'Europe/Athens', 'America/New_York', 'Asia/Tokyo']

        for timezone in test_timezones:
            comparator = DataQualityComparator(
                source_engine=oracle_engine,
                target_engine=postgres_engine,
                timezone=timezone,  # Any timezone valid for tz-naive
            )

            status, report, stats, details = comparator.compare_sample(
                source_table=DataReference(test_table, 'test'),
                target_table=DataReference(test_table, 'test'),
                date_column='record_date',
                date_range=('2024-01-01', '2024-01-03'),
                tolerance_percentage=0.0,
            )

            assert status == COMPARISON_SUCCESS, f'Failed with timezone {timezone}'
            print(
                f'Cross-db without tz-aware columns passed (timezone={timezone}): {stats.final_score:.2f}%'
            )

    def test_midnight_boundary_case_with_utc(self, postgres_engine, oracle_engine):
        """
        Test midnight boundary case with UTC timezone.
        """
        table_name = 'test_mixed_timezones_ora_pg'
        pytest.skip('issue #33')
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='UTC',  # Boundary cases must use UTC
        )

        # Test specific date range that includes the midnight-crossing record
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='record_date',
            update_column='updated_on',
            date_range=(
                '2024-01-06',
                '2024-01-07',
            ),  # Specifically test the midnight crossing
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f'midnight boundary test with UTC passed: {stats.final_score:.2f}%')
