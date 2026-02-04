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

        table_name = 'test_mixed_timezones_query_ora_pg'

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

    def test_custom_query_with_utc_for_tz_aware(self, postgres_engine, oracle_engine):
        """
        Test custom query comparison with tz-aware data must use UTC.
        """
        pytest.skip('issue #33')
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='UTC',  # MUST be UTC for tz-aware data
        )

        source_query = """
            SELECT id, event_name, created_on, record_date
            FROM test.test_mixed_timezones_query_ora_pg
            WHERE record_date >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND record_date < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """

        target_query = """
            SELECT id, event_name, created_on, record_date
            FROM test.test_mixed_timezones_query_ora_pg
            WHERE record_date >= date_trunc('day', %(start_date)s::date)
              AND record_date < date_trunc('day', %(end_date)s::date) + interval '1 days'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-08'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-08'},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        print(report)

        assert status == COMPARISON_SUCCESS
        print(f'custom query with UTC for tz-aware passed: {stats.final_score:.2f}%')
