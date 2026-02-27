"""
Test custom query comparison between PostgreSQL and ClickHouse.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestCustomQueryComparisonPGClickHouse:
    """Tests for custom query comparison between PostgreSQL and ClickHouse"""

    @pytest.fixture(autouse=True)
    def setup_custom_data(self, postgres_engine, clickhouse_engine, table_helper):
        """Setup test data in both databases"""
        table_name = 'test_custom_data_pg_ch'

        # PostgreSQL setup
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    amount      numeric,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL,
                    is_active   boolean
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, amount, created_at, updated_at, is_active) VALUES
                (1, 'Alice',  1000,     '2024-01-01', '2024-01-01 12:00:00', null),
                (2, 'Robert', 2000,     '2024-01-02', '2024-01-02 13:00:00', false),
                (3, 'Charlie', 300000,  '2024-01-03', '2024-01-03 14:00:00', null),
                (4, 'John',   null,     '2024-01-04', '2024-01-04 14:00:00', true),
                (5, 'Kate',   50000.01, '2024-01-04', '2024-01-05 14:00:00', true),
                (6, 'Mike',   1500.50,  '2024-01-05', '2024-01-05 17:30:00', false)
            """,
        )

        # ClickHouse setup - using appropriate data types
        clickhouse_create_sql = f"""
            CREATE TABLE {table_name} (
                id          Int32,
                name        String,
                amount      Nullable(Float64),
                created_at  Date,
                updated_at  DateTime,
                is_active   Nullable(UInt8)
            ) ENGINE = Memory()
        """

        clickhouse_insert_sql = f"""
            INSERT INTO {table_name} (id, name, amount, created_at, updated_at, is_active) VALUES
            (1, 'Alice',  1000,     '2024-01-01', '2024-01-01 10:00:00', NULL),
            (2, 'Robert', 2000,     '2024-01-02', '2024-01-02 11:00:00', 0),
            (3, 'Charlie', 300000,  '2024-01-03', '2024-01-03 12:00:00', NULL),
            (4, 'John',   NULL,     '2024-01-04', '2024-01-04 12:00:00', 1),
            (5, 'Kate',   50000.01, '2024-01-04', '2024-01-05 12:00:00', 1),
            (6, 'Mike',   1500.50,  '2024-01-05', '2024-01-05 15:30:00', 0)
        """

        # Create and populate ClickHouse table
        with clickhouse_engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {table_name}'))
            conn.execute(text(clickhouse_create_sql))
            conn.execute(text(clickhouse_insert_sql))
            conn.commit()

        yield

        # Cleanup
        with clickhouse_engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {table_name}'))
            conn.commit()

    def test_custom_query_comparison_basic(self, postgres_engine, clickhouse_engine):
        """Test basic comparison with id and name columns"""
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        source_query = """
            SELECT id, name, created_at
            FROM test.test_custom_data_pg_ch
            WHERE created_at >= date_trunc('day', cast(:start_date as date))
              AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 days'
        """

        target_query = """
            SELECT id, name, created_at
            FROM test_custom_data_pg_ch
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date) + INTERVAL 1 day
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.total_matched_rows == 6
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')

    def test_custom_query_comparison_numeric(self, postgres_engine, clickhouse_engine):
        """Test comparison with numeric/amount column"""
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        source_query = """
            SELECT id, amount
            FROM test.test_custom_data_pg_ch
            WHERE created_at >= date_trunc('day', cast(:start_date as date))
              AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 days'
        """

        target_query = """
            SELECT id, amount
            FROM test_custom_data_pg_ch
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date) + INTERVAL 1 day
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')

    def test_custom_query_comparison_boolean(self, postgres_engine, clickhouse_engine):
        """Test comparison with boolean/is_active column"""
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        source_query = """
            SELECT id, is_active
            FROM test.test_custom_data_pg_ch
            WHERE created_at >= date_trunc('day', cast(:start_date as date))
              AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 days'
        """

        target_query = """
            SELECT id, is_active
            FROM test_custom_data_pg_ch
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date) + INTERVAL 1 day
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')

    def test_custom_query_comparison_datetime(self, postgres_engine, clickhouse_engine):
        """Test comparison with datetime/updated_at column"""
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        source_query = """
            SELECT id, updated_at
            FROM test.test_custom_data_pg_ch
            WHERE created_at >= date_trunc('day', cast(:start_date as date))
              AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 days'
        """

        target_query = """
            SELECT id, updated_at
            FROM test_custom_data_pg_ch
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date) + INTERVAL 1 day
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')

    def test_custom_query_comparison_asterisk(self, postgres_engine, clickhouse_engine):
        """Test comparison with SELECT *"""
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        source_query = """
            SELECT *
            FROM test.test_custom_data_pg_ch
            WHERE created_at >= date_trunc('day', cast(:start_date as date))
              AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 days'
        """

        target_query = """
            SELECT *
            FROM test_custom_data_pg_ch
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date) + INTERVAL 1 day
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.total_matched_rows == 6
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')

    def test_custom_query_comparison_with_filter(
        self, postgres_engine, clickhouse_engine
    ):
        """Test comparison with LIKE filter"""
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        source_query = """
            SELECT id, name, created_at
            FROM test.test_custom_data_pg_ch
            WHERE created_at >= date_trunc('day', cast(:start_date as date))
              AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 days'
              AND name like :name_filter
        """

        target_query = """
            SELECT id, name, created_at
            FROM test_custom_data_pg_ch
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date) + INTERVAL 1 day
              AND name like :name_filter
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={
                'start_date': '2024-01-01',
                'end_date': '2024-01-06',
                'name_filter': '%lice%',
            },
            target_query=target_query,
            target_params={
                'start_date': '2024-01-01',
                'end_date': '2024-01-06',
                'name_filter': '%lice%',
            },
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.total_matched_rows == 1  # Only Alice matches
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')

    def test_custom_query_comparison_date_range(
        self, postgres_engine, clickhouse_engine
    ):
        """Test comparison with specific date range"""
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        source_query = """
            SELECT id, name, created_at
            FROM test.test_custom_data_pg_ch
            WHERE created_at >= date_trunc('day', cast(:start_date as date))
              AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 days'
        """

        target_query = """
            SELECT id, name, created_at
            FROM test_custom_data_pg_ch
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date) + INTERVAL 1 day
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-02', 'end_date': '2024-01-04'},
            target_query=target_query,
            target_params={'start_date': '2024-01-02', 'end_date': '2024-01-04'},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.total_matched_rows == 4  # IDs 2,3,4,5 (Jan 2-4)
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')

    def test_custom_query_comparison_group_by(self, postgres_engine, clickhouse_engine):
        """Test comparison with GROUP BY aggregation"""
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=clickhouse_engine,
            timezone='Europe/Athens',
        )

        source_query = """
            SELECT 
                created_at::date as dt,
                count(*) as cnt,
                sum(amount) as total_amount
            FROM test.test_custom_data_pg_ch
            WHERE created_at >= date_trunc('day', cast(:start_date as date))
              AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 days'
            GROUP BY created_at::date
            ORDER BY dt
        """

        target_query = """
            SELECT 
                toDate(created_at) as dt,
                count(*) as cnt,
                sum(amount) as total_amount
            FROM test_custom_data_pg_ch
            WHERE created_at >= toDate(:start_date)
              AND created_at < toDate(:end_date) + INTERVAL 1 day
            GROUP BY toDate(created_at)
            ORDER BY dt
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-06'},
            custom_primary_key=['dt'],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')
