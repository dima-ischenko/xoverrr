import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestClickHouseOracleCountsWithVariousDateTypes:
    """Cross-database count-based comparison tests with various date/time types"""

    @pytest.fixture(autouse=True)
    def setup_various_date_type_data(
        self, clickhouse_engine, oracle_engine, table_helper
    ):
        """Setup test data with various date/time column types"""

        table_name = 'test_ch_ora_date_types'

        # ClickHouse setup
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    event_date Date,
                    event_datetime DateTime,
                    event_datetime64 DateTime64(6),
                    event_type String
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, event_datetime, 
                                         event_datetime64, event_type) VALUES
                (1, '2024-01-01', 
                 '2024-01-01 10:30:45',
                 '2024-01-01 10:30:45.123456',
                 'login'),
                (2, '2024-01-01',
                 '2024-01-01 14:20:30',
                 '2024-01-01 14:20:30.987654',
                 'purchase'),
                (3, '2024-01-02',
                 '2024-01-02 09:15:20',
                 '2024-01-02 09:15:20.555555',
                 'logout'),
                (4, '2024-01-02',
                 '2024-01-02 15:45:10',
                 '2024-01-02 15:45:10.777777',
                 'view'),
                (5, '2024-01-03',
                 '2024-01-03 11:00:00',
                 '2024-01-03 11:00:00.000000',
                 'login')
            """,
        )

        # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    event_date DATE,
                    event_datetime TIMESTAMP,
                    event_datetime64 TIMESTAMP, 
                    event_type VARCHAR2(50)
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, event_datetime, 
                                         event_datetime64, event_type) VALUES
                (1, DATE '2024-01-01',
                 TIMESTAMP '2024-01-01 10:30:45.000000',
                 TIMESTAMP '2024-01-01 10:30:45.123456',
                 'login'),
                (2, DATE '2024-01-01',
                 TIMESTAMP '2024-01-01 14:20:30.000000',
                 TIMESTAMP '2024-01-01 14:20:30.987654',
                 'purchase'),
                (3, DATE '2024-01-02',
                 TIMESTAMP '2024-01-02 09:15:20.000000',
                 TIMESTAMP '2024-01-02 09:15:20.555555',
                 'logout'),
                (4, DATE '2024-01-02',
                 TIMESTAMP '2024-01-02 15:45:10.000000',
                 TIMESTAMP '2024-01-02 15:45:10.777777',
                 'view'),
                (5, DATE '2024-01-03',
                 TIMESTAMP '2024-01-03 11:00:00.000000',
                 TIMESTAMP '2024-01-03 11:00:00.000000',
                 'login')
            """,
        )

        yield

    def test_counts_with_date_column(self, clickhouse_engine, oracle_engine):
        """Test count comparison using DATE column type"""
        table_name = 'test_ch_ora_date_types'

        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='event_date',  # DATE type
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f'DATE column count comparison passed: {stats.final_score:.2f}%')

    def test_counts_with_datetime_column(self, clickhouse_engine, oracle_engine):
        """Test count comparison using ClickHouse DateTime vs Oracle TIMESTAMP"""
        table_name = 'test_ch_ora_date_types'

        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='event_datetime',  # ClickHouse DateTime / Oracle TIMESTAMP
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(
            f'DateTime/TIMESTAMP column count comparison passed: {stats.final_score:.2f}%'
        )

    def test_counts_with_datetime64_column(self, clickhouse_engine, oracle_engine):
        """Test count comparison using ClickHouse DateTime64 vs Oracle TIMESTAMP"""
        table_name = 'test_ch_ora_date_types'

        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='event_datetime64',  # ClickHouse DateTime64 / Oracle TIMESTAMP
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(
            f'DateTime64/TIMESTAMP column count comparison passed: {stats.final_score:.2f}%'
        )

    def test_counts_with_mixed_timezone(self, clickhouse_engine, oracle_engine):
        """Test count comparison with explicit timezone setting"""
        table_name = 'test_ch_ora_date_types'

        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='Europe/Moscow',  # Named timezone
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='event_datetime',
            date_range=('2024-01-01', '2024-01-04'),
            tolerance_percentage=0.0,
        )

        # Oracle thin client doesn't support named time zones, but should still work
        # with offset-based timezone conversion
        assert status == COMPARISON_SUCCESS
        print(f'Mixed timezone count comparison passed: {stats.final_score:.2f}%')
