import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestOraclePostgresCountsWithVariousDateTypes:
    """Cross-database count-based comparison tests with various date/time types"""
    
    @pytest.fixture(autouse=True)
    def setup_various_date_type_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup test data with various date/time column types"""
        
        table_name = "test_various_date_types"
        
        # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    event_date DATE,
                    event_timestamp TIMESTAMP,
                    event_datetime TIMESTAMP,
                    event_timestamp_tz TIMESTAMP WITH TIME ZONE,
                    event_type VARCHAR2(50)
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, event_timestamp, event_datetime, 
                                         event_timestamp_tz, event_type) VALUES
                (1, DATE '2024-01-01', 
                 TIMESTAMP '2024-01-01 10:30:45.123456',
                 TIMESTAMP '2024-01-01 10:30:45.123456',
                 TIMESTAMP '2024-01-01 10:30:45.123456 +00:00',
                 'login'),
                (2, DATE '2024-01-01',
                 TIMESTAMP '2024-01-01 14:20:30.987654',
                 TIMESTAMP '2024-01-01 14:20:30.987654',
                 TIMESTAMP '2024-01-01 14:20:30.987654 +05:00',
                 'purchase'),
                (3, DATE '2024-01-02',
                 TIMESTAMP '2024-01-02 09:15:20.555555',
                 TIMESTAMP '2024-01-02 09:15:20.555555',
                 TIMESTAMP '2024-01-02 09:15:20.555555 -08:00',
                 'logout'),
                (4, DATE '2024-01-02',
                 TIMESTAMP '2024-01-02 15:45:10.777777',
                 TIMESTAMP '2024-01-02 15:45:10.777777',
                 TIMESTAMP '2024-01-02 15:45:10.777777 +01:00',
                 'view'),
                (5, DATE '2024-01-03',
                 TIMESTAMP '2024-01-03 11:00:00.000000',
                 TIMESTAMP '2024-01-03 11:00:00.000000',
                 TIMESTAMP '2024-01-03 11:00:00.000000 +00:00',
                 'login')
            """
        )
        
        # PostgreSQL setup
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    event_date DATE,
                    event_timestamp TIMESTAMP,
                    event_datetime TIMESTAMP,
                    event_timestamp_tz TIMESTAMPTZ,
                    event_type TEXT
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, event_timestamp, event_datetime, 
                                         event_timestamp_tz, event_type) VALUES
                (1, '2024-01-01',
                 '2024-01-01 10:30:45.123456',
                 '2024-01-01 10:30:45.123456',
                 '2024-01-01 10:30:45.123456+00',
                 'login'),
                (2, '2024-01-01',
                 '2024-01-01 14:20:30.987654',
                 '2024-01-01 14:20:30.987654',
                 '2024-01-01 14:20:30.987654+05',
                 'purchase'),
                (3, '2024-01-02',
                 '2024-01-02 09:15:20.555555',
                 '2024-01-02 09:15:20.555555',
                 '2024-01-02 09:15:20.555555-08',
                 'logout'),
                (4, '2024-01-02',
                 '2024-01-02 15:45:10.777777',
                 '2024-01-02 15:45:10.777777',
                 '2024-01-02 15:45:10.777777+01',
                 'view'),
                (5, '2024-01-03',
                 '2024-01-03 11:00:00.000000',
                 '2024-01-03 11:00:00.000000',
                 '2024-01-03 11:00:00.000000+00',
                 'login')
            """
        )
        
        yield

    def test_counts_with_date_column(self, oracle_engine, postgres_engine):
        """Test count comparison using DATE column type"""
        table_name = "test_various_date_types"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",  # DATE type
            date_range=("2024-01-01", "2024-01-04"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"DATE column count comparison passed: {stats.final_score:.2f}%")

    def test_counts_with_timestamp_column(self, oracle_engine, postgres_engine):
        """Test count comparison using TIMESTAMP column type"""
        table_name = "test_various_date_types"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_timestamp",  # TIMESTAMP type
            date_range=("2024-01-01", "2024-01-04"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"TIMESTAMP column count comparison passed: {stats.final_score:.2f}%")

    def test_counts_with_datetime_column(self, oracle_engine, postgres_engine):
        """Test count comparison using DATETIME column type (TIMESTAMP in Oracle)"""
        table_name = "test_various_date_types"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_datetime",  # DATETIME/TIMESTAMP type
            date_range=("2024-01-01", "2024-01-04"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"DATETIME column count comparison passed: {stats.final_score:.2f}%")

    def test_counts_with_timestamptz_column(self, oracle_engine, postgres_engine):
        """
        Test count comparison using TIMESTAMP WITH TIME ZONE column type.
        Note: Oracle thin client doesn't support named time zones, uses offset.
        """
        table_name = "test_various_date_types"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_timestamp_tz",  # TIMESTAMP WITH TIME ZONE type
            date_range=("2024-01-01", "2024-01-04"),
            tolerance_percentage=0.0,
        )
        
        # This should work as both databases store timezone offset information
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"TIMESTAMP WITH TIME ZONE column count comparison passed: {stats.final_score:.2f}%")