import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestClickHousePostgresCountsWithVariousDateTypes:
    """Cross-database count-based comparison tests with various date/time types"""
    
    @pytest.fixture(autouse=True)
    def setup_various_date_type_data(self, clickhouse_engine, postgres_engine, table_helper):
        """Setup test data with various date/time column types"""
        
        table_name = "test_ch_pg_date_types"
        
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
                    event_datetime TIMESTAMP,
                    event_datetime64 TIMESTAMP,  
                    event_type TEXT
                )
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
            """
        )
        
        yield

    def test_counts_with_date_column(self, clickhouse_engine, postgres_engine):
        """Test count comparison using ClickHouse Date column type"""
        table_name = "test_ch_pg_date_types"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",  # ClickHouse Date type
            date_range=("2024-01-01", "2024-01-04"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"ClickHouse Date column count comparison passed: {stats.final_score:.2f}%")

    def test_counts_with_datetime_column(self, clickhouse_engine, postgres_engine):
        """Test count comparison using ClickHouse DateTime column type"""
        table_name = "test_ch_pg_date_types"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_datetime",  # ClickHouse DateTime type
            date_range=("2024-01-01", "2024-01-04"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"ClickHouse DateTime column count comparison passed: {stats.final_score:.2f}%")

    def test_counts_with_datetime64_column(self, clickhouse_engine, postgres_engine):
        """Test count comparison using ClickHouse DateTime64 column type"""
        table_name = "test_ch_pg_date_types"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_datetime64",  # ClickHouse DateTime64 type
            date_range=("2024-01-01", "2024-01-04"),
            tolerance_percentage=0.0,
        )
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"ClickHouse DateTime64 column count comparison passed: {stats.final_score:.2f}%")