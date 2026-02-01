import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS

class TestClickHouseOracleIdenticalData:
    """Cross-database identical data sample comparison tests"""
    
    @pytest.fixture(autouse=True)
    def setup_identical_data(self, clickhouse_engine, oracle_engine, table_helper):
        """Setup identical test data for ClickHouse vs Oracle"""
        
        table_name = "test_ch_ora_identical"
        
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    name String,
                    amount Decimal(15,2),
                    transaction_date Date,
                    updated_at DateTime,
                    is_active UInt8,
                    category Nullable(String)
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                        INSERT INTO {table_name} VALUES
                        (1, 'Transaction A', 1000.50, '2024-01-01', '2024-01-01 10:00:00', 1, 'Category 1'),
                        (2, 'Transaction B', 2500.75, '2024-01-02', '2024-01-02 11:30:00', 1, 'Category 2'),
                        (3, 'Transaction C', 500.00, '2024-01-03', '2024-01-03 14:45:00', 0, NULL),
                        (4, 'Transaction D', 750.25, '2024-01-04', '2024-01-04 09:15:00', 1, 'Category 3')
                    """
        )
                
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                        CREATE TABLE {table_name} (
                            id NUMBER PRIMARY KEY,
                            name VARCHAR2(100),
                            amount NUMBER(15,2),
                            transaction_date DATE,
                            updated_at TIMESTAMP,
                            is_active NUMBER(1) CHECK (is_active IN (0, 1)),
                            category VARCHAR2(100)
                        )
                    """,
            insert_sql=f"""
                -- implicitly assume Europe/Athens tz
                INSERT INTO {table_name} VALUES
                (1, 'Transaction A', 1000.50, DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00', 1, 'Category 1'),
                (2, 'Transaction B', 2500.75, DATE '2024-01-02', TIMESTAMP '2024-01-02 13:30:00', 1, 'Category 2'),
                (3, 'Transaction C', 500.00, DATE '2024-01-03', TIMESTAMP '2024-01-03 16:45:00', 0, NULL),
                (4, 'Transaction D', 750.25, DATE '2024-01-04', TIMESTAMP '2024-01-04 11:15:00', 1, 'Category 3')
            """
        )
        
        yield
        

    def test_identical_data_sample_comparison(self, clickhouse_engine, oracle_engine):
        """
        Test sample comparison between identical data in ClickHouse and Oracle.
        """
        table_name = "test_ch_ora_identical"
        
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone="Europe/Athens",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="transaction_date",
            update_column="updated_at",
            date_range=("2024-01-01", "2024-01-05"),
            exclude_recent_hours=24,
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"ClickHouse   Oracle identical data comparison passed: {stats.final_score:.2f}%")