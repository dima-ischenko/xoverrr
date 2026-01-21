
import pytest
from sqlalchemy import text


from src import (
    DataQualityComparator,
    DataReference,
    COMPARISON_SUCCESS,
    COMPARISON_FAILED,
    COMPARISON_SKIPPED,
)

class TestDataTypesComparison:
    """
    Integration tests for comparing different data types between databases.
    Test data is created directly in test cases.
    """

    @pytest.fixture(autouse=True)
    def setup_test_data(self, oracle_engine, postgres_engine, db_cleanup):
        """Setup test data for data types tests"""
        # Oracle test data
        with oracle_engine.begin() as conn:
            # Clean up if exists
            conn.execute(text("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_timestamps CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))

            
            # Create test table
            conn.execute(text("""
                CREATE TABLE test_timestamps (
                    id NUMBER PRIMARY KEY,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP,
                    description VARCHAR2(100)
                )
            """))
            
            # Insert test data (implicitly in +05:00)
            conn.execute(text("""
                INSERT INTO test_timestamps (id, created_at, updated_at, description) VALUES
                (1, TIMESTAMP '2024-01-01 15:00:00', TIMESTAMP '2024-01-01 15:00:00', 'First record'),
                (2, TIMESTAMP '2024-01-02 15:30:00', TIMESTAMP '2024-01-02 15:30:00', 'Second record'),
                (3, TIMESTAMP '2024-01-03 10:45:00', TIMESTAMP '2024-01-03 10:45:00', 'Third record')
            """))
            
            # Create test_dates table
            conn.execute(text("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_dates CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))

            conn.execute(text("""
                CREATE TABLE test_dates (
                    id NUMBER PRIMARY KEY,
                    event_date DATE NOT NULL,
                    event_name VARCHAR2(100)
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_dates (id, event_date, event_name) VALUES
                (1, DATE '2024-01-01', 'New Year'),
                (2, DATE '2024-01-02', 'Second day'),
                (3, DATE '2024-01-03', 'Third day')
            """))
        
        # PostgreSQL test data  
        with postgres_engine.begin() as conn:
            # Clean up
            conn.execute(text("DROP TABLE IF EXISTS test_timestamps CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS test_dates CASCADE"))
            
            # Create test table
            conn.execute(text("""
                CREATE TABLE test_timestamps (
                    id INTEGER PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ,
                    description TEXT
                )
            """))
            
            # Insert test data
            conn.execute(text("""
                INSERT INTO test_timestamps (id, created_at, updated_at, description) VALUES
                (1, '2024-01-01 10:00:00 +00:00', '2024-01-01 10:00:00 +00:00', 'First record'),
                (2, '2024-01-02 11:30:00 +01:00', '2024-01-02 11:30:00 +01:00', 'Second record'),
                (3, '2024-01-03 14:45:00 +09:00', '2024-01-03 14:45:00 +09:00', 'Third record')
            """))
            
            # Create test_dates table
            conn.execute(text("""
                CREATE TABLE test_dates (
                    id INTEGER PRIMARY KEY,
                    event_date DATE NOT NULL,
                    event_name TEXT
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_dates (id, event_date, event_name) VALUES
                (1, '2024-01-01', 'New Year'),
                (2, '2024-01-02', 'Second day'),
                (3, '2024-01-03', 'Third day')
            """))
        
        yield
        
        # Cleanup будет выполнен через db_cleanup фикстуру если нужно

    def test_timestamp_with_timezone(self, oracle_engine, postgres_engine):
        """
        Compare timestamp with timezone between Oracle and PostgreSQL.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="+05:00",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_timestamps", "test"),
            target_table=DataReference("test_timestamps", "test"),
            date_column="created_at",
            update_column="updated_at",
            date_range=("2024-01-01", "2024-01-31"),
            tolerance_percentage=0.0,
            exclude_recent_hours=24,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        
        print(f"✓ Timestamp with timezone comparison passed: {stats.final_score:.2f}%")

    def test_date_type_comparison(self, oracle_engine, postgres_engine):
        """
        Compare DATE type between Oracle and PostgreSQL.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_dates", "test"),
            target_table=DataReference("test_dates", "test"),
            date_column="event_date",
            date_range=("2024-01-01", "2024-01-10"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"✓ Date type comparison passed: {stats.final_score:.2f}%")


class TestBooleanComparison:
    """Tests for boolean type comparison"""
    
    @pytest.fixture(autouse=True)
    def setup_boolean_data(self, oracle_engine, postgres_engine):
        """Setup boolean test data"""
        # Oracle: boolean as NUMBER(1)
        with oracle_engine.begin() as conn:
            conn.execute(text("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_booleans CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))
            
            conn.execute(text("""
                CREATE TABLE test_booleans (
                    id NUMBER PRIMARY KEY,
                    is_active NUMBER(1) CHECK (is_active IN (0, 1)),
                    created_at DATE
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_booleans (id, is_active, created_at) VALUES
                (1, 1, DATE '2024-01-01'),
                (2, 0, DATE '2024-01-02'),
                (3, 1, DATE '2024-01-03')
            """))
        
        # PostgreSQL: boolean as BOOLEAN
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_booleans CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_booleans (
                    id INTEGER PRIMARY KEY,
                    is_active BOOLEAN,
                    created_at DATE
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_booleans (id, is_active, created_at) VALUES
                (1, TRUE, '2024-01-01'),
                (2, FALSE, '2024-01-02'),
                (3, TRUE, '2024-01-03')
            """))
        
        yield

    def test_boolean_comparison(self, oracle_engine, postgres_engine):
        """
        Compare boolean values between Oracle (0/1) and PostgreSQL (TRUE/FALSE).
        Adapters should handle type conversion.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_booleans", "test"),
            target_table=DataReference("test_booleans", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )

        # Adapters should convert both to string representation
        assert status == COMPARISON_SUCCESS
        print(f"✓ Boolean comparison passed: {stats.final_score:.2f}%")


class TestNumericTypesComparison:
    """Tests for numeric type comparison"""
    
    @pytest.fixture(autouse=True)
    def setup_numeric_data(self, oracle_engine, postgres_engine):
        """Setup numeric test data"""
        # Oracle
        with oracle_engine.begin() as conn:
            conn.execute(text("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_numerics CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))
            
            conn.execute(text("""
                CREATE TABLE test_numerics (
                    id NUMBER PRIMARY KEY,
                    price NUMBER(10,2),
                    quantity INTEGER,
                    discount FLOAT,
                    created_at DATE
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_numerics (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, DATE '2024-01-01'),
                (2, 250.75, 5, 0.15, DATE '2024-01-02'),
                (3, 99.99, 20, 0.05, DATE '2024-01-03')
            """))
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_numerics CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_numerics (
                    id INTEGER PRIMARY KEY,
                    price NUMERIC(10,2),
                    quantity INTEGER,
                    discount DOUBLE PRECISION,
                    created_at DATE
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_numerics (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, '2024-01-01'),
                (2, 250.75, 5, 0.15, '2024-01-02'),
                (3, 99.99, 20, 0.05, '2024-01-03')
            """))
        
        yield

    def test_numeric_types_comparison(self, oracle_engine, postgres_engine):
        """
        Compare numeric types: Oracle NUMBER vs PostgreSQL NUMERIC.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_numerics", "test"),
            target_table=DataReference("test_numerics", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"✓ Numeric types comparison passed: {stats.final_score:.2f}%")


class TestPostgresSelfComparison:
    """
    Tests comparing PostgreSQL with itself (same engine).
    """

    
    @pytest.fixture(autouse=True)
    def setup_postgres_view(self, postgres_engine):
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_custom_data2 CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_custom_data2 (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_custom_data2 (id, name, created_at, updated_at) VALUES
                (1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
                (2, 'Robert',  '2024-01-02', '2024-01-02 11:00:00'),
                (3, 'Charlie', '2024-01-03', '2024-01-03 12:00:00')
            """))

        """Setup view for PostgreSQL self-comparison"""
        with postgres_engine.begin() as conn:
            # Create a view
            conn.execute(text("DROP VIEW IF EXISTS vtest_custom_data2 CASCADE"))
            conn.execute(text("""
                CREATE VIEW vtest_custom_data2 AS
                SELECT id, name, created_at, updated_at
                FROM test_custom_data2
            """))
        
        yield

    def test_postgres_self_comparison_identical(self, postgres_engine):
        """
        Compare identical tables within same PostgreSQL database.
        """
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_custom_data2", "test"),
            target_table=DataReference("test_custom_data2", "test"),
            date_column="created_at",
            update_column="updated_at",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"PostgreSQL self-comparison passed: {stats.final_score:.2f}%")

    def test_postgres_table_vs_view(self, postgres_engine):
        """
        Compare PostgreSQL table with view on the same data.
        """
        comparator = DataQualityComparator(
            source_engine=postgres_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_custom_data2", "test"),  # таблица
            target_table=DataReference("vtest_custom_data2", "test"),  # вьюха
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"✓ PostgreSQL table vs view comparison passed: {stats.final_score:.2f}%")

class TestClickHouseNumericTypes:
    """Tests for ClickHouse numeric types comparison with PostgreSQL"""
    
    @pytest.fixture(autouse=True)
    def setup_clickhouse_numeric_data(self, clickhouse_engine, postgres_engine):
        """Setup numeric test data for ClickHouse vs PostgreSQL"""
        # ClickHouse
        with clickhouse_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_ch_numerics"))
            
            conn.execute(text("""
                CREATE TABLE test_ch_numerics (
                    id UInt32,
                    price Decimal(10,2),
                    quantity UInt32,
                    discount Float64,
                    created_at Date
                )
                ENGINE = MergeTree()
                ORDER BY id
            """))
            
            conn.execute(text("""
                INSERT INTO test_ch_numerics (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, '2024-01-01'),
                (2, 250.75, 5, 0.15, '2024-01-02'),
                (3, 99.99, 20, 0.05, '2024-01-03')
            """))
        
        # PostgreSQL
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_ch_numerics CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE test_ch_numerics (
                    id INTEGER PRIMARY KEY,
                    price NUMERIC(10,2),
                    quantity INTEGER,
                    discount DOUBLE PRECISION,
                    created_at DATE
                )
            """))
            
            conn.execute(text("""
                INSERT INTO test_ch_numerics (id, price, quantity, discount, created_at) VALUES
                (1, 100.50, 10, 0.1, '2024-01-01'),
                (2, 250.75, 5, 0.15, '2024-01-02'),
                (3, 99.99, 20, 0.05, '2024-01-03')
            """))
        
        yield

    def test_clickhouse_numeric_types_comparison(self, clickhouse_engine, postgres_engine):
        """
        Compare numeric types between ClickHouse and PostgreSQL.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=postgres_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_ch_numerics", "test_db"),
            target_table=DataReference("test_ch_numerics", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-05"),
            tolerance_percentage=0.0,
        )
        print(report)

        assert status == COMPARISON_SUCCESS
        print(f"✓ ClickHouse numeric types comparison passed: {stats.final_score:.2f}%")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])