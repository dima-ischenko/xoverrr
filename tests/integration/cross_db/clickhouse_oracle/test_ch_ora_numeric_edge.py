"""
Test numeric type comparison between ClickHouse and Oracle.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS, COMPARISON_FAILED
from xoverrr.core import DataQualityComparator, DataReference


class TestClickHouseOracleNumericEdge:
    """Tests for numeric type comparison between ClickHouse and Oracle"""

    @pytest.fixture
    def numeric_large_data(self, clickhouse_engine, oracle_engine, table_helper):
        """Setup numeric test data with large values for ClickHouse-Oracle"""
        table_name = 'test_ch_ora_numeric_large'

        # ClickHouse setup - uses Decimal and Float types
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    large_id Decimal(45, 20),
                    created_at Date default '2024-01-01'
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, large_id) VALUES
                (1, 11112222333344445),
                (2, 0.1111222233334444),
                (3, 11112222333344445.012345),
                (4, -11112222333344445),
                (5, -0.1111222233334444)
            """,
        )

        # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    large_id NUMBER,
                    created_at DATE default DATE '2024-01-01'
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, large_id) VALUES
                (1, 11112222333344445),
                (2, 0.1111222233334444),
                (3, 11112222333344445.012345),
                (4, -11112222333344445),
                (5, -0.1111222233334444)
            """,
        )

        yield table_name

    @pytest.fixture
    def numeric_scientific_data(self, clickhouse_engine, oracle_engine, table_helper):
        """Setup test data for scientific notation for ClickHouse-Oracle"""
        table_name = 'test_ch_ora_numeric_scientific'
        
        # Test values that might trigger scientific notation
        test_values = [
            (1, 1e-10, 0.0000000001),
            (2, 1e-15, 0.000000000000001),
            (3, 1e-20, 0.00000000000000000001),
            (4, 1e+20, 100000000000000000000),
            (5, 1e-20, 1e-20),  
            (6, 1e+30, 1e+20),  
        ]
        
        # ClickHouse setup
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    scientific Decimal(55, 20),
                    decimal_form Decimal(55, 20),
                    created_at Date default '2024-01-01'
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, scientific, decimal_form) VALUES
                {', '.join([f"({id}, {sci}, {dec})" for id, sci, dec in test_values])}
            """,
        )
        
        # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    scientific NUMBER,
                    decimal_form NUMBER,
                    created_at DATE default DATE '2024-01-01'
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, scientific, decimal_form) VALUES
                {', '.join([f"({id}, {sci}, {dec})" for id, sci, dec in test_values])}
            """,
        )
        
        yield table_name

    @pytest.fixture
    def numeric_edge_precision_data(self, clickhouse_engine, oracle_engine, table_helper):
        """Setup test data for edge cases of numeric precision for ClickHouse-Oracle"""
        table_name = 'test_ch_ora_numeric_edge_precision'
        
        max_precision = 20
        max_digits = '9' * max_precision
        
        test_cases = [
            (1, f"{max_digits}", f"{max_digits}.0"),  # Max precision integer
            (2, f"0.{max_digits}", f"0.{max_digits}"),  # Max precision decimal
            (3, f"{max_digits}.{max_digits[:10]}", f"{max_digits}.{max_digits[:10]}"),  # Mixed
            (4, "123456789012345678901234567890.123456789", None),  # digits + decimals
            (5, "0.0000000000000001", None),  # Very small
        ]
        
        # ClickHouse setup
        ch_insert_values = []
        for id, val1, val2 in test_cases:
            if val2:
                ch_insert_values.append(f"({id}, {val1}, {val2})")
            else:
                ch_insert_values.append(f"({id}, {val1}, NULL)")
        
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    high_precision Decimal(55, 20),
                    another_precision Nullable(Decimal(55, 20)),
                    created_at Date default '2024-01-01'
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, high_precision, another_precision) VALUES
                {', '.join(ch_insert_values)}
            """,
        )
        
        # Oracle setup
        oracle_insert_values = []
        for id, val1, val2 in test_cases:
            if val2:
                oracle_insert_values.append(f"({id}, {val1}, {val2})")
            else:
                oracle_insert_values.append(f"({id}, {val1}, NULL)")
        
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    high_precision NUMBER,
                    another_precision NUMBER,
                    created_at DATE default DATE '2024-01-01'
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, high_precision, another_precision) VALUES
                {', '.join(oracle_insert_values)}
            """,
        )
        
        yield table_name

    @pytest.fixture
    def numeric_arithmetic_data(self, clickhouse_engine, oracle_engine, table_helper):
        """Setup test data for arithmetic operations for ClickHouse-Oracle"""
        table_name = 'test_ch_ora_numeric_arithmetic'
        
        # ClickHouse setup
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    a Int32,
                    b Int32,
                    c Int32,
                    created_at Date default '2024-01-01'
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, a, b, c) VALUES
                (1, 100, 3, 7),
                (2, 250, 5, 2),
                (3, 99, 4, 9),
                (4, 1000, 10, 5),
                (5, 500, 2, 8)
            """,
        )
        
        # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    a NUMBER,
                    b NUMBER,
                    c NUMBER,
                    created_at DATE default DATE '2024-01-01'
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, a, b, c) VALUES
                (1, 100, 3, 7),
                (2, 250, 5, 2),
                (3, 99, 4, 9),
                (4, 1000, 10, 5),
                (5, 500, 2, 8)
            """,
        )
        
        yield table_name

    @pytest.fixture
    def numeric_null_data(self, clickhouse_engine, oracle_engine, table_helper):
        """Setup test data for NULL handling in numeric columns for ClickHouse-Oracle"""
        table_name = 'test_ch_ora_numeric_nulls'
        
        # ClickHouse setup - Nullable types for NULL support
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    value1 Nullable(Int32),
                    value2 Nullable(Int32),
                    value3 Nullable(Int32),
                    created_at Date default '2024-01-01'
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, value1, value2, value3) VALUES
                (1, NULL, NULL, NULL),
                (2, 100, NULL, 300),
                (3, NULL, 200, 300),
                (4, 100, 200, NULL),
                (5, NULL, NULL, 300)
            """,
        )
        
        # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    value1 NUMBER,
                    value2 NUMBER,
                    value3 NUMBER,
                    created_at DATE default DATE '2024-01-01'
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, value1, value2, value3) VALUES
                (1, NULL, NULL, NULL),
                (2, 100, NULL, 300),
                (3, NULL, 200, 300),
                (4, 100, 200, NULL),
                (5, NULL, NULL, 300)
            """,
        )
        
        yield table_name

    @pytest.fixture
    def numeric_decimal_precision_data(self, clickhouse_engine, oracle_engine, table_helper):
        """Setup test data for Decimal precision handling in ClickHouse vs Oracle NUMBER"""
        table_name = 'test_ch_ora_decimal_precision'
        
        # Test cases with different decimal precisions
        test_cases = [
            (1, "123.45", "123.45"),
            (2, "0.0000000001", "0.0000000001"),
            (3, "9999999999.9999999999", None),  # Very high precision
            (4, "-1234567890.123456789", "-1234567890.123456789"),
            (5, "12345678901234567890.12345678901234567890", None),  # 20+ digits
        ]
        
        # ClickHouse setup with high precision Decimal
        ch_insert_values = []
        for id, val1, val2 in test_cases:
            if val2:
                ch_insert_values.append(f"({id}, {val1}, {val2})")
            else:
                ch_insert_values.append(f"({id}, {val1}, NULL)")
        
        table_helper.create_table(
            engine=clickhouse_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id UInt32,
                    high_precision Decimal(55, 20),
                    medium_precision Nullable(Decimal(28, 10)),
                    created_at Date default '2024-01-01'
                )
                ENGINE = MergeTree()
                ORDER BY id
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, high_precision, medium_precision) VALUES
                {', '.join(ch_insert_values)}
            """,
        )
        
        # Oracle setup with NUMBER (unlimited precision)
        oracle_insert_values = []
        for id, val1, val2 in test_cases:
            if val2:
                oracle_insert_values.append(f"({id}, {val1}, {val2})")
            else:
                oracle_insert_values.append(f"({id}, {val1}, NULL)")
        
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    high_precision NUMBER,
                    medium_precision NUMBER,
                    created_at DATE default DATE '2024-01-01'
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, high_precision, medium_precision) VALUES
                {', '.join(oracle_insert_values)}
            """,
        )
        
        yield table_name

    def test_numeric_types_large_comparison(
        self, clickhouse_engine, oracle_engine, numeric_large_data
    ):
        """
        Compare numeric types with large values between ClickHouse and Oracle.
        ClickHouse uses Decimal, Oracle uses NUMBER.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='UTC',  # Use UTC for consistent comparison
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_large_data, 'test'),
            target_table=DataReference(numeric_large_data, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )
        print(report)

        assert status == COMPARISON_SUCCESS
        print(f"Large numeric values test passed: {stats.final_score:.2f}%")

    def test_numeric_scientific_notation(
        self, clickhouse_engine, oracle_engine, numeric_scientific_data
    ):
        """
        Test that scientific notation is handled correctly between ClickHouse and Oracle.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='UTC',
        )
        
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_scientific_data, 'test'),
            target_table=DataReference(numeric_scientific_data, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )
        
        print(report)
        
        assert status == COMPARISON_SUCCESS
        print(f"Scientific notation test passed: {stats.final_score:.2f}%")

    def test_numeric_edge_precision(
        self, clickhouse_engine, oracle_engine, numeric_edge_precision_data
    ):
        """
        Test edge cases of numeric precision between ClickHouse and Oracle:
        - Maximum precision numbers
        - Numbers with many decimal places
        - Very small numbers
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='UTC',
        )
        
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_edge_precision_data, 'test'),
            target_table=DataReference(numeric_edge_precision_data, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )
        
        print(report)
        
        assert status == COMPARISON_SUCCESS
        print(f"Edge precision test passed: {stats.final_score:.2f}%")

    def test_numeric_with_arithmetic_operations(
        self, clickhouse_engine, oracle_engine, numeric_arithmetic_data
    ):
        """
        Test that arithmetic operations in queries produce consistent results
        between ClickHouse and Oracle.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='UTC',
        )
        
        # Compare arithmetic expressions - note ClickHouse and Oracle have different syntax
        status, report, stats, details = comparator.compare_custom_query(
            source_query=f"""
                SELECT 
                    id,
                    a + b as sum_ab,
                    a - b as diff_ab,
                    a * b as mul_ab,
                    round(a / b, 10) as div_ab,
                    (a + b) * c as complex1,
                    round((a * c) / b, 10) as complex2
                FROM {numeric_arithmetic_data}
                WHERE created_at >= toDate('2024-01-01')
            """,
            source_params={},
            target_query=f"""
                SELECT 
                    id,
                    a + b as sum_ab,
                    a - b as diff_ab,
                    a * b as mul_ab,
                    round(a / b, 10) as div_ab,
                    (a + b) * c as complex1,
                    round((a * c) / b, 10) as complex2
                FROM {numeric_arithmetic_data}
                WHERE created_at >= DATE '2024-01-01'
            """,
            target_params={},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        
        print("\n" + "="*80)
        print("ARITHMETIC OPERATIONS TEST - ClickHouse vs Oracle")
        print("="*80)
        print(report)
        
        assert status == COMPARISON_SUCCESS
        print(f"Arithmetic operations test passed: {stats.final_score:.2f}%")

    def test_numeric_null_handling(
        self, clickhouse_engine, oracle_engine, numeric_null_data
    ):
        """
        Test that NULL values in numeric columns are handled consistently
        between ClickHouse and Oracle.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='UTC',
        )
        
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_null_data, 'test'),
            target_table=DataReference(numeric_null_data, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )
        
        print("\n" + "="*80)
        print("NUMERIC NULL HANDLING TEST - ClickHouse vs Oracle")
        print("="*80)
        print(report)
        
        assert status == COMPARISON_SUCCESS
        assert stats.total_matched_rows == stats.common_pk_rows
        print(f"NULL handling test passed: {stats.final_score:.2f}%")

    def test_decimal_precision_handling(
        self, clickhouse_engine, oracle_engine, numeric_decimal_precision_data
    ):
        """
        Test Decimal precision handling between ClickHouse Decimal and Oracle NUMBER.
        ClickHouse has fixed precision Decimal, Oracle NUMBER has arbitrary precision.
        """
        comparator = DataQualityComparator(
            source_engine=clickhouse_engine,
            target_engine=oracle_engine,
            timezone='UTC',
        )
        
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_decimal_precision_data, 'test'),
            target_table=DataReference(numeric_decimal_precision_data, 'test'),
            date_column='created_at',
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )
        
        print("\n" + "="*80)
        print("DECIMAL PRECISION HANDLING TEST - ClickHouse vs Oracle")
        print("="*80)
        print(report)
        
        # Note: Some precision differences may exist between ClickHouse Decimal and Oracle NUMBER
        # Consider tolerance if needed
        assert status == COMPARISON_SUCCESS
        print(f"Decimal precision test passed: {stats.final_score:.2f}%")