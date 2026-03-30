"""
Test numeric type comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS, COMPARISON_FAILED
from xoverrr.core import DataQualityComparator, DataReference


class TestNumericTypesComparisonEdge:
    """Tests for numeric type comparison"""

    @pytest.fixture
    def numeric_large_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup numeric test data with large values"""
        table_name = 'test_types_numeric_large'

        # Oracle
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    large_id NUMBER
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, large_id) VALUES
                (1, 11112222333344445),
                (2, 0.11112222333344445),
                (3, 11112222333344445.012345),
                (4, -11112222333344445),
                (5, -0.11112222333344445)
            """,
        )

        # PostgreSQL
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    large_id NUMERIC
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, large_id) VALUES
                (1, 11112222333344445),
                (2, 0.11112222333344445),
                (3, 11112222333344445.012345),
                (4, -11112222333344445),
                (5, -0.11112222333344445)
            """,
        )

        yield table_name

    @pytest.fixture
    def numeric_scientific_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup test data for scientific notation"""
        table_name = 'test_numeric_scientific'
        
        # Test values that might trigger scientific notation
        test_values = [
            (1, 1e-10, 0.0000000001),
            (2, 1e-15, 0.000000000000001),
            (3, 1e-20, 0.00000000000000000001),
            (4, 1e+20, 100000000000000000000),
            (5, 1e-20, 1e-20),  
            (6, 1e+30, 1e+20),  
        ]
        
        # Oracle
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    scientific NUMBER,
                    decimal_form NUMBER
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, scientific, decimal_form) VALUES
                {', '.join([f"({id}, {sci}, {dec})" for id, sci, dec in test_values])}
            """,
        )
        
        # PostgreSQL
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    scientific NUMERIC,
                    decimal_form NUMERIC
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, scientific, decimal_form) VALUES
                {', '.join([f"({id}, {sci}, {dec})" for id, sci, dec in test_values])}
            """,
        )
        
        yield table_name

    @pytest.fixture
    def numeric_edge_precision_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup test data for edge cases of numeric precision"""
        table_name = 'test_numeric_edge_precision'
        
        max_precision = 30
        max_digits = '9' * max_precision
        
        test_cases = [
            (1, f"{max_digits}", f"{max_digits}.0"),  # Max precision integer
            (2, f"0.{max_digits}", f"0.{max_digits}"),  # Max precision decimal
            (3, f"{max_digits}.{max_digits[:10]}", f"{max_digits}.{max_digits[:10]}"),  # Mixed
            (4, "123456789012345678901234567890.123456789", None),  # digits + decimals
            (5, "0.00000000000000000000000000000000000001", None),  # Very small
        ]
        
        # Oracle
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
                    another_precision NUMBER
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, high_precision, another_precision) VALUES
                {', '.join(oracle_insert_values)}
            """,
        )
        
        # PostgreSQL
        pg_insert_values = []
        for id, val1, val2 in test_cases:
            if val2:
                pg_insert_values.append(f"({id}, {val1}, {val2})")
            else:
                pg_insert_values.append(f"({id}, {val1}, NULL)")
        
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    high_precision NUMERIC,
                    another_precision NUMERIC
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, high_precision, another_precision) VALUES
                {', '.join(pg_insert_values)}
            """,
        )
        
        yield table_name

    @pytest.fixture
    def numeric_arithmetic_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup test data for arithmetic operations"""
        table_name = 'test_numeric_arithmetic'
        
        # Create base table
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    a NUMBER,
                    b NUMBER,
                    c NUMBER
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
        
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    a INTEGER,
                    b INTEGER,
                    c INTEGER
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
    def numeric_null_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup test data for NULL handling in numeric columns"""
        table_name = 'test_numeric_nulls'
        
        # Test various NULL patterns with numeric columns
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    value1 NUMBER,
                    value2 NUMBER,
                    value3 NUMBER
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
        
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    value1 INTEGER,
                    value2 INTEGER,
                    value3 INTEGER
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

    def test_numeric_types_large_comparison(
        self, oracle_engine, postgres_engine, numeric_large_data
    ):
        """
        Compare numeric types with the large value (16+ digits)
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_large_data, 'test'),
            target_table=DataReference(numeric_large_data, 'test'),
            date_range=('2024-01-01', '2024-01-05'),
            tolerance_percentage=0.0,
        )
        print(report)

        assert status == COMPARISON_SUCCESS

    def test_numeric_scientific_notation(
        self, oracle_engine, postgres_engine, numeric_scientific_data
    ):
        """
        Test that scientific notation is handled correctly.
        Numbers that might be represented in scientific notation by PostgreSQL.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )
        
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_scientific_data, 'test'),
            target_table=DataReference(numeric_scientific_data, 'test'),
            tolerance_percentage=0.0,
        )
        
        print(report)
        
        assert status == COMPARISON_SUCCESS
        print(f"Scientific notation test passed: {stats.final_score:.2f}%")

    def test_numeric_edge_precision(
        self, oracle_engine, postgres_engine, numeric_edge_precision_data
    ):
        """
        Test edge cases of numeric precision:
        - Maximum Oracle NUMBER precision (38 digits)
        - Numbers with many decimal places
        - Numbers that lose precision in float
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )
        
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_edge_precision_data, 'test'),
            target_table=DataReference(numeric_edge_precision_data, 'test'),
            tolerance_percentage=0.0,
        )
        
        print(report)
        
        assert status == COMPARISON_SUCCESS
        print(f"Edge precision test passed: {stats.final_score:.2f}%")

    def test_numeric_with_arithmetic_operations(
        self, oracle_engine, postgres_engine, numeric_arithmetic_data
    ):
        """
        Test that arithmetic operations in queries produce consistent results.
        This ensures that when users use expressions in SELECT, the comparison works.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )
        
        # Compare arithmetic expressions
        status, report, stats, details = comparator.compare_custom_query(
            source_query=f"""
                SELECT 
                    id,
                    a + b as sum_ab,
                    a - b as diff_ab,
                    a * b as mul_ab,
                    round(a / b,10) as div_ab,
                    (a + b) * c as complex1,
                    round((a * c) / b,10) as complex2
                FROM {numeric_arithmetic_data}
            """,
            source_params={},
            target_query=f"""
                SELECT 
                    id,
                    a + b as sum_ab,
                    a - b as diff_ab,
                    a * b as mul_ab,
                    round(a::numeric / b,10) as div_ab,
                    (a + b) * c as complex1,
                    round((a * c) / b::numeric,10) as complex2
                FROM {numeric_arithmetic_data}
            """,
            target_params={},
            custom_primary_key=['id'],
            tolerance_percentage=0.0,
        )
        
        print("\n" + "="*80)
        print("ARITHMETIC OPERATIONS TEST")
        print("="*80)
        print(report)
        
        # Note: This test may have precision differences between Oracle and PostgreSQL
        # Consider adjusting tolerance or marking as expected failure
        assert status == COMPARISON_SUCCESS
        print(f"Arithmetic operations test passed: {stats.final_score:.2f}%")

    def test_numeric_null_handling(
        self, oracle_engine, postgres_engine, numeric_null_data
    ):
        """
        Test that NULL values in numeric columns are handled consistently.
        This complements existing NULL tests with numeric-specific cases.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )
        
        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(numeric_null_data, 'test'),
            target_table=DataReference(numeric_null_data, 'test'),
            tolerance_percentage=0.0,
        )
        
        print("\n" + "="*80)
        print("NUMERIC NULL HANDLING TEST")
        print("="*80)
        print(report)
        
        assert status == COMPARISON_SUCCESS
        assert stats.total_matched_rows == stats.common_pk_rows
        print(f"NULL handling test passed: {stats.final_score:.2f}%")