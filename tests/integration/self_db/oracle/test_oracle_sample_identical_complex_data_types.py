"""
Test Oracle self-comparison with complex data types.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS, COMPARISON_SKIPPED


class TestOracleComplexDataTypes:
    """
    Tests for Oracle self-comparison with various data types.
    """
    
    @pytest.fixture(autouse=True)
    def setup_oracle_complex_data(self, oracle_engine, table_helper):
        """Setup Oracle test data with complex types for self-comparison"""
        
        table_name = "test_oracle_complex"
        
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id               NUMBER PRIMARY KEY,
                    varchar_col      VARCHAR2(200),
                    number_col       NUMBER(10,3),
                    date_col         DATE,
                    timestamp_col    TIMESTAMP,
                    timestamp_tz_col TIMESTAMP WITH TIME ZONE,
                    interval_col     INTERVAL DAY TO SECOND,
                    raw_col          RAW(50),
                    clob_col         CLOB,
                    created_at       DATE NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (
                    id, varchar_col, number_col, date_col, timestamp_col,
                    timestamp_tz_col, interval_col, raw_col, clob_col, created_at
                ) VALUES
                (1, 'Standard text', 123.456, 
                 DATE '2024-01-01', 
                 TIMESTAMP '2024-01-01 10:30:45.123456',
                 TIMESTAMP '2024-01-01 10:30:45.123456 +00:00',
                 INTERVAL '1 2:30:45' DAY TO SECOND,
                 HEXTORAW('414243'),
                 'This is a CLOB text with multiple lines
                 and special characters: !@#$%^&*()',
                 DATE '2024-01-01'),
                 
                (2, NULL, 789.012,
                 DATE '2024-01-02',
                 TIMESTAMP '2024-01-02 14:20:30.987654',
                 TIMESTAMP '2024-01-02 14:20:30.987654 +05:00',
                 INTERVAL '0 6:15:30' DAY TO SECOND,
                 HEXTORAW('444546'),
                 'Another CLOB content',
                 DATE '2024-01-02'),
                 
                (3, 'Text with emoji 😀🚀📊', 0.001,
                 DATE '2024-01-03',
                 TIMESTAMP '2024-01-03 09:15:20.555555',
                 TIMESTAMP '2024-01-03 09:15:20.555555 -08:00',
                 INTERVAL '2 12:00:00' DAY TO SECOND,
                 NULL,
                 NULL,
                 DATE '2024-01-03')
            """
        )
        
        yield

    def test_oracle_complex_types_self_comparison(self, oracle_engine):
        """
        Compare Oracle table with itself containing complex data types.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_oracle_complex", "test"),
            target_table=DataReference("test_oracle_complex", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-03"),
            exclude_columns=["raw_col"],  # Exclude RAW columns as they might not compare well
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"Oracle complex types self-comparison passed: {stats.final_score:.2f}%")

    def test_oracle_with_column_exclusions(self, oracle_engine):
        """
        Test Oracle self-comparison with excluded columns.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_oracle_complex", "test"),
            target_table=DataReference("test_oracle_complex", "test"),
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-03"),
            exclude_columns=["raw_col", "clob_col", "interval_col"],  # Exclude problematic columns
            include_columns=["id", "varchar_col", "number_col", "date_col"],  # Include specific columns
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f"Oracle with column exclusions passed: {stats.final_score:.2f}%")

    def test_oracle_empty_date_range(self, oracle_engine):
        """
        Test Oracle self-comparison with empty date range.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            timezone="UTC",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference("test_oracle_complex", "test"),
            target_table=DataReference("test_oracle_complex", "test"),
            date_column="created_at",
            date_range=("2025-01-01", "2025-01-31"),  # Future date range, should be empty
            tolerance_percentage=0.0,
        )

        # Should be skipped due to empty result
        assert status == COMPARISON_SKIPPED
        print(f"Oracle empty date range test passed: No data to compare")