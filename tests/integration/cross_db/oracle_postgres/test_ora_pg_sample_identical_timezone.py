"""
Test timestamp with timezone comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestTimestampWithTimezone:
    """Tests for timestamp with timezone comparison"""
    
    @pytest.fixture(autouse=True)
    def setup_timestamp_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup timestamp test data"""
        
        table_name = "test_timestamps"
        
      # Oracle
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP,
                    description VARCHAR2(100)
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, created_at, updated_at, description) VALUES
                (1, TIMESTAMP '2024-01-01 15:00:00', TIMESTAMP '2024-01-01 15:00:00', 'First record'),
                (2, TIMESTAMP '2024-01-02 15:30:00', TIMESTAMP '2024-01-02 15:30:00', 'Second record'),
                (3, TIMESTAMP '2024-01-03 10:45:00', TIMESTAMP '2024-01-03 10:45:00', 'Third record')
            """
        )
        
      # PostgreSQL
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ,
                    description TEXT
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, created_at, updated_at, description) VALUES
                (1, '2024-01-01 10:00:00 +00:00', '2024-01-01 10:00:00 +00:00', 'First record'),
                (2, '2024-01-02 11:30:00 +01:00', '2024-01-02 11:30:00 +01:00', 'Second record'),
                (3, '2024-01-03 14:45:00 +09:00', '2024-01-03 14:45:00 +09:00', 'Third record')
            """
        )
        
        yield

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
        
        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(f"Timestamp with timezone comparison passed: {stats.final_score:.2f}%")