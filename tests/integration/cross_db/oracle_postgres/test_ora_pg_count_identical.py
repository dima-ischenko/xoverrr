"""
Test count-based comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS

class TestOraclePostgresCountsComparison:
    """Cross-database count-based comparison tests Oracle ↔ PostgreSQL"""
    
    @pytest.fixture(autouse=True)
    def setup_count_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup test data for count comparison"""
        
        table_name = "test_ora_pg_counts"
        
      # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id NUMBER PRIMARY KEY,
                    event_date DATE,
                    event_type VARCHAR2(50)
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, event_type) VALUES
                (1, DATE '2024-01-01', 'login'),
                (2, DATE '2024-01-01', 'purchase'),
                (3, DATE '2024-01-01', 'logout'),
                (4, DATE '2024-01-02', 'login'),
                (5, DATE '2024-01-02', 'view')
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
                    event_type TEXT
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, event_date, event_type) VALUES
                (1, '2024-01-01', 'login'),
                (2, '2024-01-01', 'purchase'),
                (3, '2024-01-01', 'logout'),
                (4, '2024-01-02', 'login'),
                (5, '2024-01-02', 'view')
            """
        )
        
        yield

    def test_counts_comparison(self, oracle_engine, postgres_engine):
        """
        Test count-based comparison between Oracle and PostgreSQL.
        """
        table_name = "test_ora_pg_counts"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        status, report, stats, details = comparator.compare_counts(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="event_date",
            date_range=("2024-01-01", "2024-01-03"),
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        assert stats.final_score == 100.0
        print(f"Oracle → PostgreSQL count comparison passed: {stats.final_score:.2f}%")