"""
Test comparison with custom primary key specification between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS

class TestOraclePostgresCustomPrimaryKey:
    """Comparison with custom primary key specification"""
    
    @pytest.fixture(autouse=True)
    def setup_custom_pk_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup test data for custom PK test"""
        
        table_name = "test_ora_pg_custom_pk"
        
      # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    user_id NUMBER,
                    email VARCHAR2(100),
                    name VARCHAR2(100),
                    created_date DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (user_id, email, name, created_date) VALUES
                (1, 'user1@company.com', 'John Doe', DATE '2024-01-01'),
                (2, 'user1@company.com', 'Jane Smith', DATE '2024-01-02'),
                (3, 'user2@company.com', 'Bob Johnson', DATE '2024-01-03')
            """
        )
        
      # PostgreSQL setup
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    user_id INTEGER,
                    email TEXT,
                    name TEXT,
                    created_date DATE
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (user_id, email, name, created_date) VALUES
                (1, 'user1@company.com', 'John Doe', '2024-01-01'),
                (2, 'user1@company.com', 'Jane Smith', '2024-01-02'),
                (3, 'user2@company.com', 'Bob Johnson', '2024-01-03')
            """
        )
        
        yield

    def test_with_custom_primary_key(self, oracle_engine, postgres_engine):
        """
        Test comparison with custom primary key specification.
        """
        table_name = "test_ora_pg_custom_pk"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, "test"),
            target_table=DataReference(table_name, "test"),
            date_column="created_date",
            date_range=("2024-01-01", "2024-01-05"),
            custom_primary_key=["email"],  # Custom PK by email
            tolerance_percentage=5.0,
        )

      # Should detect duplicates
        assert stats.dup_source_rows > 0
        print(f"Oracle → PostgreSQL with custom PK passed: {stats.final_score:.2f}%")