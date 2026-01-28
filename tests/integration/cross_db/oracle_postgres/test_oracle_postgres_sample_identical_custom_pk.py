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
    def setup_custom_pk_data(self, oracle_engine, postgres_engine):
        """Setup test data for custom PK test"""
        
        table_name = "test_ora_pg_custom_pk"
        
        # Oracle setup
        with oracle_engine.begin() as conn:
            conn.execute(text(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """))
            
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    user_id NUMBER,
                    email VARCHAR2(100),
                    name VARCHAR2(100),
                    created_date DATE
                )
            """))
            
            # Add duplicate email to test custom PK
            conn.execute(text(f"""
                INSERT INTO {table_name} (user_id, email, name, created_date) VALUES
                (1, 'user1@company.com', 'John Doe', DATE '2024-01-01'),
                (2, 'user1@company.com', 'Jane Smith', DATE '2024-01-02'),
                (3, 'user2@company.com', 'Bob Johnson', DATE '2024-01-03')
            """))
        
        # PostgreSQL setup
        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    user_id INTEGER,
                    email TEXT,
                    name TEXT,
                    created_date DATE
                )
            """))
            
            conn.execute(text(f"""
                INSERT INTO {table_name} (user_id, email, name, created_date) VALUES
                (1, 'user1@company.com', 'John Doe', '2024-01-01'),
                (2, 'user1@company.com', 'Jane Smith', '2024-01-02'),
                (3, 'user2@company.com', 'Bob Johnson', '2024-01-03')
            """))
        
        yield
        
        # Cleanup
        with oracle_engine.begin() as conn:
            try:
                conn.execute(text(f"DROP TABLE {table_name} CASCADE CONSTRAINTS"))
            except:
                pass
        
        with postgres_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))

    def test_with_custom_primary_key(self, oracle_engine, postgres_engine):
        """
        Test comparison with custom primary key specification.
        """
        table_name = "test_ora_pg_custom_pk"
        
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="UTC",
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
        print(f"✓ Oracle → PostgreSQL with custom PK passed: {stats.final_score:.2f}%")