"""
Test HR data comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text

from xoverrr.constants import COMPARISON_SUCCESS
from xoverrr.core import DataQualityComparator, DataReference


class TestOraclePostgresHRData:
    """HR data comparison between Oracle and PostgreSQL"""

    @pytest.fixture(autouse=True)
    def setup_hr_data(self, oracle_engine, postgres_engine, table_helper):
        """Setup HR test data"""

        table_name = 'test_ora_pg_hr'

        # Oracle setup
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    employee_id NUMBER PRIMARY KEY,
                    first_name VARCHAR2(50),
                    last_name VARCHAR2(50),
                    email VARCHAR2(100),
                    hire_date DATE,
                    salary NUMBER(10,2),
                    department_id NUMBER
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} 
                (employee_id, first_name, last_name, email, hire_date, salary, department_id) 
                VALUES
                (101, 'John', 'Doe', 'john.doe@company.com', DATE '2020-01-15', 60000.00, 10),
                (102, 'Jane', 'Smith', 'jane.smith@company.com', DATE '2019-03-20', 75000.00, 20)
            """,
        )

        # PostgreSQL setup
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    employee_id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    hire_date DATE,
                    salary NUMERIC(10,2),
                    department_id INTEGER
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} 
                (employee_id, first_name, last_name, email, hire_date, salary, department_id) 
                VALUES
                (101, 'John', 'Doe', 'john.doe@company.com', '2020-01-15', 60000.00, 10),
                (102, 'Jane', 'Smith', 'jane.smith@company.com', '2019-03-20', 75000.00, 20)
            """,
        )

        yield

    def test_hr_data_comparison(self, oracle_engine, postgres_engine):
        """
        Test HR data comparison between Oracle and PostgreSQL.
        """
        table_name = 'test_ora_pg_hr'

        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='hire_date',
            update_column=None,
            date_range=('2018-01-01', '2022-01-01'),
            exclude_recent_hours=1,
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        assert stats.final_diff_score == 0.0
        print(
            f'Oracle   PostgreSQL HR data comparison passed: {stats.final_score:.2f}%'
        )

    def test_hr_data_comparison_uppercase(self, oracle_engine, postgres_engine):
        # pytest.skip("issue #31")
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone='Europe/Athens',
        )
        table_name = 'test_ora_pg_hr'

        status, report, stats, details = comparator.compare_sample(
            source_table=DataReference(table_name, 'test'),
            target_table=DataReference(table_name, 'test'),
            date_column='HIRE_DATE',
            update_column=None,
            custom_primary_key=['EMPLOYEE_ID'],
            date_range=('2018-01-01', '2022-01-01'),
            exclude_recent_hours=1,
            tolerance_percentage=0.0,
        )

        assert status == COMPARISON_SUCCESS
        print(f'Custom query comparison passed: {stats.final_score:.2f}%')
