"""
Test custom query comparison between Oracle and PostgreSQL.
"""

import pytest
from sqlalchemy import text
from xoverrr.core import DataQualityComparator, DataReference
from xoverrr.constants import COMPARISON_SUCCESS


class TestCustomQueryComparison:
    """Tests for custom query comparison"""
    
    @pytest.fixture(autouse=True)
    def setup_custom_data(self, oracle_engine, postgres_engine, table_helper):
        
        table_name = "test_custom_data"
        
        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    name        varchar2(256) NOT NULL,
                    amount      number,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL,
                    is_active   integer
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, amount, created_at, updated_at, is_active) VALUES
                (1, 'Alice',  1000, date'2024-01-01', timestamp'2024-01-01 10:00:00', null),
                (2, 'Robert', 2000, date'2024-01-02', timestamp'2024-01-02 11:00:00', 0),
                (3, 'Charlie', 300000, date'2024-01-03', timestamp'2024-01-03 12:00:00', null),
                (4, 'John', null, date'2024-01-04', timestamp'2024-01-04 12:00:00', 1),
                (5, 'Kate', 50000.01, date'2024-01-04', timestamp'2024-01-05 12:00:00', 1)
            """
        )
        
        table_helper.create_table(
            engine=postgres_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    amount      numeric,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL,
                    is_active   boolean
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, amount, created_at, updated_at, is_active) VALUES
                (1, 'Alice', 1000,  '2024-01-01', '2024-01-01 10:00:00', null),
                (2, 'Robert', 2000, '2024-01-02', '2024-01-02 11:00:00', false),
                (3, 'Charlie', 300000, '2024-01-03', '2024-01-03 12:00:00', null),
                (4, 'John', null, date'2024-01-04', timestamp'2024-01-04 12:00:00', true),
                (5, 'Kate', 50000.01, date'2024-01-04', timestamp'2024-01-05 12:00:00', true)
            """
        )
        
        yield

    def test_custom_query_comparison_char_ts(self, oracle_engine, postgres_engine):

        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        source_query = """
            SELECT id, name, created_at
            FROM test.test_custom_data
            WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """
        
        target_query = """
            SELECT id, name, created_at
            FROM test.test_custom_data
            WHERE created_at >= date_trunc('day', %(start_date)s::date)
              AND created_at < date_trunc('day', %(end_date)s::date) + interval '1 days'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-05'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-05'},
            custom_primary_key=["id"],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f"Custom query comparison passed: {stats.final_score:.2f}%")      

    def test_custom_query_comparison_char_uppercase_pk(self, oracle_engine, postgres_engine):
        pytest.skip("issue #37")
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        source_query = """
            SELECT id, name
            FROM test.test_custom_data
            WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """
        
        target_query = """
            SELECT id, name
            FROM test.test_custom_data
            WHERE created_at >= date_trunc('day', %(start_date)s::date)
              AND created_at < date_trunc('day', %(end_date)s::date) + interval '1 days'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-05'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-05'},
            custom_primary_key=["ID"],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f"Custom query comparison passed: {stats.final_score:.2f}%")         

    def test_custom_query_comparison_numeric(self, oracle_engine, postgres_engine):
        pytest.skip("issue #29")

        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        source_query = """
            SELECT id, amount
            FROM test.test_custom_data
            WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """
        
        target_query = """
            SELECT id, amount
            FROM test.test_custom_data
            WHERE created_at >= date_trunc('day', %(start_date)s::date)
              AND created_at < date_trunc('day', %(end_date)s::date) + interval '1 days'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-05'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-05'},
            custom_primary_key=["id"],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f"Custom query comparison passed: {stats.final_score:.2f}%")

    def test_custom_query_comparison_bool(self, oracle_engine, postgres_engine):
        pytest.skip("issue #29")
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        source_query = """
            SELECT id, is_active
            FROM test.test_custom_data
            WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """
        
        target_query = """
            SELECT id, is_active
            FROM test.test_custom_data
            WHERE created_at >= date_trunc('day', %(start_date)s::date)
              AND created_at < date_trunc('day', %(end_date)s::date) + interval '1 days'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-04'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-04'},
            custom_primary_key=["id"],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f"Custom query comparison passed: {stats.final_score:.2f}%")       

    def test_custom_query_comparison_asterisk(self, oracle_engine, postgres_engine):
        pytest.skip("issue #29")
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        source_query = """
            SELECT *
            FROM test.test_custom_data
            WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
        """
        
        target_query = """
            SELECT *
            FROM test.test_custom_data
            WHERE created_at >= date_trunc('day', %(start_date)s::date)
              AND created_at < date_trunc('day', %(end_date)s::date) + interval '1 days'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-04'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-04'},
            custom_primary_key=["id"],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f"Custom query comparison passed: {stats.final_score:.2f}%")         

    def test_custom_query_comparison_like_filter(self, oracle_engine, postgres_engine):
        pytest.skip("issue #30")
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=postgres_engine,
            timezone="Europe/Athens",
        )

        source_query = """
            SELECT id, name, created_at
            FROM test.test_custom_data
            WHERE created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
              AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1
              and name like '%lice%'
        """
        
        target_query = """
              SELECT *
            FROM test.test_custom_data
            WHERE created_at >= date_trunc('day', %(start_date)s::date)
              AND created_at < date_trunc('day', %(end_date)s::date) + interval '1 days'
              and name like '%lice%'
        """

        status, report, stats, details = comparator.compare_custom_query(
            source_query=source_query,
            source_params={'start_date': '2024-01-01', 'end_date': '2024-01-04'},
            target_query=target_query,
            target_params={'start_date': '2024-01-01', 'end_date': '2024-01-04'},
            custom_primary_key=["id"],
            tolerance_percentage=0.0,
        )
        print(report)
        assert status == COMPARISON_SUCCESS
        print(f"Custom query comparison passed: {stats.final_score:.2f}%")