"""
Test Oracle self-comparison with identical data.
"""

import pytest
import time
import pandas as pd
from sqlalchemy import text

from xoverrr.core import DataQualityComparator

@pytest.mark.timeout(30)
class TestOracleSelfFetchPerformance:
    """
    Tests comparing Oracle with itself (same engine).
    """
    num_rows_generate =  1000
    
    @pytest.fixture(autouse=True)
    def setup_oracle_data(self, oracle_engine, table_helper):
        """Setup Oracle test data for fetching 1000*1000 records"""

        table_name = 'test_oracle_large_fetch_self'

        table_helper.create_table(
            engine=oracle_engine,
            table_name=table_name,
            create_sql=f"""
                CREATE TABLE {table_name} (
                    id          NUMBER PRIMARY KEY,
                    name        VARCHAR2(100) NOT NULL,
                    amount      NUMBER,
                    created_at  DATE NOT NULL,
                    updated_at  TIMESTAMP NOT NULL
                )
            """,
            insert_sql=f"""
                INSERT INTO {table_name} (id, name, amount, created_at, updated_at)
                WITH r AS 
                (
                SELECT LEVEL AS id
                FROM DUAL
                CONNECT BY LEVEL <= {self.num_rows_generate})
                SELECT r.id, 
                    'Item ' || r.id AS name,
                    r.id * 1000.1 AS amount,
                    date'2023-01-01' + id/24/60/60 created_at,
                    date'2023-01-01' + (id+1)/24/60/60 updated_at
                FROM r
                ORDER BY r.id desc
            """,
        )

        yield

    def test_oracle_self_fetch_large(self, oracle_engine):
        """
        Compare identical tables within same Oracle database.
        """
        comparator = DataQualityComparator(
            source_engine=oracle_engine,
            target_engine=oracle_engine,
            timezone='Europe/Athens',
        )

        query = """
                select t1.id, t1.name, t1.amount, t1.created_at, t1.updated_at,
                       t2.id id2, t2.name name2, t2.amount amount2, t2.created_at created_at2, t2.updated_at updated_at2
                  from test_oracle_large_fetch_self t1
                  cross join test.test_oracle_large_fetch_self t2
        """
        params = None

        start_time = time.time()
        df = comparator._execute_query( (query, params), comparator.source_engine,  comparator.timezone)
        execution_time = time.time() - start_time

        assert len(df) == self.num_rows_generate*self.num_rows_generate
        assert execution_time < 15
