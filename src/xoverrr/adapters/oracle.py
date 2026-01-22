import pandas as pd
from typing import Optional, Dict, Callable, List, Tuple, Union
from datetime import datetime, timedelta
from ..constants import DATE_FORMAT,DATETIME_FORMAT
from .base import BaseDatabaseAdapter, Engine
from ..models import DataReference, ObjectType
from ..exceptions import QueryExecutionError
from ..logger import app_logger
import time

class OracleAdapter(BaseDatabaseAdapter):

    def _execute_query(self, query: Union[str, Tuple[str, Dict]], engine: Engine, timezone: str) -> pd.DataFrame:
        tz_set = None
        raw_conn = None
        cursor = None

        start_time = time.time()
        app_logger.info('start')

        if timezone:
            tz_set = f"alter session set time_zone = '{timezone}'"

        try:
            raw_conn = engine.raw_connection()
            cursor = raw_conn.cursor()

            if tz_set:
                app_logger.info(f'{tz_set}')
                cursor.execute(tz_set)

            cursor.arraysize = 100000

            if isinstance(query, tuple):
                query_text, params = query
                app_logger.info(f'query\n {query_text}')
                app_logger.info(f'{params=}')
                cursor.execute(query_text, params or {})
            else:
                app_logger.info(f'query\n {query}')
                cursor.execute(query)


            columns = [col[0].lower() for col in cursor.description]
            data = cursor.fetchall()

            execution_time = time.time() - start_time
            app_logger.info(f"Query executed in {execution_time:.2f}s")

            app_logger.info('complete')

            # excplicitly close cursor before closing the connection
            if cursor:
                cursor.close()

            return pd.DataFrame(data, columns=columns)

        except Exception as e:
            execution_time = time.time() - start_time
            app_logger.error(f"Query execution failed after {execution_time:.2f}s: {str(e)}")

            if raw_conn:
                try:
                    raw_conn.rollback()
                except Exception as rollback_error:
                    app_logger.warning(f"Rollback failed: {rollback_error}")
                try:
                    if cursor:
                        cursor.close()
                except Exception as close_error:
                    app_logger.warning(f"Cursor close failed: {close_error}")

            raise QueryExecutionError(f"Query failed: {str(e)}")

    def get_object_type(self, data_ref: DataReference, engine: Engine) -> ObjectType:
        """Determine if object is table or view in Oracle"""
        query = """
            SELECT
                CASE
                    WHEN object_type = 'TABLE' THEN 'table'
                    WHEN object_type = 'VIEW' THEN 'view'
                    WHEN object_type = 'MATERIALIZED VIEW' THEN 'materialized_view'
                    ELSE 'unknown'
                END as object_type
            FROM all_objects
            WHERE owner = UPPER(:schema_name)
            AND object_name = UPPER(:table_name)
        """
        params = {'schema_name': data_ref.schema, 'table_name': data_ref.name}

        try:
            result = self._execute_query((query, params), engine, None)
            if not result.empty:
                type_str = result.iloc[0]['object_type']
                return {
                    'table': ObjectType.TABLE,
                    'view': ObjectType.VIEW,
                    'materialized_view': ObjectType.MATERIALIZED_VIEW
                }.get(type_str, ObjectType.UNKNOWN)
        except Exception as e:
            app_logger.warning(f"Could not determine object type for {data_ref.full_name}: {str(e)}")

        return ObjectType.UNKNOWN

    def build_metadata_columns_query(self, data_ref: DataReference) -> pd.DataFrame:
        query = """
            SELECT
                lower(column_name) as column_name,
                lower(data_type) as data_type,
                column_id
            FROM all_tab_columns
            WHERE owner = upper(:schema_name)
            AND table_name = upper(:table_name)
            ORDER BY column_id
        """
        params = {}

        params['schema_name'] = data_ref.schema
        params['table_name'] = data_ref.name
        return query, params

    def build_primary_key_query(self, data_ref: DataReference) -> pd.DataFrame:

        #todo add suport of unique indexes when no pk?
        query = """
            SELECT lower(cols.column_name) as pk_column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON
                cols.owner = cons.owner AND
                cols.table_name = cons.table_name AND
                cols.constraint_name = cons.constraint_name
            WHERE cons.constraint_type = 'P'
            AND cons.owner = upper(:schema_name)
            AND cons.table_name = upper(:table_name)
        """
        params = {}

        params['schema_name'] = data_ref.schema
        params['table_name'] = data_ref.name
        return query, params


    def build_count_query(self, data_ref: DataReference, date_column: str,
                            start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, Dict]:
        query = f"""
            SELECT
                to_char(trunc({date_column}, 'dd'),'YYYY-MM-DD') as dt,
                count(*) as cnt
            FROM {data_ref.full_name}
            WHERE 1=1\n"""
        params = {}


        if start_date:
            query += f" AND {date_column} >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')\n"
            params['start_date'] = start_date
        if end_date:
            query += f" AND {date_column} < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1\n"
            params['end_date'] = end_date

        query += f" GROUP BY to_char(trunc({date_column}, 'dd'),'YYYY-MM-DD') ORDER BY dt DESC"
        return query, params

    def build_data_query(self, data_ref: DataReference, columns: List[str],
                        date_column: Optional[str], update_column: str,
                        start_date: Optional[str], end_date: Optional[str],
                        exclude_recent_hours: Optional[int] = None) -> Tuple[str, Dict]:

        params = {}
        # Add recent data exclusion flag
        exclusion_condition,  exclusion_params = self._build_exclusion_condition(
            update_column, exclude_recent_hours
        )

        if exclusion_condition:
            columns.append(exclusion_condition)
            params.update(exclusion_params)

        query = f"""
        SELECT {', '.join(columns)}
        FROM {data_ref.full_name}
        WHERE 1=1\n"""

        if start_date and date_column:
            query += f"            AND {date_column} >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')\n"
            params['start_date'] = start_date

        if end_date and date_column:
            query += f"            AND {date_column} < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1\n"
            params['end_date'] = end_date

        return query, params

    def _build_exclusion_condition(self, update_column: str,
                                    exclude_recent_hours: int) -> Tuple[str, Dict]:
        """Oracle-specific implementation for recent data exclusion"""
        if  update_column and exclude_recent_hours:



            condition = f"""case when {update_column} > (sysdate - :exclude_recent_hours/24) then 'y' end as xrecently_changed"""
            params = {'exclude_recent_hours':  exclude_recent_hours}
            return condition, params

        return None, None

    def _get_type_conversion_rules(self, timezone: str) -> Dict[str, Callable]:
        return {
            #errors='coerce' is needed as workaround for >= 2262 year: Out of bounds nanosecond timestamp (3023-04-04 00:00:00)
            #  todo need specify explicit dateformat (nls params) in sessions, for the correct string conversion to datetime
            r'date': lambda x: pd.to_datetime(x, errors='coerce').dt.strftime(DATETIME_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'timestamp.*\bwith\b.*time\szone': lambda x: pd.to_datetime(x, utc=True, errors='coerce').dt.tz_convert(timezone).dt.tz_localize(None).dt.strftime(DATETIME_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'timestamp': lambda x: pd.to_datetime(x, errors='coerce').dt.strftime(DATETIME_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'number|float|double': lambda x: x.astype(str).str.replace(r'\.0+$', '', regex=True).str.lower(), #lower case for exponential form compare
        }
