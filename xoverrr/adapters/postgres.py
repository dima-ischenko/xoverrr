import pandas as pd
from typing import Optional, Dict, Callable, List, Tuple, Union
from ..constants import DATETIME_FORMAT
from .base import BaseDatabaseAdapter, Engine
from ..models import DataReference, ObjectType
from ..exceptions import QueryExecutionError
from json import dumps

from ..logger import app_logger
import time

class PostgresAdapter(BaseDatabaseAdapter):


    def _execute_query(self, query: Union[str, Tuple[str, Dict]], engine: Engine, timezone: str) -> pd.DataFrame:

        df = None
        tz_set = None
        start_time = time.time()
        app_logger.info('start')

        if timezone:
            tz_set = f"set time zone '{timezone}';"

        try:
            if isinstance(query, tuple):
                query, params = query
                if tz_set:
                    query = f'{tz_set}\n{query}'
                app_logger.info(f'query\n {query}')
                app_logger.info(f'{params=}')
                df = pd.read_sql(query, engine, params=params)
            else:
                if tz_set:
                    query = f'{tz_set}\n{query}'
                app_logger.info(f'query\n {query}')
                df = pd.read_sql(query, engine)
            execution_time = time.time() - start_time
            app_logger.info(f"Query executed in {execution_time:.2f}s")
            app_logger.info('complete')
            return df
        except Exception as e:
            execution_time = time.time() - start_time
            app_logger.error(f"Query execution failed after {execution_time:.2f}s: {str(e)}")
            raise QueryExecutionError(f"Query failed: {str(e)}")


    def get_object_type(self, data_ref: DataReference, engine: Engine) -> ObjectType:
        """Determine if object is table, view, or materialized view"""
        query = """
            SELECT
                CASE
                    WHEN relkind = 'r' THEN 'table'
                    WHEN relkind = 'v' THEN 'view'
                    WHEN relkind = 'm' THEN 'materialized_view'
                    ELSE 'unknown'
                END as object_type
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %(schema)s
            AND c.relname = %(table)s
        """
        params = {'schema': data_ref.schema, 'table': data_ref.name}

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
                ordinal_position as column_id
            FROM information_schema.columns
            WHERE table_schema = %(schema)s
            AND table_name = %(table)s
            ORDER BY ordinal_position
        """
        params = {'schema': data_ref.schema, 'table': data_ref.name}
        return query, params

    def build_primary_key_query(self, data_ref: DataReference) -> pd.DataFrame:
        """Build primary key query with GreenPlum compatibility"""
        query = """
            select
                pg_attribute.attname as pk_column_name
            from pg_index
            join pg_class on pg_class.oid = pg_index.indrelid
            join pg_attribute on pg_attribute.attrelid = pg_class.oid
                            and pg_attribute.attnum = any(pg_index.indkey)
            join pg_namespace on pg_namespace.oid = pg_class.relnamespace
            where pg_namespace.nspname = %(schema)s
            and pg_class.relname = %(table)s
            and pg_index.indisprimary
            order by pg_attribute.attnum
        """

        params = {'schema': data_ref.schema, 'table': data_ref.name}
        return query, params

    def build_count_query(self, data_ref: DataReference, date_column: str,
                          start_date: Optional[str], end_date: Optional[str]
                         ) -> Tuple[str, Dict]:
        query = f"""
            SELECT
                to_char(date_trunc('day', {date_column}),'YYYY-MM-DD') as dt,
                count(*) as cnt
            FROM {data_ref.full_name}
            WHERE 1=1\n"""
        params = {}

        if start_date:
            query += f" AND {date_column} >= date_trunc('day', %(start_date)s::date)\n"
            params['start_date'] = start_date
        if end_date:
            query += f" AND {date_column} < date_trunc('day', %(end_date)s::date)  + interval '1 days'\n"
            params['end_date'] = end_date

        query += f" GROUP BY to_char(date_trunc('day', {date_column}),'YYYY-MM-DD') ORDER BY dt DESC"
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
            query += f"            AND {date_column} >= date_trunc('day', %(start_date)s::date)\n"
            params['start_date'] = start_date
        if end_date and date_column:
            query += f"            AND {date_column} < date_trunc('day', %(end_date)s::date)  + interval '1 days'\n"
            params['end_date'] = end_date

        return query, params

    def _build_exclusion_condition(self, update_column: str,
                                    exclude_recent_hours: int) -> Tuple[str, Dict]:
        """PostgreSQL-specific implementation for recent data exclusion"""
        if  update_column and exclude_recent_hours:


            exclude_recent_hours = exclude_recent_hours

            condition = f"""case when {update_column} > (now() - INTERVAL '%(exclude_recent_hours)s hours') then 'y' end as xrecently_changed"""
            params = {'exclude_recent_hours':  exclude_recent_hours}
            return condition, params

        return None, None

    def _get_type_conversion_rules(self, timezone) -> Dict[str, Callable]:
        return {
            r'date': lambda x: pd.to_datetime(x, errors='coerce').dt.strftime(DATETIME_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'boolean': lambda x: x.map({True: '1', False: '0', None: ''}),
            r'timestamptz|timestamp.*\bwith\b.*time\szone': lambda x: pd.to_datetime(x, utc=True, errors='coerce').dt.tz_convert(timezone).dt.tz_localize(None).dt.strftime(DATETIME_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'timestamp': lambda x: pd.to_datetime(x, errors='coerce').dt.strftime(DATETIME_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'integer|numeric|double|float|double precision|real': lambda x: x.astype(str).str.replace(r'\.0+$', '', regex=True),
            r'json': lambda x: '"' + x.astype(str).str.replace(r'"', '\\"', regex=True) + '"',
        }