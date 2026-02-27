import time
from json import dumps
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import text

from ..constants import DATETIME_FORMAT
from ..exceptions import QueryExecutionError, MetadataError
from ..logger import app_logger
from ..models import DataReference, ObjectType
from .base import BaseDatabaseAdapter, Engine


class PostgresAdapter(BaseDatabaseAdapter):
    def _execute_query(
        self, query: Union[str, Tuple[str, Dict]], engine: Engine, timezone: str
    ) -> pd.DataFrame:

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
                df = pd.read_sql(text(query), engine, params=params)
            else:
                if tz_set:
                    query = f'{tz_set}\n{query}'
                app_logger.info(f'query\n {query}')
                df = pd.read_sql(text(query), engine)
            execution_time = time.time() - start_time
            app_logger.info(f'Query executed in {execution_time:.2f}s')
            app_logger.info('complete')
            return df
        except Exception as e:
            execution_time = time.time() - start_time
            app_logger.error(
                f'Query execution failed after {execution_time:.2f}s: {str(e)}'
            )
            raise QueryExecutionError(f'Query failed: {str(e)}')

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
            WHERE n.nspname = :schema
            AND c.relname = :table
        """
        params = {'schema': data_ref.schema, 'table': data_ref.name}

        try:
            result = self._execute_query((query, params), engine, None)
            if not result.empty:
                type_str = result.iloc[0]['object_type']
                return {
                    'table': ObjectType.TABLE,
                    'view': ObjectType.VIEW,
                    'materialized_view': ObjectType.MATERIALIZED_VIEW,
                }.get(type_str, ObjectType.UNKNOWN)
        except Exception as e:
            app_logger.warning(
                f'Could not determine object type for {data_ref.full_name}: {str(e)}'
            )

        return ObjectType.UNKNOWN
    

    
    def get_metadata_for_custom_query(
        self,
        query: Union[str, Tuple[str, Dict]],
        engine: Engine,
    ) -> pd.DataFrame:
        """
        Extract column metadata from arbitrary SQL query without executing it.

        Implementation details (PostgreSQL):
        - wraps query into subquery
        - applies LIMIT 0
        - uses cursor.description (psycopg2)
        
        Returns:
            DataFrame with columns: column_id, column_name, data_type
        """
        start_time = time.time()
        app_logger.info('Getting metadata for custom query')
        
        # Extract query text and params
        if isinstance(query, tuple):
            query_text, params = query
        else:
            query_text = query
            params = {}
        
        # PostgreSQL OID to type mapping
        PG_OID_TYPE_MAP = {
            16: "boolean",      # bool
            20: "integer",      # int8
            21: "integer",      # int2
            23: "integer",      # int4
            700: "double",      # float4
            701: "double",      # float8
            1700: "numeric",    # numeric / decimal
            1082: "date",       # date
            1114: "timestamp",  # timestamp without tz
            1184: "timestamptz", # timestamp with tz
            25: "text",         # text
            1043: "text",       # varchar
            2950: "text",       # uuid
            114: "json",        # json
            3802: "json",       # jsonb
            1009: "text[]",     # array of text
            1016: "integer[]",  # array of integer
            1007: "integer[]",  # array of int4
        }
        
        wrapped_query = f"""
        SELECT *
        FROM (
            {query_text}
        ) xoverrr_subq
        LIMIT 0
        """
        
        with engine.connect() as conn:
            # SQLAlchemy 2.x execution
            result = conn.execute(text(wrapped_query), params or {})
            
            # Get cursor description
            cursor = result.cursor
            if cursor is None or cursor.description is None:
                raise MetadataError(
                    "Unable to extract cursor description for custom query"
                )
            
            metadata = []
            
            for i, col in enumerate(cursor.description, 1):
                # psycopg2 description fields:
                # (name, type_code, display_size, internal_size, precision, scale, null_ok)
                col_name = col.name
                type_code = col.type_code
                
                # Map OID to type name
                db_type = PG_OID_TYPE_MAP.get(type_code, "text")
                
                metadata.append({
                    'column_id': i,
                    'column_name': col_name.lower(),
                    'data_type': db_type
                })
            
            # Convert to DataFrame
            metadata_df = pd.DataFrame(metadata)
            
            execution_time = time.time() - start_time
            app_logger.info(f'Metadata retrieved in {execution_time:.2f}s')
            app_logger.info(f'Found {len(metadata_df)} columns')
            app_logger.debug('Discovered columns:\n' + metadata_df.to_string(index=False))
            
            return metadata_df
            
    

    def build_metadata_columns_query(self, data_ref: DataReference) -> pd.DataFrame:

        query = """
            SELECT
                lower(column_name) as column_name,
                lower(data_type) as data_type,
                ordinal_position as column_id
            FROM information_schema.columns
            WHERE table_schema = :schema
            AND table_name = :table
            ORDER BY ordinal_position
        """
        params = {'schema': data_ref.schema, 'table': data_ref.name}
        return query, params

    def build_primary_key_query(self, data_ref: DataReference) -> pd.DataFrame:
        """Build primary key query with GreenPlum compatibility"""
        query = """
            select
                lower(pg_attribute.attname) as pk_column_name
            from pg_index
            join pg_class on pg_class.oid = pg_index.indrelid
            join pg_attribute on pg_attribute.attrelid = pg_class.oid
                            and pg_attribute.attnum = any(pg_index.indkey)
            join pg_namespace on pg_namespace.oid = pg_class.relnamespace
            where pg_namespace.nspname = :schema
            and pg_class.relname = :table
            and pg_index.indisprimary
            order by pg_attribute.attnum
        """

        params = {'schema': data_ref.schema, 'table': data_ref.name}
        return query, params

    def build_count_query(
        self,
        data_ref: DataReference,
        date_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Tuple[str, Dict]:
        query = f"""
            SELECT
                to_char(date_trunc('day', {date_column}),'YYYY-MM-DD') as dt,
                count(*) as cnt
            FROM {data_ref.full_name}
            WHERE 1=1\n"""
        params = {}

        if start_date:
            query += f" AND {date_column} >= date_trunc('day', cast(:start_date as date))\n"
            params['start_date'] = start_date
        if end_date:
            query += f" AND {date_column} < date_trunc('day', cast(:end_date as date))  + interval '1 days'\n"
            params['end_date'] = end_date

        query += f" GROUP BY to_char(date_trunc('day', {date_column}),'YYYY-MM-DD') ORDER BY dt DESC"
        return query, params

    def build_data_query(
        self,
        data_ref: DataReference,
        columns: List[str],
        date_column: Optional[str],
        update_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        exclude_recent_hours: Optional[int] = None,
    ) -> Tuple[str, Dict]:

        params = {}
        # Add recent data exclusion flag
        exclusion_condition, exclusion_params = self._build_exclusion_condition(
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
            query += f"            AND {date_column} >= date_trunc('day', cast(:start_date as date))\n"
            params['start_date'] = start_date
        if end_date and date_column:
            query += f"            AND {date_column} < date_trunc('day', cast(:end_date as date))  + interval '1 days'\n"
            params['end_date'] = end_date

        return query, params

    def _build_exclusion_condition(
        self, update_column: str, exclude_recent_hours: int
    ) -> Tuple[str, Dict]:
        """PostgreSQL-specific implementation for recent data exclusion"""
        if update_column and exclude_recent_hours:
            exclude_recent_hours = exclude_recent_hours

            condition = f"""case when {update_column} > (now() - INTERVAL ':exclude_recent_hours hours') then 'y' end as xrecently_changed"""
            params = {'exclude_recent_hours': exclude_recent_hours}
            return condition, params

        return None, None

    def _get_type_conversion_rules(self, timezone) -> Dict[str, Callable]:
        return {
            r'date': lambda x: (
                pd.to_datetime(x, errors='coerce')
                .dt.strftime(DATETIME_FORMAT)
                .str.replace(r'\s00:00:00$', '', regex=True)
            ),
            r'boolean': lambda x: x.map({True: '1', False: '0', None: ''}),
            r'timestamptz|timestamp.*\bwith\b.*time\szone': lambda x: (
                pd.to_datetime(x, utc=True, errors='coerce')
                .dt.tz_convert(timezone)
                .dt.tz_localize(None)
                .dt.strftime(DATETIME_FORMAT)
                .str.replace(r'\s00:00:00$', '', regex=True)
            ),
            r'timestamp': lambda x: (
                pd.to_datetime(x, errors='coerce')
                .dt.strftime(DATETIME_FORMAT)
                .str.replace(r'\s00:00:00$', '', regex=True)
            ),
            r'integer|numeric|double|float|double precision|real': lambda x: x.astype(
                str
            ).str.replace(r'\.0+$', '', regex=True),
            r'json': lambda x: (
                '"' + x.astype(str).str.replace(r'"', '\\"', regex=True) + '"'
            ),
        }
