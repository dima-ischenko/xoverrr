import time
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

from ..constants import DATETIME_FORMAT
from ..exceptions import QueryExecutionError
from ..logger import app_logger
from ..models import DataReference, ObjectType
from .base import BaseDatabaseAdapter, Engine


class OracleAdapter(BaseDatabaseAdapter):
    def _execute_query(
        self,
        query: Union[str, Tuple[str, Dict]],
        engine: Engine,
        timezone: str,
        sqltype: str = 'sql',
    ) -> pd.DataFrame:
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

                # Check if this is a PL/SQL block with OUT parameter
                if sqltype == 'plsql':
                    app_logger.info('executing PL/SQL block with OUT parameter')

                    # Create output variable
                    result_var = cursor.var(str)

                    # Add the output variable to params if it's not already there
                    if params is None:
                        params = {}

                    # Make sure :result is not in params as it's an OUT parameter
                    if 'result' in params:
                        del params['result']

                    # Execute with output variable
                    cursor.execute(query_text, {**params, 'result': result_var})

                    # Get the result and convert to DataFrame
                    result_value = result_var.getvalue()

                    # Create a simple DataFrame with the result
                    df = pd.DataFrame([[result_value]], columns=['result'])

                else:
                    # Regular query execution
                    app_logger.info(f'query\n {query_text}')
                    app_logger.info(f'{params=}')
                    cursor.execute(query_text, params or {})

                    if cursor.description:
                        columns = [col[0].lower() for col in cursor.description]
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=columns)
                    else:
                        # For DML operations that don't return rows
                        df = pd.DataFrame()
            else:
                app_logger.info(f'query\n {query}')
                cursor.execute(query)

                if cursor.description:
                    columns = [col[0].lower() for col in cursor.description]
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=columns)
                else:
                    df = pd.DataFrame()

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

        finally:
            # Clean up resources
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if raw_conn:
                try:
                    raw_conn.close()
                except:
                    pass

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
                    'materialized_view': ObjectType.MATERIALIZED_VIEW,
                }.get(type_str, ObjectType.UNKNOWN)
        except Exception as e:
            app_logger.warning(
                f'Could not determine object type for {data_ref.full_name}: {str(e)}'
            )

        return ObjectType.UNKNOWN

    def get_metadata_for_custom_query(
        self, query: Union[str, Tuple[str, Dict]], engine: Engine
    ) -> pd.DataFrame:
        """
        Determine columns metadata based on arbitrary query without executing it.
        Uses DBMS_SQL to parse and describe the query.

        Returns:
            DataFrame with columns: column_name, data_type, column_id
        """
        start_time = time.time()
        app_logger.info('Getting metadata for custom query')

        # Extract query text if tuple is passed
        if isinstance(query, tuple):
            query_text, _ = query
        else:
            query_text = query

        # PL/SQL block to describe query and return metadata as pipe-separated string
        describe_plsql = """
        declare
            l_cursor      integer;
            l_columns     number;
            l_desc_tbl    dbms_sql.desc_tab;
            l_query       varchar2(32767) := :sql_query;
            l_output      varchar2(32767) := '';
        begin
            -- open cursor and parse query
            l_cursor := dbms_sql.open_cursor;
            dbms_sql.parse(l_cursor, l_query, dbms_sql.native);
            
            -- describe columns
            dbms_sql.describe_columns(l_cursor, l_columns, l_desc_tbl);
            
            -- build output string with delimiter
            for i in 1..l_columns loop
                if i > 1 then
                    l_output := l_output || '||';
                end if;
                l_output := l_output || 
                    i || '|' || 
                    lower(l_desc_tbl(i).col_name) || '|' ||
                    case l_desc_tbl(i).col_type
                        when 1 then 'varchar2'
                        when 2 then 'number'
                        when 8 then 'long'
                        when 12 then 'date'
                        when 23 then 'raw'
                        when 96 then 'char'
                        when 112 then 'clob'
                        when 180 then 'timestamp'
                        when 181 then 'timestamp with time zone'
                        when 182 then 'interval year to month'
                        when 183 then 'interval day to second'
                        when 208 then 'urowid'
                        when 231 then 'timestamp with local time zone'
                        else 'unknown_' || l_desc_tbl(i).col_type
                    end;
            end loop;
            
            -- return as single string
            :result := l_output;
            
            dbms_sql.close_cursor(l_cursor);
        exception
            when others then
                dbms_sql.close_cursor(l_cursor);
                raise;
        end;
        """

        try:
            # Execute the describe block using enhanced _execute_query
            result_df = self._execute_query(
                (describe_plsql, {'sql_query': query_text}),
                engine,
                None,  # timezone not needed for metadata
                sqltype='plsql',
            )

            if result_df.empty:
                raise QueryExecutionError('No metadata returned from describe')

            # Get the result string from the first row, first column
            result_str = result_df.iloc[0, 0]

            if not result_str:
                raise QueryExecutionError('Empty metadata returned from describe')

            # Parse pipe-separated values
            rows = result_str.split('||')
            metadata = []

            for row in rows:
                if row:
                    parts = row.split('|')
                    if len(parts) == 3:
                        col_id, col_name, col_type = parts
                        metadata.append(
                            {
                                'column_id': int(col_id),
                                'column_name': col_name,
                                'data_type': col_type,
                            }
                        )

            execution_time = time.time() - start_time
            app_logger.info(f'Metadata retrieved in {execution_time:.2f}s')
            app_logger.info(f'Found {len(metadata)} columns')

            df = pd.DataFrame(metadata)

            # Log the discovered columns
            if not df.empty:
                app_logger.debug('Discovered columns:\n' + df.to_string(index=False))

            return df

        except Exception as e:
            execution_time = time.time() - start_time
            app_logger.error(
                f'Failed to get metadata after {execution_time:.2f}s: {str(e)}'
            )
            raise QueryExecutionError(f'Failed to describe query: {str(e)}')

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

        # todo add suport of unique indexes when no pk?
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

    def build_count_query(
        self,
        data_ref: DataReference,
        date_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Tuple[str, Dict]:
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
            query += f"            AND {date_column} >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')\n"
            params['start_date'] = start_date

        if end_date and date_column:
            query += f"            AND {date_column} < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1\n"
            params['end_date'] = end_date

        return query, params

    def _build_exclusion_condition(
        self, update_column: str, exclude_recent_hours: int
    ) -> Tuple[str, Dict]:
        """Oracle-specific implementation for recent data exclusion"""
        if update_column and exclude_recent_hours:
            condition = f"""case when {update_column} > (sysdate - :exclude_recent_hours/24) then 'y' end as xrecently_changed"""
            params = {'exclude_recent_hours': exclude_recent_hours}
            return condition, params

        return None, None

    def _get_type_conversion_rules(self, timezone: str) -> Dict[str, Callable]:
        return {
            # errors='coerce' is needed as workaround for >= 2262 year: Out of bounds nanosecond timestamp (3023-04-04 00:00:00)
            #  todo need specify explicit dateformat (nls params) in sessions, for the correct string conversion to datetime
            r'date': lambda x: (
                pd.to_datetime(x, errors='coerce')
                .dt.strftime(DATETIME_FORMAT)
                .str.replace(r'\s00:00:00$', '', regex=True)
            ),
            r'timestamp.*\bwith\b.*time\szone': lambda x: (
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
            r'number|float|double': lambda x: (
                x.astype(str).str.replace(r'\.0+$', '', regex=True).str.lower()
            ),  # lower case for exponential form compare
        }
