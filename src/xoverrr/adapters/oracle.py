import time
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import text

from ..constants import DATETIME_FORMAT, FLAG_VALUE_YES, XRECENTLY_CHANGED_COLUMN
from ..exceptions import QueryExecutionError
from ..logger import app_logger
from ..models import DataReference, ObjectType
from .base import BaseDatabaseAdapter, Engine


class OracleAdapter(BaseDatabaseAdapter):
    PERSIST_TYPE_MAP = {
        'short_string': 'VARCHAR2(32)',
        'string': 'VARCHAR2(64)',
        'name': 'VARCHAR2(512)',
        'table_ref': 'VARCHAR2(256)',
        'tz_name': 'VARCHAR2(128)',
        'datetime': 'TIMESTAMP',
        'text': 'CLOB',
        'float': 'NUMBER',
        'int': 'NUMBER(19)',
    }

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
        columns_meta: Optional[pd.DataFrame],
        timezone: Optional[str],
    ) -> Tuple[str, Dict]:

        tz_columns = []
        tz_columns = self._identify_timestamp_tz_columns(columns_meta)

        date_expr = None
        date_expr = self._build_cast_tz_column_expression(
            column_name=date_column,
            tz_columns=tz_columns,
            target_timezone=timezone,
            as_alias=False,
        )

        query = f"""
            SELECT
                to_char(trunc({date_expr}, 'dd'),'YYYY-MM-DD') as dt,
                count(*) as cnt
            FROM {data_ref.full_name}
            WHERE 1=1\n"""
        params = {}

        if start_date:
            query += (
                f" AND {date_expr} >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')\n"
            )
            params['start_date'] = start_date
        if end_date:
            query += f" AND {date_expr} < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1\n"
            params['end_date'] = end_date

        query += (
            f" GROUP BY to_char(trunc({date_expr}, 'dd'),'YYYY-MM-DD') ORDER BY dt DESC"
        )
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
        columns_meta: pd.DataFrame = None,
        timezone: str = None,
    ) -> Tuple[str, Dict]:

        tz_columns = []
        tz_columns = self._identify_timestamp_tz_columns(columns_meta)

        converted_columns = self._apply_timestamp_tz_casts(
            columns=columns,
            tz_columns=tz_columns,
            target_timezone=timezone,
        )

        app_logger.info(columns_meta)

        params = {}
        # Add recent data exclusion flag
        exclusion_condition, exclusion_params = self._build_exclusion_condition(
            update_column, exclude_recent_hours
        )

        if exclusion_condition:
            converted_columns.append(exclusion_condition)
            params.update(exclusion_params)

        query = f"""
        SELECT {', '.join(converted_columns)}
        FROM {data_ref.full_name}
        WHERE 1=1\n"""

        date_expr = None
        if date_column:
            date_expr = self._build_cast_tz_column_expression(
                column_name=date_column,
                tz_columns=tz_columns,
                target_timezone=timezone,
                as_alias=False,  # No alias in WHERE
            )

        if start_date and date_expr:
            query += f"            AND {date_expr} >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')\n"
            params['start_date'] = start_date

        if end_date and date_expr:
            query += f"            AND {date_expr} < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1\n"
            params['end_date'] = end_date

        return query, params

    def _build_exclusion_condition(
        self, update_column: str, exclude_recent_hours: int
    ) -> Tuple[str, Dict]:
        """Oracle-specific implementation for recent data exclusion"""
        if update_column and exclude_recent_hours:
            condition = (
                f'case when {update_column} > (sysdate - :exclude_recent_hours/24) '
                f"then '{FLAG_VALUE_YES}' end as {XRECENTLY_CHANGED_COLUMN}"
            )
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
                pd.to_datetime(x, errors='coerce')
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

    def _identify_timestamp_tz_columns(
        self, columns_metadata: pd.DataFrame
    ) -> List[str]:
        """
        Identify columns that need timezone casting, because of the thin driver as well (missed tz info in the result column)

        Parameters:
            columns_metadata: DataFrame with column metadata (from _get_metadata_cols)
                            Must contain 'column_name' and 'data_type' columns

        Returns:
            List of column names that are TIMESTAMP WITH TIME ZONE (not LOCAL)
        """
        if columns_metadata is None or columns_metadata.empty:
            return []

        # Filter for TIMESTAMP WITH TIME ZONE (excluding LOCAL TIME ZONE)
        tz_mask = (
            columns_metadata['data_type']
            .str.lower()
            .str.contains(r'timestamp.*time zone', regex=True, na=False)
        )

        # Exclude LOCAL TIME ZONE
        local_mask = (
            columns_metadata['data_type']
            .str.lower()
            .str.contains(r'local', regex=True, na=False)
        )

        tz_columns = columns_metadata[tz_mask & ~local_mask]['column_name'].tolist()

        if tz_columns:
            app_logger.info(
                f'Identified TIMESTAMP WITH TIME ZONE columns: {tz_columns}'
            )

        return tz_columns

    def _build_cast_tz_column_expression(
        self,
        column_name: str,
        tz_columns: List[str],
        target_timezone: str,
        as_alias: bool = True,  # Add AS alias for SELECT clause
    ) -> str:
        """
        Wrapper to cast a single TIMESTAMP WITH TIME ZONE column if needed.

        Parameters:
            column_name: Name of the column
            tz_columns: List of columns that need casting
            target_timezone: Target timezone for conversion
            as_alias: If True, adds 'AS column_name' to the expression

        Returns:
            SQL expression (original column or CAST expression)
        """

        if column_name not in tz_columns:
            return column_name

        # Build CAST expression
        cast_expr = f"cast({column_name} at time zone '{target_timezone}' as timestamp)"

        # Add alias if needed (for SELECT clause)
        if as_alias:
            return f'{cast_expr} AS {column_name}'

        return cast_expr

    def _apply_timestamp_tz_casts(
        self,
        columns: List[str],
        tz_columns: List[str],
        target_timezone: str,
    ) -> List[str]:
        return [
            self._build_cast_tz_column_expression(
                col,
                tz_columns,
                target_timezone,
                as_alias=True,
            )
            for col in columns
        ]

    def ensure_persistence_table(
        self,
        engine: Engine,
        table_ref: DataReference,
        column_types: Dict[str, str],
        primary_key: Optional[str] = None,
    ) -> None:
        columns_sql = ',\n                    '.join(
            self._format_persist_column(name, col_type, primary_key)
            for name, col_type in column_types.items()
        )
        create_table_sql = f"""
            CREATE TABLE {table_ref.full_name} (
                    {columns_sql}
            )
        """.strip()
        escaped_create_sql = create_table_sql.replace("'", "''")
        plsql = f"""
            BEGIN
                EXECUTE IMMEDIATE '{escaped_create_sql}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
        """
        with engine.begin() as conn:
            conn.execute(text(plsql))

    def _format_persist_column(
        self, name: str, col_type: str, primary_key: Optional[str]
    ) -> str:
        sql_type = self.PERSIST_TYPE_MAP[col_type]
        if name == primary_key:
            return f'{name} {sql_type} PRIMARY KEY'
        return f'{name} {sql_type}'

    def insert_persistence_record(
        self, engine: Engine, table_ref: DataReference, record: Dict
    ) -> None:
        columns_sql = ', '.join(record.keys())
        values_sql = ', '.join(f':{col}' for col in record.keys())
        insert_sql = (
            f'INSERT INTO {table_ref.full_name} ({columns_sql}) VALUES ({values_sql})'
        )
        with engine.begin() as conn:
            conn.execute(text(insert_sql), record)
