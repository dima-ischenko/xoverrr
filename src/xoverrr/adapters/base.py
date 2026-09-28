import re
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy.engine import Engine

from ..constants import HASH_KEY_SEPARATOR, RESERVED_WORDS
from ..logger import app_logger
from ..models import DataReference, ObjectType
from ..utils import normalize_hash_pct

# Persist-row clock column; filled by DEFAULT now()/SYSTIMESTAMP, omitted from INSERT.
PERSIST_INSERTED_AT_COLUMN = 'inserted_at'


class BaseDatabaseAdapter(ABC):
    """Abstract base class for DBMS adapters with parameterised queries."""

    @abstractmethod
    def _execute_query(
        self, query: Union[str, Tuple[str, Dict]], engine: Engine, timezone: str
    ) -> pd.DataFrame:
        """Execute a query with DBMS-specific optimisations."""
        pass

    @abstractmethod
    def get_object_type(self, data_ref: DataReference, engine: Engine) -> ObjectType:
        """Determine the database object type."""
        pass

    @abstractmethod
    def get_metadata_for_custom_query(
        self, query: Union[str, Tuple[str, Dict]], engine: Engine
    ) -> pd.DataFrame:  # col_name, col_type, id
        """Determine column metadata for an arbitrary query."""
        pass

    @abstractmethod
    def build_metadata_columns_query(self, data_ref: DataReference) -> Tuple[str, Dict]:
        pass

    @abstractmethod
    def build_primary_key_query(self, data_ref: DataReference) -> Tuple[str, Dict]:
        pass

    def build_count_query_common(
        self,
        data_ref: DataReference,
        date_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        columns_meta: Optional[pd.DataFrame],
        timezone: Optional[str],
    ) -> Tuple[str, Dict]:
        """Return a (query, params) tuple for counts grouped by date."""
        return self.build_count_query(
            data_ref, date_column, start_date, end_date, columns_meta, timezone
        )

    def build_total_count_query(
        self,
        data_ref: DataReference,
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns_meta: Optional[pd.DataFrame] = None,
        timezone: Optional[str] = None,
    ) -> Tuple[str, Dict]:
        """Return a ``COUNT(*)`` query, optionally filtered by a date window."""
        query = f"""
            SELECT count(*) as cnt
            FROM {data_ref.full_name}
            WHERE 1=1
        """
        extra_sql, params = self._total_count_date_filters(
            date_column, start_date, end_date, columns_meta, timezone
        )
        return query + extra_sql, params

    def build_custom_query_aggregate_sql(
        self,
        query: str,
        max_columns: Optional[List[str]] = None,
        sum_columns: Optional[List[str]] = None,
        include_count: bool = False,
    ) -> str:
        """Wrap a custom query so the database computes MAX / SUM / COUNT(*)."""
        select_parts = []
        for column in self._normalize_aggregate_columns(max_columns):
            select_parts.append(f'max({column}) as max_{column}')
        for column in self._normalize_aggregate_columns(sum_columns):
            select_parts.append(f'sum({column}) as sum_{column}')
        if include_count:
            select_parts.append('count(*) as cnt')
        if not select_parts:
            raise ValueError('max_columns, sum_columns, or include_count is required')

        inner = self._strip_query(query)
        if not inner:
            raise ValueError('query is empty')
        return f'SELECT {", ".join(select_parts)} FROM ({inner}) x_subq'

    @staticmethod
    def _strip_query(query: str) -> str:
        return (query or '').strip().rstrip(';').strip()

    def wrap_query_with_hash_sample(
        self,
        query: str,
        hash_columns: Optional[List[str]],
        hash_pct: Optional[int],
        columns_meta: Optional[pd.DataFrame] = None,
        timezone: Optional[str] = None,
    ) -> str:
        """Wrap a custom query so only a stable hash-bucket of keys is read."""
        predicate = self.build_hash_filter(
            hash_columns, hash_pct, columns_meta, timezone
        )
        if not predicate:
            return query
        inner = self._strip_query(query)
        if not inner:
            raise ValueError('query is empty')
        return f'SELECT * FROM ({inner}) x_hash WHERE {predicate}'

    def build_hash_filter(
        self,
        hash_columns: Optional[List[str]],
        hash_pct: Optional[int],
        columns_meta: Optional[pd.DataFrame] = None,
        timezone: Optional[str] = None,
    ) -> Optional[str]:
        """Return a SQL predicate that keeps ``hash_pct`` percent of keys."""
        percent = normalize_hash_pct(hash_pct)
        if percent is None:
            return None
        columns = [str(column).strip() for column in (hash_columns or []) if column]
        if not columns:
            raise ValueError('hash_pct requires hash columns')
        type_map = self._column_type_map(columns_meta)
        parts = []
        for column in columns:
            quoted = self._quote_ident(column)
            expr = self.hash_key_expression(
                quoted, type_map.get(column.lower(), ''), timezone
            )
            parts.append(f"coalesce({expr}, '')")
        concat = f" || '{HASH_KEY_SEPARATOR}' || ".join(parts)
        return self.hash_mod_predicate(concat, percent)

    @staticmethod
    def _column_type_map(columns_meta: Optional[pd.DataFrame]) -> Dict[str, str]:
        if columns_meta is None or columns_meta.empty:
            return {}
        return {
            str(row.column_name).lower(): str(row.data_type).lower()
            for row in columns_meta.itertuples()
            if getattr(row, 'column_name', None) is not None
        }

    @staticmethod
    def _quote_ident(column: str) -> str:
        if column.lower() in RESERVED_WORDS:
            return f'"{column}"'
        return column

    def hash_key_expression(
        self, column: str, data_type: str, timezone: Optional[str]
    ) -> str:
        """Return SQL that casts ``column`` to a canonical string for hashing."""
        raise NotImplementedError

    def hash_mod_predicate(self, concat_sql: str, percent: int) -> str:
        """Return ``md5(concat) % 100 < percent`` for this DBMS."""
        raise NotImplementedError

    @staticmethod
    def _normalize_aggregate_columns(columns: Optional[List[str]]) -> List[str]:
        names: List[str] = []
        for column in columns or []:
            name = str(column).strip().lower()
            if not re.match(r'^[a-z_][a-z0-9_]*$', name):
                raise ValueError(f'Invalid aggregate column name: {column}')
            if name not in names:
                names.append(name)
        return names

    def _total_count_date_filters(
        self,
        date_column: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        columns_meta: Optional[pd.DataFrame],
        timezone: Optional[str],
    ) -> Tuple[str, Dict]:
        return '', {}

    @abstractmethod
    def build_count_query(
        self,
        data_ref: DataReference,
        date_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        columns_meta: Optional[pd.DataFrame],
        timezone: Optional[str],
    ) -> Tuple[str, Dict]:
        """Return a (query, params) tuple with optional recent-row exclusion."""
        pass

    def build_data_query_common(
        self,
        data_ref: DataReference,
        common_columns: List[str],
        date_column: Optional[str],
        update_column: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        exclude_recent_hours: Optional[int] = None,
        columns_meta: pd.DataFrame = None,
        timezone: str = None,
        key_column: List[str] = None,
        hash_pct: int = None,
    ) -> Tuple[str, Dict]:
        """Build a data query for the DBMS, with optional recent-row exclusion."""
        # Handle reserved words
        cols_select = [
            f'"{col}"' if col.lower() in RESERVED_WORDS else col
            for col in common_columns
        ]

        result = self.build_data_query(
            data_ref,
            cols_select,
            date_column,
            update_column,
            start_date,
            end_date,
            exclude_recent_hours,
            columns_meta,
            timezone,
            key_column,
            hash_pct,
        )
        return result

    @abstractmethod
    def build_data_query(
        self,
        data_ref: DataReference,
        columns: List[str],
        date_column: Optional[str],
        update_column: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        exclude_recent_hours: Optional[int] = None,
        columns_meta: pd.DataFrame = None,
        timezone: str = None,
        key_column: List[str] = None,
        hash_pct: int = None,
    ) -> Tuple[str, Dict]:
        pass

    @abstractmethod
    def _build_exclusion_condition(
        self, update_column: str, exclude_recent_hours: int
    ) -> Tuple[str, Dict]:
        """Build the DBMS-specific predicate for recent-row exclusion."""
        pass

    def convert_types(
        self, df: pd.DataFrame, metadata: pd.DataFrame, timezone: str
    ) -> pd.DataFrame:
        """Convert DBMS-specific types to standardised formats."""
        # A timezone is required for conversion: pandas implicitly converts
        # time-zone-aware columns to UTC, and there is no portable way to
        # disable this across pandas versions.
        type_rules = self._get_type_conversion_rules(timezone)
        return self._apply_type_conversion(df, metadata, type_rules)

    @abstractmethod
    def _get_type_conversion_rules(self, timezone: str) -> Dict[str, Callable]:
        """Return type-conversion rules for a specific DBMS."""
        pass

    @abstractmethod
    def ensure_persistence_table(
        self,
        engine: Engine,
        table_ref: DataReference,
        column_types: Dict[str, str],
        primary_key: Optional[str] = None,
    ) -> None:
        """Create the persistence table if it is missing."""
        pass

    def build_persistence_insert_sql(
        self, table_ref: DataReference, record: Dict
    ) -> str:
        columns = [name for name in record if name != PERSIST_INSERTED_AT_COLUMN]
        columns_sql = ', '.join(columns)
        values_sql = ', '.join(f':{col}' for col in columns)
        return (
            f'INSERT INTO {table_ref.full_name} ({columns_sql}) VALUES ({values_sql})'
        )

    @abstractmethod
    def insert_persistence_record(
        self,
        engine: Engine,
        table_ref: DataReference,
        record: Dict,
        column_types: Optional[Dict[str, str]] = None,
    ) -> None:
        """Insert one persistence record using explicit SQL."""
        pass

    def _apply_type_conversion(
        self, df: pd.DataFrame, metadata: pd.DataFrame, type_rules: Dict[str, Callable]
    ) -> pd.DataFrame:
        """Apply type-conversion rules to a DataFrame."""
        if df.empty:
            return df

        app_logger.debug(f'rules: {type_rules.items()}')
        app_logger.debug(f'df.dtypes: {df.dtypes}')
        app_logger.debug(f'db col metadata: {metadata}')

        # Apply conversion from database column metadata only.
        for _, col_info in metadata.iterrows():
            col_name = col_info['column_name']
            if col_name not in df.columns:
                continue

            col_type = col_info['data_type'].lower()
            # Find matching conversion rule
            converter = None
            for pattern, rule in type_rules.items():
                if re.search(pattern, col_type):
                    converter = rule
                    app_logger.debug(f'{col_name=}: found rule {converter=}')
                    break

            if converter is None:
                continue  # Skip columns without converters

            try:
                df[col_name] = converter(df[col_name])
            except Exception as e:
                app_logger.warning(f'Type conversion failed for {col_name}: {str(e)}')
                df[col_name] = df[col_name].astype(str)

            new_type = df[col_name].dtype
            app_logger.debug(f'old: {col_type}, new: {new_type}')

        return df
