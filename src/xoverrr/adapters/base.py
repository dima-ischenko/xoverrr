import re
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy.engine import Engine

from ..constants import RESERVED_WORDS
from ..logger import app_logger
from ..models import DataReference, ObjectType

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
