from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Callable, List, Tuple, Optional, Union
import re
from datetime import datetime, timedelta
from ..models import DataReference, ObjectType
from ..constants import RESERVED_WORDS
from sqlalchemy.engine import Engine
from ..logger import app_logger
from ..logger import app_logger

class BaseDatabaseAdapter(ABC):
    """Abstract base class with updated method signatures for parameterized queries"""
    @abstractmethod
    def _execute_query(self, query: Union[str, Tuple[str, Dict]], engine: Engine, timezone:str) -> pd.DataFrame:
        """Execute query with DBMS-specific optimizations"""
        pass

    @abstractmethod
    def get_object_type(self, data_ref: DataReference, engine: Engine) -> ObjectType:
        """Determine database object type"""
        pass

    @abstractmethod
    def build_metadata_columns_query(self, data_ref: DataReference) -> Tuple[str, Dict]:
        pass

    @abstractmethod
    def build_primary_key_query(self, data_ref: DataReference) -> Tuple[str, Dict]:
        pass

    @abstractmethod
    def build_count_query(self, data_ref: DataReference, date_column: str,
                         start_date: Optional[str], end_date: Optional[str]
                         ) -> Tuple[str, Dict]:
        """Returns tuple of (query, params) with recent data exclusion"""
        pass

    def build_data_query_common(self, data_ref: DataReference, columns: List[str],
                        date_column: Optional[str], update_column: Optional[str],
                        start_date: Optional[str], end_date: Optional[str],
                        exclude_recent_hours: Optional[int] = None) -> Tuple[str, Dict]:
        """Build data query for the DBMS with recent data exclusion"""
        # Handle reserved words
        cols_select = [
            f'"{col}"' if col.lower() in RESERVED_WORDS
            else col
            for col in columns
        ]

        result = self.build_data_query(data_ref, cols_select, date_column, update_column,
                                     start_date, end_date, exclude_recent_hours)
        return result

    @abstractmethod
    def build_data_query(self, data_ref: DataReference, columns: List[str],
                        date_column: Optional[str], update_column: Optional[str],
                        start_date: Optional[str], end_date: Optional[str],
                        exclude_recent_hours: Optional[int] = None) -> Tuple[str, Dict]:
        pass

    @abstractmethod
    def _build_exclusion_condition(self, update_column: str,
                                 exclude_recent_hours: int) -> Tuple[str, Dict]:
        """DBMS-specific implementation for recent data exclusion"""
        pass

    def convert_types(self, df: pd.DataFrame, metadata: pd.DataFrame, timezone: str) -> pd.DataFrame:
        """Convert DBMS-specific types to standardized formats"""
        # there is need to specify timezone for covnersion as
        #   pandas implicitly converts to UTC tz aware cols
        #   and there is general way for different version of pandas to disable this
        type_rules = self._get_type_conversion_rules(timezone)
        return self._apply_type_conversion(df, metadata, type_rules)

    @abstractmethod
    def _get_type_conversion_rules(self, timezone: str) -> Dict[str, Callable]:
        """Get type conversion rules for specific DBMS"""
        pass

    def _apply_type_conversion(self, df: pd.DataFrame, metadata: pd.DataFrame,
                             type_rules: Dict[str, Callable]) -> pd.DataFrame:
        """Apply type conversion rules to DataFrame"""
        if df.empty:
            return df

        app_logger.debug(f'rules: {type_rules.items()}')
        app_logger.debug(f'df.dtypes: {df.dtypes}')
        app_logger.debug(f'db col metadata: {metadata}')

        # apply conversion based on db col meta only
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
                continue # Skip columns without converters

            try:
                df[col_name] = converter(df[col_name])
            except Exception as e:
                app_logger.warning(f"Type conversion failed for {col_name}: {str(e)}")
                df[col_name] = df[col_name].astype(str)

            new_type = df[col_name].dtype
            app_logger.debug(f'old: {col_type}, new: {new_type}')

        return df