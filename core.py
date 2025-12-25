
import sys
from enum import Enum, auto
from typing import Optional, List, Dict, Callable, Union, Tuple, Any
import pandas as pd
from sqlalchemy.engine import Engine
from .models import (
    DBMSType,
    DataReference,
    ObjectType
)

from .logger import app_logger

from .adapters.oracle import OracleAdapter
from .adapters.postgres import PostgresAdapter
from .adapters.clickhouse import ClickHouseAdapter
from .adapters.base import BaseDatabaseAdapter

from . import constants as ct

from .exceptions import (
    MetadataError,
    DQCompareException
)
from .utils import (
    prepare_dataframe,
    compare_dataframes,
    clean_recently_changed_data,
    generate_comparison_sample_report,
    generate_comparison_count_report,
    cross_fill_missing_dates,
    validate_dataframe_size,
    ComparisonStats,
    ComparisonDiffDetails
)




class DataQualityComparator:
    """
    Main comparison class implementing data quality checks between databases.
    """

    def __init__(
        self,
        source_engine: Engine,
        target_engine: Engine,
        default_exclude_recent_hours: Optional[int] = 24,
        timezone: str = ct.DEFAULT_TZ
    ):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.source_db_type = DBMSType.from_engine(source_engine)
        self.target_db_type = DBMSType.from_engine(target_engine)
        self.default_exclude_recent_hours = default_exclude_recent_hours
        self.timezone = timezone

        self.adapters = {
            DBMSType.ORACLE: OracleAdapter(),
            DBMSType.POSTGRESQL: PostgresAdapter(),
            DBMSType.CLICKHOUSE: ClickHouseAdapter(),
        }
        self._reset_stats()
        app_logger.info('start')

    def reset_stats(self):
        self._reset_stats()

    def _reset_stats(self):
        self.comparison_stats = {
            'compared': 0,
            ct.COMPARISON_SUCCESS: 0,
            ct.COMPARISON_FAILED: 0,
            ct.COMPARISON_SKIPPED: 0,
            'tables_success' : set(),
            'tables_failed' : set(),
            'tables_skipped': set(),
            'start_time': pd.Timestamp.now().strftime(ct.DATETIME_FORMAT),
            'end_time': None
        }

    def _update_stats(self, status: str, source_table:DataReference):
        """Update comparison statistics"""
        self.comparison_stats[status] += 1
        self.comparison_stats['end_time'] = pd.Timestamp.now().strftime(ct.DATETIME_FORMAT)
        if source_table:
            match status:
                case ct.COMPARISON_SUCCESS:
                    self.comparison_stats['tables_success'].add(source_table.full_name)
                case ct.COMPARISON_FAILED:
                    self.comparison_stats['tables_failed'].add(source_table.full_name)
                case ct.COMPARISON_SKIPPED:
                    self.comparison_stats['tables_skipped'].add(source_table.full_name)

    def compare_counts(
        self,
        source_table: DataReference,
        target_table: DataReference,
        date_column: Optional[str] = None,
        date_range: Optional[Tuple[str, str]] = None,
        tolerance_percentage: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES
    ) -> Tuple[str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:

        self._validate_inputs(source_table, target_table)

        start_date, end_date = date_range or (None, None)

        try:
            self.comparison_stats['compared'] += 1


            status, report, stats, details = self._compare_counts(
                    source_table, target_table, date_column, start_date, end_date,
                    tolerance_percentage, max_examples
            )

            self._update_stats(status, source_table)
            return status, report, stats, details

        except Exception as e:
            app_logger.exception(f"Count comparison failed: {str(e)}")
            status = ct.COMPARISON_FAILED
            self._update_stats(status, source_table)
            return status, None, None, None

    def compare_sample(
        self,
        source_table: DataReference,
        target_table: DataReference,
        date_column: Optional[str] = None,
        update_column: Optional[str] = None,
        date_range: Optional[Tuple[str, str]] = None,
        exclude_columns: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        custom_primary_key: Optional[List[str]] = None,
        tolerance_percentage: float = 0.0,
        exclude_recent_hours: Optional[int] = None,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES
    ) -> Tuple[str, str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:
        """
        Compare data from custom queries with specified key columns

        Parameters:
            source_table: `DataReference` 
                source table to compare
            target_table: `DataReference`
                target table to compare
            custom_primary_key : `List[str]`
                List of primary key columns for comparison.
            exclude_columns : `Optional[List[str]] = None` 
                Columns to exclude from comparison.
            include_columns : `Optional[List[str]] = None` 
                Columns to include from comparison (default all cols)
            tolerance_percentage : `float` 
                Tolerance percentage for discrepancies.
            max_examples 
                Maximum number of discrepancy examples per column
        """
        self._validate_inputs(source_table, target_table)

        exclude_hours = exclude_recent_hours or self.default_exclude_recent_hours

        start_date, end_date = date_range or (None, None)
        exclude_cols = exclude_columns or []
        custom_keys = custom_primary_key
        include_cols = include_columns or []

        try:
            self.comparison_stats['compared'] += 1

            status, report, stats, details = self._compare_samples(
                    source_table, target_table, date_column, update_column,
                    start_date, end_date, exclude_cols,include_cols, 
                    custom_keys, tolerance_percentage, exclude_hours, max_examples
            )

            self._update_stats(status, source_table)
            return status, report, stats, details

        except Exception as e:
            app_logger.exception(f"Sample comparison failed: {str(e)}")
            status = ct.COMPARISON_FAILED
            self._update_stats(status, source_table)
            return status, None, None, None

    def _compare_counts(self, source_table: DataReference,
                        target_table: DataReference,
                        date_column: str,
                        start_date: Optional[str],
                        end_date: Optional[str],
                        tolerance_percentage:float,
                        max_examples:int) -> Tuple[str, str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:

        try:
            source_adapter = self._get_adapter(self.source_db_type)
            target_adapter = self._get_adapter(self.target_db_type)

            source_query, source_params = source_adapter.build_count_query(
                source_table, date_column, start_date, end_date
            )
            source_counts = self._execute_query((source_query, source_params), self.source_engine, self.timezone)

            target_query, target_params = target_adapter.build_count_query(
                target_table, date_column, start_date, end_date
            )
            target_counts = self._execute_query((target_query, target_params), self.target_engine, self.timezone)

            source_counts_filled, target_counts_filled = cross_fill_missing_dates(source_counts, target_counts)
            source_counts_filled['dt'] = pd.to_datetime(source_counts_filled['dt'], format='%Y-%m-%d')
            target_counts_filled['dt'] = pd.to_datetime(target_counts_filled['dt'], format='%Y-%m-%d')

            merged = source_counts_filled.merge(target_counts_filled, on='dt')
            total_count_source = source_counts_filled['cnt'].sum()
            total_count_taget =  target_counts_filled['cnt'].sum()

            if (total_count_source, total_count_taget)  ==  (0,0):
                app_logger.warning('nothing to compare to you')
                status = ct.COMPARISON_SKIPPED
                return status, None, None, None

            else:

                result_diff_in_counters = abs(merged['cnt_x'] - merged['cnt_y']).sum()
                result_equal_in_counters = merged[['cnt_x', 'cnt_y']].min(axis=1).sum()

                discrepancies_counters_percentage = 100*result_diff_in_counters/(result_diff_in_counters+result_equal_in_counters)
                stats, details = compare_dataframes(source_df=source_counts_filled,
                                                    target_df=target_counts_filled,
                                                    key_columns=['dt'],
                                                    max_examples=max_examples)

                status = ct.COMPARISON_FAILED if discrepancies_counters_percentage > tolerance_percentage else ct.COMPARISON_SUCCESS

                report = generate_comparison_count_report(source_table.full_name,
                                                          target_table.full_name,
                                                          stats,
                                                          details,
                                                          total_count_source,
                                                          total_count_taget,
                                                          discrepancies_counters_percentage,
                                                          result_diff_in_counters,
                                                          result_equal_in_counters,
                                                          self.timezone,
                                                          source_query,
                                                          source_params,
                                                          target_query,
                                                          target_params
                                                        )

                return status, report, stats, details

        except Exception as e:
            app_logger.error(f"Count comparison failed: {str(e)}")
            raise

    def _compare_samples(
        self,
        source_table: DataReference,
        target_table: DataReference,
        date_column: str,
        update_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        exclude_columns: List[str],
        include_columns: List[str],
        custom_key_columns: Optional[List[str]],
        tolerance_percentage:float,
        exclude_recent_hours: Optional[int],
        max_examples:Optional[int]
    ) -> Tuple[str, str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:

        try:
            source_object_type = self._get_object_type(source_table, self.source_engine)
            target_object_type = self._get_object_type(target_table, self.target_engine)
            app_logger.info(f'object type source: {source_object_type} vs target {target_object_type}')

            source_columns_meta = self._get_metadata_cols(source_table, self.source_engine)
            app_logger.info('source_columns meta:\n')
            app_logger.info(source_columns_meta.to_string(index=False))

            target_columns_meta = self._get_metadata_cols(target_table, self.target_engine)
            app_logger.info('target_columns meta:\n')
            app_logger.info(target_columns_meta.to_string(index=False))

            intersect = list(set(include_columns)&set(exclude_columns))
            if intersect:
                app_logger.warning(f'Intersection columns between Include and exclude: {",".join(intersect)}')
            
            key_columns = None

            if custom_key_columns:
                key_columns = custom_key_columns
                source_cols = source_columns_meta['column_name'].tolist()
                target_cols = target_columns_meta['column_name'].tolist()

                missing_in_source = [col for col in custom_key_columns if col not in source_cols]
                missing_in_target = [col for col in custom_key_columns if col not in target_cols]

                if missing_in_source:
                    raise MetadataError(f"Custom key columns missing in source: {missing_in_source}")
                if missing_in_target:
                    raise MetadataError(f"Custom key columns missing in target: {missing_in_target}")
            else:
                source_pk = self._get_metadata_pk(source_table, self.source_engine) \
                                         if source_object_type == ObjectType.TABLE else pd.DataFrame({'pk_column_name': []})
                target_pk = self._get_metadata_pk(target_table, self.target_engine) \
                                         if target_object_type == ObjectType.TABLE else pd.DataFrame({'pk_column_name': []})

                if source_pk['pk_column_name'].tolist() != target_pk['pk_column_name'].tolist():
                    app_logger.warning(f"Primary keys differ: source={source_pk['pk_column_name'].tolist()}, target={target_pk['pk_column_name'].tolist()}")
                key_columns = source_pk['pk_column_name'].tolist() or target_pk['pk_column_name'].tolist()
                if not key_columns:
                    raise MetadataError(f"Primary key not found in the source neither in the target and not provided") 

            if include_columns:
            
                if not set(include_columns) & set(key_columns):
                    app_logger.warning(f'The primary key was not included in the column list.\
                                       The key column was included in the resulting query automatically. PK:{key_columns}') 

                include_columns = list(set(include_columns + key_columns))

                source_columns_meta = source_columns_meta[
                    source_columns_meta['column_name'].isin(include_columns)
                ]
                target_columns_meta = target_columns_meta[
                    target_columns_meta['column_name'].isin(include_columns)
                ]
            
            if exclude_columns:

                if set(exclude_columns) & set(key_columns):
                    app_logger.warning(f'The primary key has been excluded from the column list.\
                                       However, the key column must be present in the resulting query.s PK:{key_columns}') 

                exclude_columns = list(set(exclude_columns) - set(key_columns))

                source_columns_meta = source_columns_meta[
                    ~source_columns_meta['column_name'].isin(exclude_columns)
                ]
                target_columns_meta = target_columns_meta[
                    ~target_columns_meta['column_name'].isin(exclude_columns)
                ]

            common_cols_df, source_only_cols, target_only_cols = self._analyze_columns_meta(source_columns_meta, target_columns_meta)
            common_cols = common_cols_df['column_name'].tolist()

            if not common_cols:
                raise MetadataError(f"No one column to compare, need to check tables or reduce the exclude_columns list: {','.join(exclude_columns)}")
            
            source_data, source_query, source_params = self._get_table_data(
                self.source_engine, source_table, source_columns_meta, common_cols,
                date_column, update_column, start_date, end_date, exclude_recent_hours
            )

            target_data, target_query, target_params = self._get_table_data(
                self.target_engine, target_table, target_columns_meta, common_cols,
                date_column, update_column, start_date, end_date, exclude_recent_hours
            )
            status = None
            #special case
            if target_data.empty and source_data.empty:
                status = ct.COMPARISON_SKIPPED
                return status, None, None, None
            elif source_data.empty or target_data.empty:
                raise DQCompareException(f"Nothing to compare, rows returned from source: {len(source_data)}, from target: {len(target_data)}")


            source_data = prepare_dataframe(source_data)
            target_data = prepare_dataframe(target_data)
            if update_column and exclude_recent_hours:
                source_data, target_data = clean_recently_changed_data(source_data, target_data, key_columns)


            stats, details = compare_dataframes(
                source_data, target_data,
                key_columns, max_examples
            )

            if stats:
                details.skipped_source_columns = source_only_cols
                details.skipped_target_columns = target_only_cols

                report = generate_comparison_sample_report(source_table.full_name,
                                                            target_table.full_name,
                                                            stats,
                                                            details,
                                                            self.timezone,
                                                            source_query,
                                                            source_params,
                                                            target_query,
                                                            target_params
                                                            )
                status = ct.COMPARISON_FAILED if stats.final_diff_score > tolerance_percentage else ct.COMPARISON_SUCCESS
                return status, report, stats, details
            else:
                status = ct.COMPARISON_SKIPPED
                return status, None, None, None

        except Exception as e:
            app_logger.error(f"Sample comparison failed: {str(e)}")
            raise

    def compare_custom_query(
        self,
        source_query: str,
        source_params: Tuple[str, Dict],
        target_query: str,
        target_params: Tuple[str, Dict],
        custom_primary_key: List[str],
        exclude_columns: Optional[List[str]] = None,
        tolerance_percentage: float = 0.0,
        max_examples:Optional[int] = ct.DEFAULT_MAX_EXAMPLES
    ) -> Tuple[str, str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:
        """
        Compare data from custom queries with specified key columns

        Parameters:
            source_query : Union[str, Tuple[str, Dict]]  
                Source query (can be string or tuple with query and params).
            target_query : Union[str, Tuple[str, Dict]]
                Target query (can be string or tuple with query and params).
            custom_primary_key : List[str]
                List of primary key columns for comparison.
            exclude_columns : Optional[List[str]] = None 
                Columns to exclude from comparison.
            tolerance_percentage : float 
                Tolerance percentage for discrepancies.
            max_examples: int
                Maximum number of discrepancy examples per column 
                
        Returns:
        ----------
            Tuple[str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]
        """
        source_engine = self.source_engine
        target_engine = self.target_engine
        timezone = self.timezone

        try:
            self.comparison_stats['compared'] += 1

            # Execute queries
            source_data = self._execute_query((source_query,source_params), source_engine, timezone)
            target_data = self._execute_query((target_query,target_params), target_engine, timezone)
            app_logger.info('preparing source dataframe')
            source_data_prepared = prepare_dataframe(source_data)
            app_logger.info('preparing target dataframe')
            target_data_prepared = prepare_dataframe(target_data)

            # Exclude columns if specified
            exclude_cols = exclude_columns or []
            common_cols = [col for col in source_data_prepared.columns
                        if col in target_data_prepared.columns and col not in exclude_cols]

            source_data_filtered = source_data_prepared[common_cols]
            target_data_filtered = target_data_prepared[common_cols]
            if 'xrecently_changed' in common_cols:
                source_data_filtered, target_data_filtered = clean_recently_changed_data(source_data_filtered, target_data_filtered, custom_primary_key)
            # Compare dataframes
            stats, details = compare_dataframes(
                source_data_filtered, target_data_filtered, custom_primary_key, max_examples
            )

            if stats:
                report = generate_comparison_sample_report(None,
                                                           None,
                                                           stats,
                                                           details,
                                                           self.timezone,
                                                           source_query,
                                                           source_params,
                                                           target_query,
                                                           target_params
                                                          )
                status = ct.COMPARISON_FAILED if stats.final_diff_score > tolerance_percentage else ct.COMPARISON_SUCCESS
            else:
                status = ct.COMPARISON_SKIPPED


            self._update_stats(status, None)
            return status, report, stats, details

        except Exception as e:
            app_logger.exception("Custom query comparison failed")
            status = ct.COMPARISON_FAILED
            self._update_stats(status, None)
            return status, None, None, None
    def _get_metadata_cols(self, data_ref: DataReference, engine: Engine) -> pd.DataFrame:
        """Get metadata with proper source handling"""
        adapter = self._get_adapter(DBMSType.from_engine(engine))

        query, params = adapter.build_metadata_columns_query(data_ref)
        columns_meta = self._execute_query((query, params), engine)

        if columns_meta.empty:
            raise ValueError(f"Failed to get metadata for: {data_ref.full_name}")

        return columns_meta

    def _get_metadata_pk(self, data_ref: DataReference, engine: Engine) -> pd.DataFrame:
        """Get metadata with proper source handling
        """
        adapter = self._get_adapter(DBMSType.from_engine(engine))

        query, params = adapter.build_primary_key_query(data_ref)
        primary_key = self._execute_query((query, params), engine)

        return primary_key

    def _get_object_type(self, data_ref: DataReference, engine: Engine) -> pd.DataFrame:

        adapter = self._get_adapter(DBMSType.from_engine(engine))
        object_type = adapter.get_object_type(data_ref, engine)
        return object_type

    def _get_table_data(
        self,
        engine,
        data_ref: DataReference,
        metadata,
        columns: List[str],
        date_column: str,
        update_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        exclude_recent_hours: Optional[int]
    ) -> Tuple[pd.DataFrame, str, Dict] :
        """Retrieve and prepare table data"""
        db_type = DBMSType.from_engine(engine)
        adapter = self._get_adapter(db_type)
        app_logger.info(db_type)

        query, params = adapter.build_data_query_common(
            data_ref, columns, date_column, update_column,
            start_date, end_date, exclude_recent_hours
        )

        df = self._execute_query((query,params), engine, self.timezone)

        # Apply type conversions
        df = adapter.convert_types(df, metadata, self.timezone)

        return df, query, params

    def _get_adapter(self, db_type: DBMSType) -> BaseDatabaseAdapter:
        """Get adapter for specific DBMS"""
        try:
            return self.adapters[db_type]
        except KeyError:
            raise ValueError(f"No adapter available for {db_type}")

    def _execute_query(self, query: Union[str, Tuple[str, Dict]], engine: Engine, timezone: str = None) -> pd.DataFrame:
        """Execute SQL query using appropriate adapter"""
        db_type = DBMSType.from_engine(engine)
        adapter = self._get_adapter(db_type)
        df = adapter._execute_query(query, engine, timezone)
        validate_dataframe_size(df, ct.DEFAULT_MAX_SAMPLE_SIZE_GB)
        return df

    def _analyze_columns_meta(
        self,
        source_columns_meta: pd.DataFrame,
        target_columns_meta: pd.DataFrame
    ) -> tuple[pd.DataFrame, list, list]:
        """Find common columns between source and target and return unique columns for each"""

        source_columns = source_columns_meta['column_name'].tolist()
        target_columns = target_columns_meta['column_name'].tolist()

        common_columns = pd.merge(
            source_columns_meta, target_columns_meta,
            on='column_name', suffixes=('_source', '_target')
        )

        source_set = set(source_columns)
        target_set = set(target_columns)

        source_unique = list(source_set - target_set)
        target_unique = list(target_set - source_set)

        return common_columns, source_unique, target_unique

    def _validate_inputs(
        self,
        source: DataReference,
        target: DataReference
    ):
        """Validate input parameters"""
        if not isinstance(source, DataReference):
            raise TypeError("source must be a DataReference")
        if not isinstance(target, DataReference):
            raise TypeError("target must be a DataReference")