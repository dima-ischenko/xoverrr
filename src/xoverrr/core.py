from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy.engine import Engine

from . import constants as ct
from .adapters.base import BaseDatabaseAdapter
from .adapters.clickhouse import ClickHouseAdapter
from .adapters.oracle import OracleAdapter
from .adapters.postgres import PostgresAdapter
from .exceptions import DQCompareException, MetadataError
from .logger import app_logger
from .models import DataReference, DBMSType, ObjectType
from .persistence import ComparisonResultPersister
from .utils import (ComparisonDiffDetails, ComparisonStats,
                    build_comparison_stats, clean_recently_changed_data,
                    compare_dataframes, cross_fill_missing_dates,
                    generate_comparison_count_report,
                    generate_comparison_sample_report, normalize_column_names,
                    prepare_dataframe, validate_dataframe_size)
from .reporting import generate_sample_report, generate_count_report, ComparisonResult


class DataQualityComparator:
    """
    Main comparison class implementing data quality checks between databases.
    """

    def __init__(
        self,
        source_engine: Engine,
        target_engine: Engine,
        default_exclude_recent_hours: Optional[int] = 24,
        timezone: str = ct.DEFAULT_TZ,
        results_engine: Optional[Engine] = None,
    ):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.source_db_type = DBMSType.from_engine(source_engine)
        self.target_db_type = DBMSType.from_engine(target_engine)
        self.default_exclude_recent_hours = default_exclude_recent_hours
        self.timezone = timezone
        self.results_engine = results_engine
        self.result_persister = ComparisonResultPersister(
            results_engine=results_engine,
        )

        self.adapters = {
            DBMSType.ORACLE: OracleAdapter(),
            DBMSType.POSTGRESQL: PostgresAdapter(),
            DBMSType.CLICKHOUSE: ClickHouseAdapter(),
        }
        self._reset_stats()
        from . import __version__

        app_logger.info('start')
        app_logger.info(f'Version: v{__version__}')
        app_logger.info(f'Source DB: {self.source_db_type.name}')
        app_logger.info(f'Target DB: {self.target_db_type.name}')

    def reset_stats(self):
        self._reset_stats()

    def _reset_stats(self):
        self.comparison_stats = {
            'compared': 0,
            ct.COMPARISON_SUCCESS: 0,
            ct.COMPARISON_FAILED: 0,
            ct.COMPARISON_SKIPPED: 0,
            'tables_success': set(),
            'tables_failed': set(),
            'tables_skipped': set(),
            'start_time': pd.Timestamp.now().strftime(ct.DATETIME_FORMAT),
            'end_time': None,
        }

    def _update_stats(self, status: str, source_table: DataReference):
        """Update comparison statistics"""
        self.comparison_stats[status] += 1
        self.comparison_stats['end_time'] = pd.Timestamp.now().strftime(
            ct.DATETIME_FORMAT
        )
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
        chunk_size_days: Optional[int] = None,
        tolerance_percentage: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Union[bool, DataReference] = False,
        comparison_name: Optional[str] = None,
        comparison_tags: Optional[Dict] = None,
        report_output_format: str = 'json',
    ) -> Tuple[str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:

        self._validate_inputs(source_table, target_table)
        self._validate_report_output_options(
            report_output_format=report_output_format,
        )
        persist_enabled, persist_result_ref = self._resolve_persist_options(
            persist_result
        )

        start_date, end_date = date_range or (None, None)

        try:
            self.comparison_stats['compared'] += 1

            status, report, stats, details = self._compare_counts(
                source_table,
                target_table,
                date_column,
                start_date,
                end_date,
                chunk_size_days,
                tolerance_percentage,
                max_examples,
            )

            report = self._persist_outputs(
                status=status,
                report=report,
                stats=stats,
                details=details,
                comparison_type='count',
                comparison_name=comparison_name,
                comparison_tags=comparison_tags,
                source_table=source_table.full_name,
                target_table=target_table.full_name,
                persist_result=persist_enabled,
                persist_result_ref=persist_result_ref,
                report_output_format=report_output_format,
            )
            self._update_stats(status, source_table)
            return status, report, stats, details

        except Exception as e:
            app_logger.exception(f'Count comparison failed: {str(e)}')
            status = ct.COMPARISON_FAILED
            report = self._persist_outputs(
                status=status,
                report=None,
                stats=None,
                details=None,
                comparison_type='count',
                comparison_name=comparison_name,
                comparison_tags=comparison_tags,
                source_table=source_table.full_name,
                target_table=target_table.full_name,
                persist_result=persist_enabled,
                persist_result_ref=persist_result_ref,
                report_output_format=report_output_format,
            )
            self._update_stats(status, source_table)
            return status, report, None, None

    def compare_sample(
        self,
        source_table: DataReference,
        target_table: DataReference,
        date_column: Optional[str] = None,
        update_column: Optional[str] = None,
        date_range: Optional[Tuple[str, str]] = None,
        chunk_size_days: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        custom_primary_key: Optional[List[str]] = None,
        tolerance_percentage: float = 0.0,
        exclude_recent_hours: Optional[int] = None,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Union[bool, DataReference] = False,
        comparison_name: Optional[str] = None,
        comparison_tags: Optional[Dict] = None,
        report_output_format: str = 'json',
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
        self._validate_report_output_options(
            report_output_format=report_output_format,
        )
        persist_enabled, persist_result_ref = self._resolve_persist_options(
            persist_result
        )

        exclude_hours = exclude_recent_hours or self.default_exclude_recent_hours

        start_date, end_date = date_range or (None, None)
        exclude_cols = normalize_column_names(exclude_columns or [])
        custom_keys = (
            normalize_column_names(custom_primary_key or [])
            if custom_primary_key
            else None
        )
        include_cols = normalize_column_names(include_columns or [])

        try:
            self.comparison_stats['compared'] += 1

            status, report, stats, details = self._compare_samples(
                source_table,
                target_table,
                date_column,
                update_column,
                start_date,
                end_date,
                chunk_size_days,
                exclude_cols,
                include_cols,
                custom_keys,
                tolerance_percentage,
                exclude_hours,
                max_examples,
            )

            report = self._persist_outputs(
                status=status,
                report=report,
                stats=stats,
                details=details,
                comparison_type='sample',
                comparison_name=comparison_name,
                comparison_tags=comparison_tags,
                source_table=source_table.full_name,
                target_table=target_table.full_name,
                persist_result=persist_enabled,
                persist_result_ref=persist_result_ref,
                report_output_format=report_output_format,
            )
            self._update_stats(status, source_table)
            return status, report, stats, details

        except Exception as e:
            app_logger.exception(f'Sample comparison failed: {str(e)}')
            status = ct.COMPARISON_FAILED
            report = self._persist_outputs(
                status=status,
                report=None,
                stats=None,
                details=None,
                comparison_type='sample',
                comparison_name=comparison_name,
                comparison_tags=comparison_tags,
                source_table=source_table.full_name,
                target_table=target_table.full_name,
                persist_result=persist_enabled,
                persist_result_ref=persist_result_ref,
                report_output_format=report_output_format,
            )
            self._update_stats(status, source_table)
            return status, report, None, None

    def _compare_counts(
        self,
        source_table: DataReference,
        target_table: DataReference,
        date_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        chunk_size_days: Optional[int],
        tolerance_percentage: float,
        max_examples: int,
    ) -> Tuple[str, str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:

        try:
            source_adapter = self._get_adapter(self.source_db_type)
            target_adapter = self._get_adapter(self.target_db_type)

            source_columns_meta = self._get_metadata_cols(
                source_table, self.source_engine
            )
            app_logger.info('source_columns meta:\n')
            app_logger.info(source_columns_meta.to_string(index=False))

            target_columns_meta = self._get_metadata_cols(
                target_table, self.target_engine
            )
            app_logger.info('target_columns meta:\n')
            app_logger.info(target_columns_meta.to_string(index=False))

            source_chunks = []
            target_chunks = []
            source_query, source_params = None, None
            target_query, target_params = None, None
            
            date_chunks = self._iter_date_chunks(
                date_column, start_date, end_date, chunk_size_days
            )

            for chunk_start, chunk_end in date_chunks:
                source_query, source_params = source_adapter.build_count_query_common(
                    source_table,
                    date_column,
                    chunk_start,
                    chunk_end,
                    source_columns_meta,
                    self.timezone,
                )
                chunk_source = self._execute_query(
                    (source_query, source_params), self.source_engine, self.timezone
                )
                source_chunks.append(chunk_source)

                target_query, target_params = target_adapter.build_count_query_common(
                    target_table,
                    date_column,
                    chunk_start,
                    chunk_end,
                    target_columns_meta,
                    self.timezone,
                )
                chunk_target = self._execute_query(
                    (target_query, target_params), self.target_engine, self.timezone
                )
                target_chunks.append(chunk_target)

            source_counts = pd.concat(source_chunks, ignore_index=True)
            target_counts = pd.concat(target_chunks, ignore_index=True)
            source_counts = source_counts.groupby('dt', as_index=False)['cnt'].sum()
            target_counts = target_counts.groupby('dt', as_index=False)['cnt'].sum()

            source_counts_filled, target_counts_filled = cross_fill_missing_dates(
                source_counts, target_counts
            )

            merged = source_counts_filled.merge(target_counts_filled, on='dt')
            total_count_source = source_counts_filled['cnt'].sum()
            total_count_taget = target_counts_filled['cnt'].sum()

            if (total_count_source, total_count_taget) == (0, 0):
                app_logger.warning('nothing to compare to you')
                status = ct.COMPARISON_SKIPPED
                return status, None, None, None

            else:
                result_diff_in_counters = abs(merged['cnt_x'] - merged['cnt_y']).sum()
                result_equal_in_counters = merged[['cnt_x', 'cnt_y']].min(axis=1).sum()

                discrepancies_counters_percentage = (
                    100
                    * result_diff_in_counters
                    / (result_diff_in_counters + result_equal_in_counters)
                )
                stats, details = compare_dataframes(
                    source_df=source_counts_filled,
                    target_df=target_counts_filled,
                    key_columns=['dt'],
                    max_examples=max_examples,
                )

                status = (
                    ct.COMPARISON_FAILED
                    if discrepancies_counters_percentage > tolerance_percentage
                    else ct.COMPARISON_SUCCESS
                )

                report = generate_count_report(
                    source_table.full_name,
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
                    target_params,
                )

                return status, report, stats, details

        except Exception as e:
            app_logger.error(f'Count comparison failed: {str(e)}')
            raise

    def _compare_samples(
        self,
        source_table: DataReference,
        target_table: DataReference,
        date_column: str,
        update_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        chunk_size_days: Optional[int],
        exclude_columns: List[str],
        include_columns: List[str],
        custom_key_columns: Optional[List[str]],
        tolerance_percentage: float,
        exclude_recent_hours: Optional[int],
        max_examples: Optional[int],
    ) -> Tuple[str, str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:

        try:
            source_object_type = self._get_object_type(source_table, self.source_engine)
            target_object_type = self._get_object_type(target_table, self.target_engine)
            app_logger.info(
                f'object type source: {source_object_type} vs target {target_object_type}'
            )

            source_columns_meta = self._get_metadata_cols(
                source_table, self.source_engine
            )
            app_logger.info('source_columns meta:\n')
            app_logger.info(source_columns_meta.to_string(index=False))

            target_columns_meta = self._get_metadata_cols(
                target_table, self.target_engine
            )
            app_logger.info('target_columns meta:\n')
            app_logger.info(target_columns_meta.to_string(index=False))

            intersect = list(set(include_columns) & set(exclude_columns))
            if intersect:
                app_logger.warning(
                    f'Intersection columns between Include and exclude: {",".join(intersect)}'
                )

            key_columns = None

            if custom_key_columns:
                key_columns = custom_key_columns
                source_cols = source_columns_meta['column_name'].tolist()
                target_cols = target_columns_meta['column_name'].tolist()

                missing_in_source = [
                    col for col in custom_key_columns if col not in source_cols
                ]
                missing_in_target = [
                    col for col in custom_key_columns if col not in target_cols
                ]

                if missing_in_source:
                    raise MetadataError(
                        f'Custom key columns missing in source: {missing_in_source}'
                    )
                if missing_in_target:
                    raise MetadataError(
                        f'Custom key columns missing in target: {missing_in_target}'
                    )
            else:
                source_pk = (
                    self._get_metadata_pk(source_table, self.source_engine)
                    if source_object_type == ObjectType.TABLE
                    else pd.DataFrame({'pk_column_name': []})
                )
                target_pk = (
                    self._get_metadata_pk(target_table, self.target_engine)
                    if target_object_type == ObjectType.TABLE
                    else pd.DataFrame({'pk_column_name': []})
                )

                if (
                    source_pk['pk_column_name'].tolist()
                    != target_pk['pk_column_name'].tolist()
                ):
                    app_logger.warning(
                        f'Primary keys differ: source={source_pk["pk_column_name"].tolist()}, target={target_pk["pk_column_name"].tolist()}'
                    )
                key_columns = (
                    source_pk['pk_column_name'].tolist()
                    or target_pk['pk_column_name'].tolist()
                )
                if not key_columns:
                    raise MetadataError(
                        f'Primary key not found in the source neither in the target and not provided'
                    )

            if include_columns:
                if not set(include_columns) & set(key_columns):
                    app_logger.warning(
                        f'The primary key was not included in the column list.\
                                       The key column was included in the resulting query automatically. PK:{key_columns}'
                    )

                include_columns = list(set(include_columns + key_columns))

                source_columns_meta = source_columns_meta[
                    source_columns_meta['column_name'].isin(include_columns)
                ]
                target_columns_meta = target_columns_meta[
                    target_columns_meta['column_name'].isin(include_columns)
                ]

            if exclude_columns:
                if set(exclude_columns) & set(key_columns):
                    app_logger.warning(
                        f'The primary key has been excluded from the column list.\
                                       However, the key column must be present in the resulting query.s PK:{key_columns}'
                    )

                exclude_columns = list(set(exclude_columns) - set(key_columns))

                source_columns_meta = source_columns_meta[
                    ~source_columns_meta['column_name'].isin(exclude_columns)
                ]
                target_columns_meta = target_columns_meta[
                    ~target_columns_meta['column_name'].isin(exclude_columns)
                ]

            common_cols_df, source_only_cols, target_only_cols = (
                self._analyze_columns_meta(source_columns_meta, target_columns_meta)
            )
            common_cols = common_cols_df['column_name'].tolist()

            if not common_cols:
                raise MetadataError(
                    f'No one column to compare, need to check tables or reduce the exclude_columns list: {",".join(exclude_columns)}'
                )

            return self._compare_samples_iterative(
                source_table=source_table,
                target_table=target_table,
                source_columns_meta=source_columns_meta,
                target_columns_meta=target_columns_meta,
                common_cols=common_cols,
                key_columns=key_columns,
                source_only_cols=source_only_cols,
                target_only_cols=target_only_cols,
                date_column=date_column,
                update_column=update_column,
                start_date=start_date,
                end_date=end_date,
                chunk_size_days=chunk_size_days,
                exclude_recent_hours=exclude_recent_hours,
                tolerance_percentage=tolerance_percentage,
                max_examples=max_examples,
            )

        except Exception as e:
            app_logger.error(f'Sample comparison failed: {str(e)}')
            raise

    def compare_custom_query(
        self,
        source_query: str,
        source_params: Tuple[str, Dict],
        target_query: str,
        target_params: Tuple[str, Dict],
        custom_primary_key: List[str],
        chunk_size_days: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
        tolerance_percentage: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Union[bool, DataReference] = False,
        comparison_name: Optional[str] = None,
        comparison_tags: Optional[Dict] = None,
        report_output_format: str = 'json',
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
        self._validate_report_output_options(
            report_output_format=report_output_format,
        )
        persist_enabled, persist_result_ref = self._resolve_persist_options(
            persist_result
        )

        try:
            self.comparison_stats['compared'] += 1

            # Get metadata for both queries
            app_logger.info('Getting metadata for source query')
            source_metadata = self._get_metadata_cols_for_custom_query(
                (source_query, source_params), source_engine
            )

            app_logger.info('Getting metadata for target query')
            target_metadata = self._get_metadata_cols_for_custom_query(
                (target_query, target_params), target_engine
            )

            source_adapter = self._get_adapter(self.source_db_type)
            target_adapter = self._get_adapter(self.target_db_type)
            date_chunks = self._resolve_custom_query_chunks(
                source_params, target_params, chunk_size_days
            )

            if len(date_chunks) == 1:
                stats, details = self._execute_custom_query_chunk(
                    source_query=source_query,
                    source_params=date_chunks[0][0],
                    target_query=target_query,
                    target_params=date_chunks[0][1],
                    source_engine=source_engine,
                    target_engine=target_engine,
                    source_adapter=source_adapter,
                    target_adapter=target_adapter,
                    source_metadata=source_metadata,
                    target_metadata=target_metadata,
                    custom_primary_key=custom_primary_key,
                    exclude_columns=exclude_columns,
                    max_examples=max_examples,
                    timezone=timezone,
                )
            else:
                stats, details = self._compare_custom_query_iterative(
                    source_query=source_query,
                    target_query=target_query,
                    chunk_ranges=date_chunks,
                    source_engine=source_engine,
                    target_engine=target_engine,
                    source_adapter=source_adapter,
                    target_adapter=target_adapter,
                    source_metadata=source_metadata,
                    target_metadata=target_metadata,
                    custom_primary_key=custom_primary_key,
                    exclude_columns=exclude_columns,
                    max_examples=max_examples,
                    timezone=timezone,
                )

            if not stats:
                status = ct.COMPARISON_SKIPPED
                report = None
            else:
                report = generate_comparison_sample_report(
                    None,
                    None,
                    stats,
                    details,
                    self.timezone,
                    source_query,
                    source_params,
                    target_query,
                    target_params,
                    date_chunks=date_chunks
                )
                status = (
                    ct.COMPARISON_FAILED
                    if stats.final_diff_score > tolerance_percentage
                    else ct.COMPARISON_SUCCESS
                )

            report = self._persist_outputs(
                status=status,
                report=report,
                stats=stats,
                details=details,
                comparison_type='custom_query',
                comparison_name=comparison_name,
                comparison_tags=comparison_tags,
                source_table=None,
                target_table=None,
                source_query=source_query,
                source_params=source_params,
                target_query=target_query,
                target_params=target_params,
                persist_result=persist_enabled,
                persist_result_ref=persist_result_ref,
                report_output_format=report_output_format,
            )
            self._update_stats(status, None)
            return status, report, stats, details

        except Exception as e:
            app_logger.exception('Custom query comparison failed')
            status = ct.COMPARISON_FAILED
            report = self._persist_outputs(
                status=status,
                report=None,
                stats=None,
                details=None,
                comparison_type='custom_query',
                comparison_name=comparison_name,
                comparison_tags=comparison_tags,
                source_table=None,
                target_table=None,
                source_query=source_query,
                source_params=source_params,
                target_query=target_query,
                target_params=target_params,
                persist_result=persist_enabled,
                persist_result_ref=persist_result_ref,
                report_output_format=report_output_format,
            )
            self._update_stats(status, None)
            return status, report, None, None

    def _validate_report_output_options(
        self,
        report_output_format: str,
    ) -> None:
        normalized_format = (report_output_format or 'json').lower()
        if normalized_format not in {'json', 'text'}:
            raise ValueError(
                "report_output_format must be either 'json' or 'text'"
            )

    def _resolve_persist_options(
        self, persist_result: Union[bool, DataReference]
    ) -> Tuple[bool, Optional[DataReference]]:
        if isinstance(persist_result, DataReference):
            return True, persist_result
        return bool(persist_result), None

    def _persist_outputs(
        self,
        status: str,
        report: Optional[str],
        stats: Optional[ComparisonStats],
        details: Optional[ComparisonDiffDetails],
        comparison_type: str,
        comparison_name: Optional[str] = None,
        comparison_tags: Optional[Dict] = None,
        source_table: Optional[str] = None,
        target_table: Optional[str] = None,
        source_query: Optional[str] = None,
        source_params: Optional[Dict] = None,
        target_query: Optional[str] = None,
        target_params: Optional[Dict] = None,
        persist_result: bool = False,
        persist_result_ref: Optional[DataReference] = None,
        report_output_format: str = 'json',
    ) -> Optional[str]:
        result = ComparisonResult(
            timestamp=pd.Timestamp.now().strftime(ct.DATETIME_FORMAT),
            comparison_type=comparison_type,
            status=status,
            comparison_name=comparison_name,
            comparison_tags=comparison_tags,
            report=report,
            source_table=source_table,
            target_table=target_table,
            timezone=self.timezone,
            stats=stats,
            details=details,
            source_query=source_query,
            source_params=source_params,
            target_query=target_query,
            target_params=target_params,
        )
        self.result_persister.persist(
            result=result,
            persist_result=persist_result,
            persist_result_ref=persist_result_ref,
        )
        if report_output_format == 'json':
            return result.to_json()
        return result.report

    def _resolve_custom_query_chunks(
        self,
        source_params: Dict,
        target_params: Dict,
        chunk_size_days: Optional[int],
    ) -> List[Tuple[Dict, Dict]]:
        source_params = source_params or {}
        target_params = target_params or {}
        source_start = source_params.get('start_date')
        source_end = source_params.get('end_date')
        target_start = target_params.get('start_date')
        target_end = target_params.get('end_date')

        if not (
            chunk_size_days
            and source_start is not None
            and source_end is not None
            and target_start is not None
            and target_end is not None
        ):
            return [(dict(source_params), dict(target_params))]

        source_chunks = self._iter_date_chunks(
            'date', source_start, source_end, chunk_size_days
        )
        target_chunks = self._iter_date_chunks(
            'date', target_start, target_end, chunk_size_days
        )
        if len(source_chunks) != len(target_chunks):
            raise ValueError(
                'source and target custom query date ranges produce different chunk counts'
            )

        chunk_ranges: List[Tuple[Dict, Dict]] = []
        for (s_start, s_end), (t_start, t_end) in zip(source_chunks, target_chunks):
            source_chunk_params = dict(source_params)
            target_chunk_params = dict(target_params)
            source_chunk_params['start_date'] = s_start
            source_chunk_params['end_date'] = s_end
            target_chunk_params['start_date'] = t_start
            target_chunk_params['end_date'] = t_end
            chunk_ranges.append((source_chunk_params, target_chunk_params))
        return chunk_ranges

    def _execute_custom_query_chunk(
        self,
        source_query: str,
        source_params: Dict,
        target_query: str,
        target_params: Dict,
        source_engine: Engine,
        target_engine: Engine,
        source_adapter,
        target_adapter,
        source_metadata: pd.DataFrame,
        target_metadata: pd.DataFrame,
        custom_primary_key: List[str],
        exclude_columns: Optional[List[str]],
        max_examples: Optional[int],
        timezone: str,
    ) -> Tuple[Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:
        source_data = self._execute_query(
            (source_query, source_params), source_engine, timezone
        )
        target_data = self._execute_query(
            (target_query, target_params), target_engine, timezone
        )

        source_data = source_adapter.convert_types(
            source_data, source_metadata, timezone
        )
        target_data = target_adapter.convert_types(
            target_data, target_metadata, timezone
        )
        source_data_prepared = prepare_dataframe(source_data)
        target_data_prepared = prepare_dataframe(target_data)

        exclude_cols = exclude_columns or []
        common_cols = [
            col
            for col in source_data_prepared.columns
            if col in target_data_prepared.columns and col not in exclude_cols
        ]
        source_data_filtered = source_data_prepared[common_cols]
        target_data_filtered = target_data_prepared[common_cols]
        if 'xrecently_changed' in common_cols:
            source_data_filtered, target_data_filtered = clean_recently_changed_data(
                source_data_filtered, target_data_filtered, custom_primary_key
            )
        return compare_dataframes(
            source_data_filtered,
            target_data_filtered,
            custom_primary_key,
            max_examples,
        )

    def _compare_custom_query_iterative(
        self,
        source_query: str,
        target_query: str,
        chunk_ranges: List[Tuple[Dict, Dict]],
        source_engine: Engine,
        target_engine: Engine,
        source_adapter,
        target_adapter,
        source_metadata: pd.DataFrame,
        target_metadata: pd.DataFrame,
        custom_primary_key: List[str],
        exclude_columns: Optional[List[str]],
        max_examples: Optional[int],
        timezone: str,
    ) -> Tuple[Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:
        examples_limit = max_examples or ct.DEFAULT_MAX_EXAMPLES
        total_source_rows = 0
        total_target_rows = 0
        dup_source_rows = 0
        dup_target_rows = 0
        only_source_rows = 0
        only_target_rows = 0
        common_pk_rows = 0
        total_matched_rows = 0
        mismatch_counter = defaultdict(int)
        has_data = False

        dup_source_examples: set = set()
        dup_target_examples: set = set()
        source_only_examples: set = set()
        target_only_examples: set = set()
        discrepant_chunks: List[pd.DataFrame] = []
        discrepancy_examples_rows: List[Dict] = []
        discrepancy_examples_by_col = defaultdict(int)

        for source_chunk_params, target_chunk_params in chunk_ranges:
            chunk_stats, chunk_details = self._execute_custom_query_chunk(
                source_query=source_query,
                source_params=source_chunk_params,
                target_query=target_query,
                target_params=target_chunk_params,
                source_engine=source_engine,
                target_engine=target_engine,
                source_adapter=source_adapter,
                target_adapter=target_adapter,
                source_metadata=source_metadata,
                target_metadata=target_metadata,
                custom_primary_key=custom_primary_key,
                exclude_columns=exclude_columns,
                max_examples=examples_limit,
                timezone=timezone,
            )
            if not chunk_stats:
                continue
            has_data = True
            total_source_rows += chunk_stats.total_source_rows
            total_target_rows += chunk_stats.total_target_rows
            dup_source_rows += chunk_stats.dup_source_rows
            dup_target_rows += chunk_stats.dup_target_rows
            only_source_rows += chunk_stats.only_source_rows
            only_target_rows += chunk_stats.only_target_rows
            common_pk_rows += chunk_stats.common_pk_rows
            total_matched_rows += chunk_stats.total_matched_rows

            if not chunk_details.mismatches_per_column.empty:
                for row in chunk_details.mismatches_per_column.itertuples(index=False):
                    mismatch_counter[row.column_name] += int(row.mismatch_count)

            self._merge_examples_set(
                dup_source_examples,
                chunk_details.dup_source_keys_examples,
                examples_limit,
            )
            self._merge_examples_set(
                dup_target_examples,
                chunk_details.dup_target_keys_examples,
                examples_limit,
            )
            self._merge_examples_set(
                source_only_examples,
                chunk_details.source_only_keys_examples,
                examples_limit,
            )
            self._merge_examples_set(
                target_only_examples,
                chunk_details.target_only_keys_examples,
                examples_limit,
            )

            if (
                chunk_details.discrepant_data_examples is not None
                and not chunk_details.discrepant_data_examples.empty
                and len(discrepant_chunks) < examples_limit
            ):
                needed = examples_limit * 2
                current_cnt = sum(len(x) for x in discrepant_chunks)
                if current_cnt < needed:
                    remain = needed - current_cnt
                    discrepant_chunks.append(
                        chunk_details.discrepant_data_examples.head(remain)
                    )

            if (
                chunk_details.discrepancies_per_col_examples is not None
                and not chunk_details.discrepancies_per_col_examples.empty
            ):
                for row in chunk_details.discrepancies_per_col_examples.to_dict(
                    'records'
                ):
                    col = row['column_name']
                    if discrepancy_examples_by_col[col] < examples_limit:
                        discrepancy_examples_rows.append(row)
                        discrepancy_examples_by_col[col] += 1

        if not has_data:
            return None, None

        stats = build_comparison_stats(
            total_source_rows=total_source_rows,
            total_target_rows=total_target_rows,
            dup_source_rows=dup_source_rows,
            dup_target_rows=dup_target_rows,
            only_source_rows=only_source_rows,
            only_target_rows=only_target_rows,
            common_pk_rows=common_pk_rows,
            total_matched_rows=total_matched_rows,
            mismatch_counts=list(mismatch_counter.values()),
        )
        mismatches_per_column = (
            pd.DataFrame(
                sorted(
                    mismatch_counter.items(), key=lambda item: item[1], reverse=True
                ),
                columns=['column_name', 'mismatch_count'],
            )
            if mismatch_counter
            else pd.DataFrame(columns=['column_name', 'mismatch_count'])
        )
        details = ComparisonDiffDetails(
            mismatches_per_column=mismatches_per_column,
            discrepancies_per_col_examples=(
                pd.DataFrame(discrepancy_examples_rows)
                if discrepancy_examples_rows
                else pd.DataFrame()
            ),
            dup_source_keys_examples=tuple(dup_source_examples) or None,
            dup_target_keys_examples=tuple(dup_target_examples) or None,
            source_only_keys_examples=tuple(source_only_examples) or None,
            target_only_keys_examples=tuple(target_only_examples) or None,
            discrepant_data_examples=(
                pd.concat(discrepant_chunks, ignore_index=True)
                if discrepant_chunks
                else pd.DataFrame()
            ),
            common_attribute_columns=[],
        )
        return stats, details

    def _get_metadata_cols_for_custom_query(
        self, query, engine: Engine
    ) -> pd.DataFrame:
        """Get metadata with proper source handling"""
        adapter = self._get_adapter(DBMSType.from_engine(engine))

        columns_meta = adapter.get_metadata_for_custom_query(query, engine)

        if columns_meta.empty:
            raise ValueError(f'Failed to get metadata for custom query: {query}')

        return columns_meta

    def _get_metadata_cols(
        self, data_ref: DataReference, engine: Engine
    ) -> pd.DataFrame:
        """Get metadata with proper source handling"""
        adapter = self._get_adapter(DBMSType.from_engine(engine))

        query, params = adapter.build_metadata_columns_query(data_ref)
        columns_meta = self._execute_query((query, params), engine)

        if columns_meta.empty:
            raise ValueError(f'Failed to get metadata for: {data_ref.full_name}')

        return columns_meta

    def _get_metadata_pk(self, data_ref: DataReference, engine: Engine) -> pd.DataFrame:
        """Get metadata with proper source handling"""
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
        columns_meta: pd.DataFrame,
        common_columns: List[str],
        date_column: str,
        update_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        exclude_recent_hours: Optional[int],
    ) -> Tuple[pd.DataFrame, str, Dict]:
        """Retrieve and prepare table data"""
        db_type = DBMSType.from_engine(engine)
        adapter = self._get_adapter(db_type)
        app_logger.info(db_type)

        query, params = adapter.build_data_query_common(
            data_ref,
            common_columns,
            date_column,
            update_column,
            start_date,
            end_date,
            exclude_recent_hours,
            columns_meta,
            self.timezone,
        )

        df = self._execute_query((query, params), engine, self.timezone)

        # Apply type conversions
        df = adapter.convert_types(df, columns_meta, self.timezone)

        return df, query, params

    def _get_adapter(self, db_type: DBMSType) -> BaseDatabaseAdapter:
        """Get adapter for specific DBMS"""
        try:
            return self.adapters[db_type]
        except KeyError:
            raise ValueError(f'No adapter available for {db_type}')

    def _iter_date_chunks(
        self,
        date_column: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        chunk_size_days: Optional[int],
    ) -> List[Tuple[Optional[str], Optional[str]]]:
        if chunk_size_days is not None and chunk_size_days <= 0:
            raise ValueError('chunk_size_days must be greater than 0')

        if not (
            chunk_size_days
            and date_column
            and start_date is not None
            and end_date is not None
        ):
            return [(start_date, end_date)]

        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        if start_ts > end_ts:
            raise ValueError(
                f'date_range start {start_date} is greater than end {end_date}'
            )

        chunks: List[Tuple[str, str]] = []
        current = start_ts
        while current <= end_ts:
            chunk_end = min(current + pd.Timedelta(days=chunk_size_days - 1), end_ts)
            chunks.append(
                (
                    current.strftime(ct.DATE_FORMAT),
                    chunk_end.strftime(ct.DATE_FORMAT),
                )
            )
            current = chunk_end + pd.Timedelta(days=1)
        return chunks

    def _compare_samples_iterative(
        self,
        source_table: DataReference,
        target_table: DataReference,
        source_columns_meta: pd.DataFrame,
        target_columns_meta: pd.DataFrame,
        common_cols: List[str],
        key_columns: List[str],
        source_only_cols: List[str],
        target_only_cols: List[str],
        date_column: Optional[str],
        update_column: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        chunk_size_days: Optional[int],
        exclude_recent_hours: Optional[int],
        tolerance_percentage: float,
        max_examples: Optional[int],
    ) -> Tuple[str, str, Optional[ComparisonStats], Optional[ComparisonDiffDetails]]:
        examples_limit = max_examples or ct.DEFAULT_MAX_EXAMPLES

        total_source_rows = 0
        total_target_rows = 0
        dup_source_rows = 0
        dup_target_rows = 0
        only_source_rows = 0
        only_target_rows = 0
        common_pk_rows = 0
        total_matched_rows = 0
        mismatch_counter = defaultdict(int)

        dup_source_examples: set = set()
        dup_target_examples: set = set()
        source_only_examples: set = set()
        target_only_examples: set = set()
        discrepant_chunks: List[pd.DataFrame] = []
        discrepancy_examples_rows: List[Dict] = []
        discrepancy_examples_by_col = defaultdict(int)

        total_source_rows_raw = 0
        total_target_rows_raw = 0
        source_query, source_params = None, None
        target_query, target_params = None, None

        date_chunks = self._iter_date_chunks(
            date_column, start_date, end_date, chunk_size_days
        )
        for chunk_start, chunk_end in date_chunks:
            source_data, source_query, source_params = self._get_table_data(
                self.source_engine,
                source_table,
                source_columns_meta,
                common_cols,
                date_column,
                update_column,
                chunk_start,
                chunk_end,
                exclude_recent_hours,
            )
            target_data, target_query, target_params = self._get_table_data(
                self.target_engine,
                target_table,
                target_columns_meta,
                common_cols,
                date_column,
                update_column,
                chunk_start,
                chunk_end,
                exclude_recent_hours,
            )

            total_source_rows_raw += len(source_data)
            total_target_rows_raw += len(target_data)

            if source_data.empty and target_data.empty:
                continue

            source_data = prepare_dataframe(source_data)
            target_data = prepare_dataframe(target_data)
            if update_column and exclude_recent_hours:
                source_data, target_data = clean_recently_changed_data(
                    source_data, target_data, key_columns
                )

            if source_data.empty and target_data.empty:
                continue

            chunk_stats, chunk_details = compare_dataframes(
                source_data, target_data, key_columns, examples_limit
            )
            if not chunk_stats:
                continue

            total_source_rows += chunk_stats.total_source_rows
            total_target_rows += chunk_stats.total_target_rows
            dup_source_rows += chunk_stats.dup_source_rows
            dup_target_rows += chunk_stats.dup_target_rows
            only_source_rows += chunk_stats.only_source_rows
            only_target_rows += chunk_stats.only_target_rows
            common_pk_rows += chunk_stats.common_pk_rows
            total_matched_rows += chunk_stats.total_matched_rows

            if not chunk_details.mismatches_per_column.empty:
                for row in chunk_details.mismatches_per_column.itertuples(index=False):
                    mismatch_counter[row.column_name] += int(row.mismatch_count)

            self._merge_examples_set(
                dup_source_examples,
                chunk_details.dup_source_keys_examples,
                examples_limit,
            )
            self._merge_examples_set(
                dup_target_examples,
                chunk_details.dup_target_keys_examples,
                examples_limit,
            )
            self._merge_examples_set(
                source_only_examples,
                chunk_details.source_only_keys_examples,
                examples_limit,
            )
            self._merge_examples_set(
                target_only_examples,
                chunk_details.target_only_keys_examples,
                examples_limit,
            )

            if (
                chunk_details.discrepant_data_examples is not None
                and not chunk_details.discrepant_data_examples.empty
                and len(discrepant_chunks) < examples_limit
            ):
                needed = examples_limit * 2
                current_cnt = sum(len(x) for x in discrepant_chunks)
                if current_cnt < needed:
                    remain = needed - current_cnt
                    discrepant_chunks.append(
                        chunk_details.discrepant_data_examples.head(remain)
                    )

            if (
                chunk_details.discrepancies_per_col_examples is not None
                and not chunk_details.discrepancies_per_col_examples.empty
            ):
                for row in chunk_details.discrepancies_per_col_examples.to_dict(
                    'records'
                ):
                    col = row['column_name']
                    if discrepancy_examples_by_col[col] < examples_limit:
                        discrepancy_examples_rows.append(row)
                        discrepancy_examples_by_col[col] += 1

        if (total_source_rows, total_target_rows) == (0, 0):
            status = ct.COMPARISON_SKIPPED
            return status, None, None, None

        stats = build_comparison_stats(
            total_source_rows=total_source_rows,
            total_target_rows=total_target_rows,
            dup_source_rows=dup_source_rows,
            dup_target_rows=dup_target_rows,
            only_source_rows=only_source_rows,
            only_target_rows=only_target_rows,
            common_pk_rows=common_pk_rows,
            total_matched_rows=total_matched_rows,
            mismatch_counts=list(mismatch_counter.values()),
        )

        mismatches_per_column = (
            pd.DataFrame(
                sorted(
                    mismatch_counter.items(),
                    key=lambda item: item[1],
                    reverse=True,
                ),
                columns=['column_name', 'mismatch_count'],
            )
            if mismatch_counter
            else pd.DataFrame(columns=['column_name', 'mismatch_count'])
        )
        discrepancies_per_col_examples = (
            pd.DataFrame(discrepancy_examples_rows)
            if discrepancy_examples_rows
            else pd.DataFrame()
        )
        discrepant_data_examples = (
            pd.concat(discrepant_chunks, ignore_index=True)
            if discrepant_chunks
            else pd.DataFrame()
        )

        details = ComparisonDiffDetails(
            mismatches_per_column=mismatches_per_column,
            discrepancies_per_col_examples=discrepancies_per_col_examples,
            dup_source_keys_examples=tuple(dup_source_examples) or None,
            dup_target_keys_examples=tuple(dup_target_examples) or None,
            source_only_keys_examples=tuple(source_only_examples) or None,
            target_only_keys_examples=tuple(target_only_examples) or None,
            discrepant_data_examples=discrepant_data_examples,
            common_attribute_columns=common_cols,
            skipped_source_columns=source_only_cols,
            skipped_target_columns=target_only_cols,
        )

        report = generate_comparison_sample_report(
            source_table.full_name,
            target_table.full_name,
            stats,
            details,
            self.timezone,
            source_query,
            source_params,
            target_query,
            target_params,
            date_chunks=date_chunks,
        )
        status = (
            ct.COMPARISON_FAILED
            if stats.final_diff_score > tolerance_percentage
            else ct.COMPARISON_SUCCESS
        )
        return status, report, stats, details

    def _merge_examples_set(
        self, target_set: set, source_items, max_examples: int
    ) -> None:
        if not source_items:
            return
        for item in source_items:
            if len(target_set) >= max_examples:
                break
            target_set.add(item)

    def _execute_query(
        self, query: Union[str, Tuple[str, Dict]], engine: Engine, timezone: str = None
    ) -> pd.DataFrame:
        """Execute SQL query using appropriate adapter"""
        db_type = DBMSType.from_engine(engine)
        adapter = self._get_adapter(db_type)
        df = adapter._execute_query(query, engine, timezone)
        validate_dataframe_size(df, ct.DEFAULT_MAX_SAMPLE_SIZE_GB)
        return df

    def _analyze_columns_meta(
        self, source_columns_meta: pd.DataFrame, target_columns_meta: pd.DataFrame
    ) -> tuple[pd.DataFrame, list, list]:
        """Find common columns between source and target and return unique columns for each"""

        source_columns = source_columns_meta['column_name'].tolist()
        target_columns = target_columns_meta['column_name'].tolist()

        common_columns = pd.merge(
            source_columns_meta,
            target_columns_meta,
            on='column_name',
            suffixes=('_source', '_target'),
        )

        source_set = set(source_columns)
        target_set = set(target_columns)

        source_unique = list(source_set - target_set)
        target_unique = list(target_set - source_set)

        return common_columns, source_unique, target_unique

    def _validate_inputs(self, source: DataReference, target: DataReference):
        """Validate input parameters"""
        if not isinstance(source, DataReference):
            raise TypeError('source must be a DataReference')
        if not isinstance(target, DataReference):
            raise TypeError('target must be a DataReference')
