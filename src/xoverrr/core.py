from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy.engine import Engine

from . import constants as ct
from .adapters.base import BaseDatabaseAdapter
from .adapters.clickhouse import ClickHouseAdapter
from .adapters.oracle import OracleAdapter
from .adapters.postgres import PostgresAdapter
from .exceptions import DQCheckException, MetadataError
from .logger import app_logger
from .models import DataReference, DBMSType, ObjectType
from .persistence import (
    CheckResultPersister,
    CheckRunTimings,
    PersistResultOptions,
    build_run_id,
    parse_persist_result_option,
)
from .utils import (CheckDetails, CheckStats,
                    build_check_stats, build_sniff_issue_stats,
                    clean_recently_changed_data,
                    compare_dataframes, cross_fill_missing_dates,
                    evaluate_sniff_query_data,
                    generate_check_count_report,
                    generate_check_sample_report, normalize_column_names,
                    prepare_dataframe, sniff_issue_row_count,
                    validate_dataframe_size)
from .reporting import (
    build_check_result,
    format_check_result,
    generate_count_report,
    generate_sample_report,
    generate_sniff_query_report,
    validate_report_output_format,
)
from .version import __version__


class DataQualityChecker:
    """
    Main checker class implementing data quality checks on and between databases.
    """

    def __init__(
        self,
        source_engine: Engine,
        target_engine: Optional[Engine] = None,
        default_exclude_recent_hours: Optional[int] = 24,
        timezone: str = ct.DEFAULT_TZ,
        results_engine: Optional[Engine] = None,
    ):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.source_db_type = DBMSType.from_engine(source_engine)
        self.target_db_type = (
            DBMSType.from_engine(target_engine) if target_engine is not None else None
        )
        self.default_exclude_recent_hours = default_exclude_recent_hours
        self.timezone = timezone
        self.results_engine = results_engine
        self.result_persister = CheckResultPersister(
            results_engine=results_engine,
        )

        self.adapters = {
            DBMSType.ORACLE: OracleAdapter(),
            DBMSType.POSTGRESQL: PostgresAdapter(),
            DBMSType.CLICKHOUSE: ClickHouseAdapter(),
        }
        self._reset_stats()
        self._report_context = {
            'library_version': __version__,
            'source_db_type': self.source_db_type.name.lower(),
            'target_db_type': (
                self.target_db_type.name.lower() if self.target_db_type else None
            ),
        }
        app_logger.info('start')
        app_logger.info(f'Version: v{self._report_context["library_version"]}')
        app_logger.info(f'Source DB: {self._report_context["source_db_type"]}')
        target_db_label = self._report_context['target_db_type'] or 'not configured'
        app_logger.info(f'Target DB: {target_db_label}')

    def reset_stats(self):
        self._reset_stats()

    def _reset_stats(self):
        self.check_stats = {
            'checked': 0,
            ct.CHECK_SUCCESS: 0,
            ct.CHECK_FAILED: 0,
            ct.CHECK_SKIPPED: 0,
            'tables_success': set(),
            'tables_failed': set(),
            'tables_skipped': set(),
            'start_time': pd.Timestamp.now().strftime(ct.DATETIME_FORMAT),
            'end_time': None,
        }

    def _update_stats(self, status: str, source_table: DataReference):
        """Update check statistics"""
        self.check_stats[status] += 1
        self.check_stats['end_time'] = pd.Timestamp.now().strftime(
            ct.DATETIME_FORMAT
        )
        if source_table:
            match status:
                case ct.CHECK_SUCCESS:
                    self.check_stats['tables_success'].add(source_table.full_name)
                case ct.CHECK_FAILED:
                    self.check_stats['tables_failed'].add(source_table.full_name)
                case ct.CHECK_SKIPPED:
                    self.check_stats['tables_skipped'].add(source_table.full_name)

    def check_counts(
        self,
        source_table: DataReference,
        target_table: DataReference,
        check_name: Optional[str] = None,
        date_column: Optional[str] = None,
        date_range: Optional[Tuple[str, str]] = None,
        chunk_size_days: Optional[int] = None,
        tolerance_pct: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Union[bool, DataReference] = False,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
    ) -> Tuple[str, Optional[CheckStats], Optional[CheckDetails]]:

        self._validate_inputs(source_table, target_table)
        self._require_target_engine()
        validate_report_output_format(report_output_format)
        persist_options = parse_persist_result_option(persist_result)
        run_id, run_started_at = self._start_check_run(
            ct.CHECK_TYPE_COUNT, check_name
        )

        start_date, end_date = date_range or (None, None)

        try:
            self.check_stats['checked'] += 1

            status, draft_report, stats, details = self._check_counts(
                source_table,
                target_table,
                date_column,
                start_date,
                end_date,
                chunk_size_days,
                tolerance_pct,
                max_examples,
                run_id=run_id,
                run_started_at=run_started_at,
            )

            report = self._finalize_check(
                status=status,
                report=draft_report,
                stats=stats,
                details=details,
                check_type=ct.CHECK_TYPE_COUNT,
                check_name=check_name,
                check_tags=check_tags,
                source_table=source_table.full_name,
                target_table=target_table.full_name,
                persist_options=persist_options,
                report_output_format=report_output_format,
            )
            self._update_stats(status, source_table)
            return status, report, stats, details

        except Exception as e:
            app_logger.exception(f'Count check failed: {str(e)}')
            status = ct.CHECK_FAILED
            report = self._finalize_check(
                status=status,
                report=None,
                stats=None,
                details=None,
                check_type=ct.CHECK_TYPE_COUNT,
                check_name=check_name,
                check_tags=check_tags,
                source_table=source_table.full_name,
                target_table=target_table.full_name,
                persist_options=persist_options,
                report_output_format=report_output_format,
            )
            self._update_stats(status, source_table)
            return status, report, None, None

    def check_sample(
        self,
        source_table: DataReference,
        target_table: DataReference,
        check_name: Optional[str] = None,
        date_column: Optional[str] = None,
        update_column: Optional[str] = None,
        date_range: Optional[Tuple[str, str]] = None,
        chunk_size_days: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        custom_primary_key: Optional[List[str]] = None,
        tolerance_pct: float = 0.0,
        exclude_recent_hours: Optional[int] = None,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Union[bool, DataReference] = False,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
    ) -> Tuple[str, str, Optional[CheckStats], Optional[CheckDetails]]:
        """
        Compare data from custom queries with specified key columns

        Parameters:
            source_table: `DataReference`
                source table to check
            target_table: `DataReference`
                target table to check
            custom_primary_key : `List[str]`
                List of primary key columns for the check.
            exclude_columns : `Optional[List[str]] = None`
                Columns to exclude from the check.
            include_columns : `Optional[List[str]] = None`
                Columns to include in the check (default all cols)
            tolerance_pct : `float`
                Tolerance pct for discrepancies (0–100).
            max_examples
                Maximum number of discrepancy examples per column
        """
        self._validate_inputs(source_table, target_table)
        self._require_target_engine()
        validate_report_output_format(report_output_format)
        persist_options = parse_persist_result_option(persist_result)
        run_id, run_started_at = self._start_check_run(
            ct.CHECK_TYPE_SAMPLE, check_name
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
            self.check_stats['checked'] += 1

            status, draft_report, stats, details = self._check_samples(
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
                tolerance_pct,
                exclude_hours,
                max_examples,
                run_id=run_id,
                run_started_at=run_started_at,
            )

            report = self._finalize_check(
                status=status,
                report=draft_report,
                stats=stats,
                details=details,
                check_type=ct.CHECK_TYPE_SAMPLE,
                check_name=check_name,
                check_tags=check_tags,
                source_table=source_table.full_name,
                target_table=target_table.full_name,
                persist_options=persist_options,
                report_output_format=report_output_format,
            )
            self._update_stats(status, source_table)
            return status, report, stats, details

        except Exception as e:
            app_logger.exception(f'Sample check failed: {str(e)}')
            status = ct.CHECK_FAILED
            report = self._finalize_check(
                status=status,
                report=None,
                stats=None,
                details=None,
                check_type=ct.CHECK_TYPE_SAMPLE,
                check_name=check_name,
                check_tags=check_tags,
                source_table=source_table.full_name,
                target_table=target_table.full_name,
                persist_options=persist_options,
                report_output_format=report_output_format,
            )
            self._update_stats(status, source_table)
            return status, report, None, None

    def _start_check_run(
        self, check_type: str, check_name: Optional[str]
    ) -> Tuple[str, str]:
        run_started_at = pd.Timestamp.now().strftime(ct.DATETIME_FORMAT)
        run_id = build_run_id()
        app_logger.info(
            f'Check run started: run_id={run_id} '
            f'check_name={check_name} check_type={check_type}'
        )
        self._active_run_id = run_id
        self._active_run_started_at = run_started_at
        self._active_check_name = check_name
        self._run_timings = CheckRunTimings(run_started_at=run_started_at)
        return run_id, run_started_at

    def _check_counts(
        self,
        source_table: DataReference,
        target_table: DataReference,
        date_column: str,
        start_date: Optional[str],
        end_date: Optional[str],
        chunk_size_days: Optional[int],
        tolerance_pct: float,
        max_examples: int,
        run_id: str,
        run_started_at: str,
    ) -> Tuple[str, str, Optional[CheckStats], Optional[CheckDetails]]:

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
                    (source_query, source_params),
                    self.source_engine,
                    self.timezone,
                    query_side='source',
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
                    (target_query, target_params),
                    self.target_engine,
                    self.timezone,
                    query_side='target',
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
                status = ct.CHECK_SKIPPED
                return status, None, None, None

            else:
                result_diff_in_counters = abs(merged['cnt_x'] - merged['cnt_y']).sum()
                result_equal_in_counters = merged[['cnt_x', 'cnt_y']].min(axis=1).sum()

                discrepancies_counters_pct = (
                    100
                    * result_diff_in_counters
                    / (result_diff_in_counters + result_equal_in_counters)
                )
                stats, details = self._check_dataframes_timed(
                    source_df=source_counts_filled,
                    target_df=target_counts_filled,
                    key_columns=['dt'],
                    max_examples=max_examples,
                )

                status = (
                    ct.CHECK_FAILED
                    if discrepancies_counters_pct > tolerance_pct
                    else ct.CHECK_SUCCESS
                )

                report = generate_count_report(
                    source_table.full_name,
                    target_table.full_name,
                    stats,
                    details,
                    total_count_source,
                    total_count_taget,
                    discrepancies_counters_pct,
                    result_diff_in_counters,
                    result_equal_in_counters,
                    self.timezone,
                    run_id,
                    run_started_at,
                    source_query,
                    source_params,
                    target_query,
                    target_params,
                    **self._report_context,
                )

                return status, report, stats, details

        except Exception as e:
            app_logger.error(f'Count check failed: {str(e)}')
            raise

    def _check_samples(
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
        tolerance_pct: float,
        exclude_recent_hours: Optional[int],
        max_examples: Optional[int],
        run_id: str,
        run_started_at: str,
    ) -> Tuple[str, str, Optional[CheckStats], Optional[CheckDetails]]:

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

            return self._check_samples_iterative(
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
                tolerance_pct=tolerance_pct,
                max_examples=max_examples,
                run_id=run_id,
                run_started_at=run_started_at,
            )

        except Exception as e:
            app_logger.error(f'Sample check failed: {str(e)}')
            raise

    def sniff_query(
        self,
        source_query: str,
        source_params: Optional[Dict] = None,
        check_name: Optional[str] = None,
        chunk_size_days: Optional[int] = None,
        tolerance_pct: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Union[bool, DataReference] = False,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
    ) -> Tuple[str, str, Optional[CheckStats], Optional[CheckDetails]]:
        """
        Sniff out data issues with a source-only SQL check.

        Row-level and scalar pass/fail checks both use ``xsniff_passed``
        (``y`` = passed, ``n`` = failed).
        """
        source_engine = self.source_engine
        timezone = self.timezone
        source_params = source_params or {}

        validate_report_output_format(report_output_format)
        persist_options = parse_persist_result_option(persist_result)
        run_id, run_started_at = self._start_check_run(
            ct.CHECK_TYPE_SNIFF_QUERY, check_name
        )

        try:
            self.check_stats['checked'] += 1

            app_logger.info('Getting metadata for sniff query')
            source_metadata = self._get_metadata_cols_for_custom_query(
                (source_query, source_params), source_engine
            )
            source_adapter = self._get_adapter(self.source_db_type)
            source_chunks = self._resolve_source_check_query_chunks(
                source_params, chunk_size_days
            )

            if len(source_chunks) == 1:
                stats, details = self._execute_source_check_query_chunk(
                    source_query=source_query,
                    source_params=source_chunks[0],
                    source_engine=source_engine,
                    source_adapter=source_adapter,
                    source_metadata=source_metadata,
                    max_examples=max_examples,
                    timezone=timezone,
                )
            else:
                stats, details = self._sniff_query_iterative(
                    source_query=source_query,
                    source_chunks=source_chunks,
                    source_engine=source_engine,
                    source_adapter=source_adapter,
                    source_metadata=source_metadata,
                    max_examples=max_examples,
                    timezone=timezone,
                )

            date_chunks = [
                (chunk.get('start_date'), chunk.get('end_date'))
                for chunk in source_chunks
                if chunk.get('start_date') is not None
                and chunk.get('end_date') is not None
            ] or None

            if not stats:
                status = ct.CHECK_SKIPPED
                draft_report = None
            else:
                status = (
                    ct.CHECK_FAILED
                    if stats.final_diff_score > tolerance_pct
                    else ct.CHECK_SUCCESS
                )
                draft_report = generate_sniff_query_report(
                    stats,
                    details,
                    self.timezone,
                    run_id,
                    run_started_at,
                    source_query,
                    source_params,
                    date_chunks=date_chunks,
                    library_version=self._report_context['library_version'],
                    source_db_type=self._report_context['source_db_type'],
                )

            report = self._finalize_check(
                status=status,
                report=draft_report,
                stats=stats,
                details=details,
                check_type=ct.CHECK_TYPE_SNIFF_QUERY,
                check_name=check_name,
                check_tags=check_tags,
                source_table=None,
                target_table=None,
                source_query=source_query,
                source_params=source_params,
                persist_options=persist_options,
                report_output_format=report_output_format,
            )
            self._update_stats(status, None)
            return status, report, stats, details

        except Exception:
            app_logger.exception('Sniff query failed')
            status = ct.CHECK_FAILED
            report = self._finalize_check(
                status=status,
                report=None,
                stats=None,
                details=None,
                check_type=ct.CHECK_TYPE_SNIFF_QUERY,
                check_name=check_name,
                check_tags=check_tags,
                source_table=None,
                target_table=None,
                source_query=source_query,
                source_params=source_params,
                persist_options=persist_options,
                report_output_format=report_output_format,
            )
            self._update_stats(status, None)
            return status, report, None, None

    def check_query(
        self,
        source_query: str,
        source_params: Dict,
        target_query: str,
        target_params: Dict,
        custom_primary_key: List[str],
        check_name: Optional[str] = None,
        chunk_size_days: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
        tolerance_pct: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Union[bool, DataReference] = False,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
    ) -> Tuple[str, str, Optional[CheckStats], Optional[CheckDetails]]:
        """
        Compare data from custom queries with specified key columns.

        For source-only issue checks, use :meth:`sniff_query`.
        """
        self._require_target_engine()
        source_engine = self.source_engine
        target_engine = self.target_engine
        timezone = self.timezone
        source_params = source_params or {}
        target_params = target_params or {}
        exclude_cols = normalize_column_names(exclude_columns or [])
        custom_keys = normalize_column_names(custom_primary_key)
        if not custom_keys:
            raise ValueError('custom_primary_key is mandatory')

        validate_report_output_format(report_output_format)
        persist_options = parse_persist_result_option(persist_result)
        run_id, run_started_at = self._start_check_run(
            ct.CHECK_TYPE_CUSTOM_QUERY, check_name
        )

        try:
            self.check_stats['checked'] += 1

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
                    custom_primary_key=custom_keys,
                    exclude_columns=exclude_cols,
                    max_examples=max_examples,
                    timezone=timezone,
                )
            else:
                stats, details = self._check_query_iterative(
                    source_query=source_query,
                    target_query=target_query,
                    chunk_ranges=date_chunks,
                    source_engine=source_engine,
                    target_engine=target_engine,
                    source_adapter=source_adapter,
                    target_adapter=target_adapter,
                    source_metadata=source_metadata,
                    target_metadata=target_metadata,
                    custom_primary_key=custom_keys,
                    exclude_columns=exclude_cols,
                    max_examples=max_examples,
                    timezone=timezone,
                )

            if not stats:
                status = ct.CHECK_SKIPPED
                draft_report = None
            else:
                status = (
                    ct.CHECK_FAILED
                    if stats.final_diff_score > tolerance_pct
                    else ct.CHECK_SUCCESS
                )
                draft_report = generate_check_sample_report(
                    None,
                    None,
                    stats,
                    details,
                    self.timezone,
                    run_id,
                    run_started_at,
                    source_query,
                    source_params,
                    target_query,
                    target_params,
                    date_chunks=date_chunks,
                    **self._report_context,
                )

            report = self._finalize_check(
                status=status,
                report=draft_report,
                stats=stats,
                details=details,
                check_type=ct.CHECK_TYPE_CUSTOM_QUERY,
                check_name=check_name,
                check_tags=check_tags,
                source_table=None,
                target_table=None,
                source_query=source_query,
                source_params=source_params,
                target_query=target_query,
                target_params=target_params,
                persist_options=persist_options,
                report_output_format=report_output_format,
            )
            self._update_stats(status, None)
            return status, report, stats, details

        except Exception:
            app_logger.exception('Custom query check failed')
            status = ct.CHECK_FAILED
            report = self._finalize_check(
                status=status,
                report=None,
                stats=None,
                details=None,
                check_type=ct.CHECK_TYPE_CUSTOM_QUERY,
                check_name=check_name,
                check_tags=check_tags,
                source_table=None,
                target_table=None,
                source_query=source_query,
                source_params=source_params,
                target_query=target_query,
                target_params=target_params,
                persist_options=persist_options,
                report_output_format=report_output_format,
            )
            self._update_stats(status, None)
            return status, report, None, None

    def _finalize_check(
        self,
        *,
        status: str,
        report: Optional[str],
        stats: Optional[CheckStats],
        details: Optional[CheckDetails],
        check_type: str,
        persist_options: PersistResultOptions,
        report_output_format: str,
        check_name: Optional[str] = None,
        check_tags: Optional[Dict] = None,
        source_table: Optional[str] = None,
        target_table: Optional[str] = None,
        source_query: Optional[str] = None,
        source_params: Optional[Dict] = None,
        target_query: Optional[str] = None,
        target_params: Optional[Dict] = None,
    ) -> Optional[str]:
        if not getattr(self, '_active_run_id', None):
            raise RuntimeError('check run was not started; run_id is missing')
        self._run_timings.finish_run()
        result = build_check_result(
            run_id=self._active_run_id,
            timestamp=self._active_run_started_at,
            timezone=self.timezone,
            status=status,
            report=report,
            stats=stats,
            details=details,
            check_type=check_type,
            check_name=self._active_check_name,
            check_tags=check_tags,
            source_table=source_table,
            target_table=target_table,
            source_query=source_query,
            source_params=source_params,
            target_query=target_query,
            target_params=target_params,
            timings=self._run_timings,
        )
        self.result_persister.persist(
            result,
            persist_result=persist_options.enabled,
            persist_result_ref=persist_options.table_ref,
        )
        app_logger.info(
            f'Check run finished: run_id={self._active_run_id} status={status}'
        )
        return format_check_result(result, report_output_format)

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

    def _resolve_source_check_query_chunks(
        self,
        source_params: Dict,
        chunk_size_days: Optional[int],
    ) -> List[Dict]:
        source_params = dict(source_params or {})
        source_start = source_params.get('start_date')
        source_end = source_params.get('end_date')

        if not (
            chunk_size_days
            and source_start is not None
            and source_end is not None
        ):
            return [source_params]

        source_chunks = self._iter_date_chunks(
            'date', source_start, source_end, chunk_size_days
        )
        chunk_params: List[Dict] = []
        for start, end in source_chunks:
            params = dict(source_params)
            params['start_date'] = start
            params['end_date'] = end
            chunk_params.append(params)
        return chunk_params

    def _execute_source_check_query_chunk(
        self,
        source_query: str,
        source_params: Dict,
        source_engine: Engine,
        source_adapter,
        source_metadata: pd.DataFrame,
        max_examples: Optional[int],
        timezone: str,
    ) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
        source_data = self._execute_query(
            (source_query, source_params), source_engine, timezone, query_side='source'
        )
        source_data = source_adapter.convert_types(
            source_data, source_metadata, timezone
        )
        if source_data.empty:
            return build_sniff_issue_stats(0, 0, 0), CheckDetails(
                issue_breakdown=pd.DataFrame(),
                issue_examples=pd.DataFrame(),
                dup_source_keys_examples=tuple(),
                dup_target_keys_examples=tuple(),
                source_only_keys_examples=tuple(),
                target_only_keys_examples=tuple(),
                issue_row_examples=pd.DataFrame(),
                evaluated_columns=[],
            )

        self._run_timings.mark_dataset_check_start()
        try:
            return evaluate_sniff_query_data(
                source_data,
                max_examples=max_examples or ct.DEFAULT_MAX_EXAMPLES,
            )
        finally:
            self._run_timings.mark_dataset_check_end()

    def _sniff_query_iterative(
        self,
        source_query: str,
        source_chunks: List[Dict],
        source_engine: Engine,
        source_adapter,
        source_metadata: pd.DataFrame,
        max_examples: Optional[int],
        timezone: str,
    ) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
        examples_limit = max_examples or ct.DEFAULT_MAX_EXAMPLES
        total_rows = 0
        passed_rows = 0
        issue_rows = 0
        status_counter = defaultdict(int)
        issue_example_frames: List[pd.DataFrame] = []
        example_columns: List[str] = []
        has_data = False

        for source_chunk_params in source_chunks:
            chunk_stats, chunk_details = self._execute_source_check_query_chunk(
                source_query=source_query,
                source_params=source_chunk_params,
                source_engine=source_engine,
                source_adapter=source_adapter,
                source_metadata=source_metadata,
                max_examples=examples_limit,
                timezone=timezone,
            )
            if not chunk_stats:
                continue
            has_data = True
            total_rows += chunk_stats.total_source_rows
            passed_rows += chunk_stats.passed_rows
            issue_rows += sniff_issue_row_count(chunk_stats)

            if not chunk_details.issue_breakdown.empty:
                for row in chunk_details.issue_breakdown.itertuples(index=False):
                    status_counter[row.status_value] += int(row.count)

            if chunk_details.evaluated_columns:
                example_columns = chunk_details.evaluated_columns

            if (
                chunk_details.issue_row_examples is not None
                and not chunk_details.issue_row_examples.empty
                and sum(len(frame) for frame in issue_example_frames) < examples_limit
            ):
                issue_example_frames.append(chunk_details.issue_row_examples)

        if not has_data:
            return None, None

        stats = build_sniff_issue_stats(total_rows, passed_rows, issue_rows)
        status_value_counts = (
            pd.DataFrame(
                [
                    {'status_value': value, 'count': count}
                    for value, count in sorted(status_counter.items(), key=str)
                ]
            )
            if status_counter
            else pd.DataFrame(columns=['status_value', 'count'])
        )
        merged_issue_row_examples = (
            pd.concat(issue_example_frames, ignore_index=True).head(examples_limit)
            if issue_example_frames
            else pd.DataFrame()
        )
        details = CheckDetails(
            issue_breakdown=status_value_counts,
            issue_examples=pd.DataFrame(),
            dup_source_keys_examples=tuple(),
            dup_target_keys_examples=tuple(),
            source_only_keys_examples=tuple(),
            target_only_keys_examples=tuple(),
            issue_row_examples=merged_issue_row_examples,
            evaluated_columns=example_columns,
        )
        return stats, details

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
    ) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
        source_data = self._execute_query(
            (source_query, source_params), source_engine, timezone, query_side='source'
        )
        target_data = self._execute_query(
            (target_query, target_params), target_engine, timezone, query_side='target'
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
        if ct.XRECENTLY_CHANGED_COLUMN in common_cols:
            source_data_filtered, target_data_filtered = clean_recently_changed_data(
                source_data_filtered, target_data_filtered, custom_primary_key
            )
        return self._check_dataframes_timed(
            source_data_filtered,
            target_data_filtered,
            custom_primary_key,
            max_examples,
        )

    def _check_query_iterative(
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
    ) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
        examples_limit = max_examples or ct.DEFAULT_MAX_EXAMPLES
        total_source_rows = 0
        total_target_rows = 0
        dup_source_rows = 0
        dup_target_rows = 0
        only_source_rows = 0
        only_target_rows = 0
        comparable_rows = 0
        passed_rows = 0
        issue_counter = defaultdict(int)
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
            comparable_rows += chunk_stats.comparable_rows
            passed_rows += chunk_stats.passed_rows

            if not chunk_details.issue_breakdown.empty:
                for row in chunk_details.issue_breakdown.itertuples(index=False):
                    issue_counter[row.column_name] += int(row.issue_count)

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
                chunk_details.issue_row_examples is not None
                and not chunk_details.issue_row_examples.empty
                and len(discrepant_chunks) < examples_limit
            ):
                needed = examples_limit * 2
                current_cnt = sum(len(x) for x in discrepant_chunks)
                if current_cnt < needed:
                    remain = needed - current_cnt
                    discrepant_chunks.append(
                        chunk_details.issue_row_examples.head(remain)
                    )

            if (
                chunk_details.issue_examples is not None
                and not chunk_details.issue_examples.empty
            ):
                for row in chunk_details.issue_examples.to_dict(
                    'records'
                ):
                    col = row['column_name']
                    if discrepancy_examples_by_col[col] < examples_limit:
                        discrepancy_examples_rows.append(row)
                        discrepancy_examples_by_col[col] += 1

        if not has_data:
            return None, None

        stats = build_check_stats(
            total_source_rows=total_source_rows,
            total_target_rows=total_target_rows,
            dup_source_rows=dup_source_rows,
            dup_target_rows=dup_target_rows,
            only_source_rows=only_source_rows,
            only_target_rows=only_target_rows,
            comparable_rows=comparable_rows,
            passed_rows=passed_rows,
            issue_counts=list(issue_counter.values()),
        )
        issue_breakdown = (
            pd.DataFrame(
                sorted(
                    issue_counter.items(), key=lambda item: item[1], reverse=True
                ),
                columns=['column_name', 'issue_count'],
            )
            if issue_counter
            else pd.DataFrame(columns=['column_name', 'issue_count'])
        )
        details = CheckDetails(
            issue_breakdown=issue_breakdown,
            issue_examples=(
                pd.DataFrame(discrepancy_examples_rows)
                if discrepancy_examples_rows
                else pd.DataFrame()
            ),
            dup_source_keys_examples=tuple(dup_source_examples),
            dup_target_keys_examples=tuple(dup_target_examples),
            source_only_keys_examples=tuple(source_only_examples),
            target_only_keys_examples=tuple(target_only_examples),
            issue_row_examples=(
                pd.concat(discrepant_chunks, ignore_index=True)
                if discrepant_chunks
                else pd.DataFrame()
            ),
            evaluated_columns=[],
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
        query_side: str,
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

        df = self._execute_query(
            (query, params), engine, self.timezone, query_side=query_side
        )

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

    def _check_samples_iterative(
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
        tolerance_pct: float,
        max_examples: Optional[int],
        run_id: str,
        run_started_at: str,
    ) -> Tuple[str, str, Optional[CheckStats], Optional[CheckDetails]]:
        examples_limit = max_examples or ct.DEFAULT_MAX_EXAMPLES

        total_source_rows = 0
        total_target_rows = 0
        dup_source_rows = 0
        dup_target_rows = 0
        only_source_rows = 0
        only_target_rows = 0
        comparable_rows = 0
        passed_rows = 0
        issue_counter = defaultdict(int)

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
                query_side='source',
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
                query_side='target',
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

            chunk_stats, chunk_details = self._check_dataframes_timed(
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
            comparable_rows += chunk_stats.comparable_rows
            passed_rows += chunk_stats.passed_rows

            if not chunk_details.issue_breakdown.empty:
                for row in chunk_details.issue_breakdown.itertuples(index=False):
                    issue_counter[row.column_name] += int(row.issue_count)

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
                chunk_details.issue_row_examples is not None
                and not chunk_details.issue_row_examples.empty
                and len(discrepant_chunks) < examples_limit
            ):
                needed = examples_limit * 2
                current_cnt = sum(len(x) for x in discrepant_chunks)
                if current_cnt < needed:
                    remain = needed - current_cnt
                    discrepant_chunks.append(
                        chunk_details.issue_row_examples.head(remain)
                    )

            if (
                chunk_details.issue_examples is not None
                and not chunk_details.issue_examples.empty
            ):
                for row in chunk_details.issue_examples.to_dict(
                    'records'
                ):
                    col = row['column_name']
                    if discrepancy_examples_by_col[col] < examples_limit:
                        discrepancy_examples_rows.append(row)
                        discrepancy_examples_by_col[col] += 1

        if (total_source_rows, total_target_rows) == (0, 0):
            status = ct.CHECK_SKIPPED
            return status, None, None, None

        stats = build_check_stats(
            total_source_rows=total_source_rows,
            total_target_rows=total_target_rows,
            dup_source_rows=dup_source_rows,
            dup_target_rows=dup_target_rows,
            only_source_rows=only_source_rows,
            only_target_rows=only_target_rows,
            comparable_rows=comparable_rows,
            passed_rows=passed_rows,
            issue_counts=list(issue_counter.values()),
        )

        issue_breakdown = (
            pd.DataFrame(
                sorted(
                    issue_counter.items(),
                    key=lambda item: item[1],
                    reverse=True,
                ),
                columns=['column_name', 'issue_count'],
            )
            if issue_counter
            else pd.DataFrame(columns=['column_name', 'issue_count'])
        )
        issue_examples = (
            pd.DataFrame(discrepancy_examples_rows)
            if discrepancy_examples_rows
            else pd.DataFrame()
        )
        issue_row_examples = (
            pd.concat(discrepant_chunks, ignore_index=True)
            if discrepant_chunks
            else pd.DataFrame()
        )

        details = CheckDetails(
            issue_breakdown=issue_breakdown,
            issue_examples=issue_examples,
            dup_source_keys_examples=tuple(dup_source_examples),
            dup_target_keys_examples=tuple(dup_target_examples),
            source_only_keys_examples=tuple(source_only_examples),
            target_only_keys_examples=tuple(target_only_examples),
            issue_row_examples=issue_row_examples,
            evaluated_columns=common_cols,
            skipped_source_columns=source_only_cols,
            skipped_target_columns=target_only_cols,
        )

        report = generate_check_sample_report(
            source_table.full_name,
            target_table.full_name,
            stats,
            details,
            self.timezone,
            run_id,
            run_started_at,
            source_query,
            source_params,
            target_query,
            target_params,
            date_chunks=date_chunks,
            **self._report_context,
        )
        status = (
            ct.CHECK_FAILED
            if stats.final_diff_score > tolerance_pct
            else ct.CHECK_SUCCESS
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

    def _check_dataframes_timed(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_columns: List[str],
        max_examples: Optional[int],
    ):
        self._run_timings.mark_dataset_check_start()
        try:
            return compare_dataframes(
                source_df, target_df, key_columns, max_examples
            )
        finally:
            self._run_timings.mark_dataset_check_end()

    def _execute_query(
        self,
        query: Union[str, Tuple[str, Dict]],
        engine: Engine,
        timezone: str = None,
        query_side: Optional[str] = None,
    ) -> pd.DataFrame:
        """Execute SQL query using appropriate adapter."""
        if query_side:
            self._run_timings.mark_query_start(query_side)
        try:
            db_type = DBMSType.from_engine(engine)
            adapter = self._get_adapter(db_type)
            df = adapter._execute_query(query, engine, timezone)
            validate_dataframe_size(df, ct.DEFAULT_MAX_SAMPLE_SIZE_GB)
            return df
        finally:
            if query_side:
                self._run_timings.mark_query_end(query_side)

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

    def _require_target_engine(self) -> Engine:
        if self.target_engine is None:
            raise ValueError(
                'target_engine is required for check_sample, check_counts, '
                'and check_query'
            )
        return self.target_engine
