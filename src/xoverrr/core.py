from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy.engine import Engine

from . import constants as ct
from .adapters.base import BaseDatabaseAdapter
from .adapters.clickhouse import ClickHouseAdapter
from .adapters.oracle import OracleAdapter
from .adapters.postgres import PostgresAdapter
from .checks.counts_group_by_date import run_counts_group_by_date
from .checks.custom_queries import run_custom_queries
from .checks.custom_queries_agg import run_custom_queries_agg
from .checks.samples import run_samples
from .checks.sniff import run_sniff_query
from .checks.total_counts import run_total_counts
from .chunking import unpack_date_range, validate_date_window_args
from .compare import compare_dataframes, validate_dataframe_size
from .logger import app_logger
from .models import DataReference, DBMSType
from .persistence import (CheckResultPersister, CheckRunTimings, build_run_id,
                          normalize_persist_result)
from .reporting import (CheckResult, build_check_result, format_check_result,
                        validate_report_output_format)
from .stats import (CheckDetails, CheckStats, normalize_column_names,
                    normalize_hash_pct)
from .version import __version__


class DataQualityChecker:
    """
    Main checker for intra-source and cross-database data-quality checks.
    """

    def __init__(
        self,
        source_engine: Engine,
        target_engine: Optional[Engine] = None,
        default_exclude_recent_hours: Optional[int] = 24,
        timezone: str = ct.DEFAULT_TZ,
        results_engine: Optional[Engine] = None,
        max_dataframe_size_gb: float = ct.DEFAULT_MAX_DATAFRAME_SIZE_GB,
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
        self.max_dataframe_size_gb = self._normalize_max_dataframe_size_gb(
            max_dataframe_size_gb
        )
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

    @staticmethod
    def _normalize_max_dataframe_size_gb(max_dataframe_size_gb: float) -> float:
        if max_dataframe_size_gb is None or max_dataframe_size_gb <= 0:
            raise ValueError('max_dataframe_size_gb must be greater than 0')
        return float(max_dataframe_size_gb)

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
        """Update the checker run statistics."""
        self.check_stats[status] += 1
        self.check_stats['end_time'] = pd.Timestamp.now().strftime(ct.DATETIME_FORMAT)
        if source_table:
            if status == ct.CHECK_SUCCESS:
                self.check_stats['tables_success'].add(source_table.full_name)
            elif status == ct.CHECK_FAILED:
                self.check_stats['tables_failed'].add(source_table.full_name)
            elif status == ct.CHECK_SKIPPED:
                self.check_stats['tables_skipped'].add(source_table.full_name)

    def check_counts_group_by_date(
        self,
        source_table: DataReference,
        target_table: DataReference,
        date_column: str,
        check_name: Optional[str] = None,
        date_range: Optional[Tuple[Optional[str], Optional[str]]] = None,
        chunk_size_days: Optional[int] = None,
        tolerance_pct: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Optional[DataReference] = None,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
    ) -> CheckResult:
        """
        Compare row counts grouped by a date or timestamp column.

        Returns:
            ``CheckResult`` including ``run_id``, ``status``, ``report``,
            ``stats``, and ``details``.
        """
        self._validate_inputs(source_table, target_table)
        self._require_target_engine()
        validate_date_window_args(date_column, date_range, chunk_size_days)
        start_date, end_date = unpack_date_range(date_range)

        return self._run_check(
            impl=run_counts_group_by_date,
            impl_kwargs=dict(
                source_table=source_table,
                target_table=target_table,
                date_column=date_column,
                start_date=start_date,
                end_date=end_date,
                chunk_size_days=chunk_size_days,
                tolerance_pct=tolerance_pct,
                max_examples=max_examples,
            ),
            check_type=ct.CHECK_TYPE_COUNTS_GROUP_BY_DATE,
            check_name=check_name,
            persist_result=persist_result,
            check_tags=check_tags,
            report_output_format=report_output_format,
            fail_message='Counts check failed',
            stats_table=source_table,
            source_table=source_table.full_name,
            target_table=target_table.full_name,
        )

    def check_total_counts(
        self,
        source_table: DataReference,
        target_table: DataReference,
        check_name: Optional[str] = None,
        date_column: Optional[str] = None,
        date_range: Optional[Tuple[Optional[str], Optional[str]]] = None,
        chunk_size_days: Optional[int] = None,
        tolerance_pct: float = 0.0,
        persist_result: Optional[DataReference] = None,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
    ) -> CheckResult:
        """
        Compare ``COUNT(*)`` between two tables or views.

        Omit ``date_column`` for a whole-table count. Pass ``date_column``
        with ``date_range`` (one or both bounds) and optional
        ``chunk_size_days`` to scan in date windows and sum the chunk
        counts (avoids one heavy query). Chunking requires both bounds.

        Returns:
            ``CheckResult`` including ``run_id``, ``status``, ``report``,
            and ``stats``.
        """
        self._validate_inputs(source_table, target_table)
        self._require_target_engine()
        validate_date_window_args(date_column, date_range, chunk_size_days)
        start_date, end_date = unpack_date_range(date_range)

        return self._run_check(
            impl=run_total_counts,
            impl_kwargs=dict(
                source_table=source_table,
                target_table=target_table,
                date_column=date_column,
                start_date=start_date,
                end_date=end_date,
                chunk_size_days=chunk_size_days,
                tolerance_pct=tolerance_pct,
            ),
            check_type=ct.CHECK_TYPE_TOTAL_COUNTS,
            check_name=check_name,
            persist_result=persist_result,
            check_tags=check_tags,
            report_output_format=report_output_format,
            fail_message='Total counts check failed',
            stats_table=source_table,
            source_table=source_table.full_name,
            target_table=target_table.full_name,
        )

    def check_samples(
        self,
        source_table: DataReference,
        target_table: DataReference,
        check_name: Optional[str] = None,
        date_column: Optional[str] = None,
        update_column: Optional[str] = None,
        date_range: Optional[Tuple[Optional[str], Optional[str]]] = None,
        chunk_size_days: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        custom_primary_key: Optional[List[str]] = None,
        tolerance_pct: float = 0.0,
        exclude_recent_hours: Optional[int] = None,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Optional[DataReference] = None,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
        hash_pct: Optional[int] = None,
    ) -> CheckResult:
        """
        Compare sample rows and column values between two tables or views.

        Parameters:
            source_table: `DataReference`
                Source table to check.
            target_table: `DataReference`
                Target table to check.
            custom_primary_key : `List[str]`
                Primary-key columns for the check.
            exclude_columns : `Optional[List[str]] = None`
                Columns to exclude from the check.
            include_columns : `Optional[List[str]] = None`
                Columns to include in the check (default: all columns).
            tolerance_pct : `float`
                Tolerance percentage for discrepancies (0-100).
            max_examples
                Maximum number of discrepancy examples per column.

        Returns:
            ``CheckResult`` including ``run_id``, ``status``, ``report``,
            ``stats``, and ``details``.
        """
        self._validate_inputs(source_table, target_table)
        self._require_target_engine()
        validate_date_window_args(date_column, date_range, chunk_size_days)

        exclude_hours = exclude_recent_hours or self.default_exclude_recent_hours
        hash_pct = normalize_hash_pct(hash_pct)
        start_date, end_date = unpack_date_range(date_range)
        exclude_cols = normalize_column_names(exclude_columns or [])
        custom_keys = (
            normalize_column_names(custom_primary_key or [])
            if custom_primary_key
            else None
        )
        include_cols = normalize_column_names(include_columns or [])

        return self._run_check(
            impl=run_samples,
            impl_kwargs=dict(
                source_table=source_table,
                target_table=target_table,
                date_column=date_column,
                update_column=update_column,
                start_date=start_date,
                end_date=end_date,
                chunk_size_days=chunk_size_days,
                exclude_columns=exclude_cols,
                include_columns=include_cols,
                custom_key_columns=custom_keys,
                tolerance_pct=tolerance_pct,
                exclude_recent_hours=exclude_hours,
                max_examples=max_examples,
                hash_pct=hash_pct,
            ),
            check_type=ct.CHECK_TYPE_SAMPLES,
            check_name=check_name,
            persist_result=persist_result,
            check_tags=check_tags,
            report_output_format=report_output_format,
            fail_message='Samples check failed',
            stats_table=source_table,
            source_table=source_table.full_name,
            target_table=target_table.full_name,
        )

    def check_sniff_query(
        self,
        source_query: str,
        source_params: Optional[Dict] = None,
        check_name: Optional[str] = None,
        chunk_size_days: Optional[int] = None,
        tolerance_pct: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Optional[DataReference] = None,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
    ) -> CheckResult:
        """
        Sniff out data issues with a source-only SQL check.

        Row-level and scalar pass/fail checks both use ``xsniff_passed``
        (``y`` = passed, ``n`` = failed).

        Returns:
            ``CheckResult`` including ``run_id``, ``status``, ``report``,
            ``stats``, and ``details``.
        """
        source_params = source_params or {}

        return self._run_check(
            impl=run_sniff_query,
            impl_kwargs=dict(
                source_query=source_query,
                source_params=source_params,
                chunk_size_days=chunk_size_days,
                tolerance_pct=tolerance_pct,
                max_examples=max_examples,
            ),
            check_type=ct.CHECK_TYPE_SNIFF_QUERY,
            check_name=check_name,
            persist_result=persist_result,
            check_tags=check_tags,
            report_output_format=report_output_format,
            fail_message='Sniff query failed',
            source_query=source_query,
            source_params=source_params,
        )

    def check_custom_queries(
        self,
        source_query: str,
        target_query: str,
        custom_primary_key: List[str],
        source_params: Optional[Dict] = None,
        target_params: Optional[Dict] = None,
        check_name: Optional[str] = None,
        chunk_size_days: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
        tolerance_pct: float = 0.0,
        max_examples: Optional[int] = ct.DEFAULT_MAX_EXAMPLES,
        persist_result: Optional[DataReference] = None,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
        hash_pct: Optional[int] = None,
    ) -> CheckResult:
        """
        Compare data from custom queries with specified key columns.

        For source-only issue checks, use :meth:`check_sniff_query`.

        Returns:
            ``CheckResult`` including ``run_id``, ``status``, ``report``,
            ``stats``, and ``details``.
        """
        self._require_target_engine()
        source_params = source_params or {}
        target_params = target_params or {}
        exclude_cols = normalize_column_names(exclude_columns or [])
        custom_keys = normalize_column_names(custom_primary_key)
        if not custom_keys:
            raise ValueError('custom_primary_key is mandatory')
        hash_pct = normalize_hash_pct(hash_pct)
        identity = {
            'source_query': source_query,
            'source_params': source_params,
            'target_query': target_query,
            'target_params': target_params,
        }

        return self._run_check(
            impl=run_custom_queries,
            impl_kwargs=dict(
                source_query=source_query,
                target_query=target_query,
                source_params=source_params,
                target_params=target_params,
                custom_keys=custom_keys,
                exclude_cols=exclude_cols,
                chunk_size_days=chunk_size_days,
                tolerance_pct=tolerance_pct,
                max_examples=max_examples,
                hash_pct=hash_pct,
                identity=identity,
            ),
            check_type=ct.CHECK_TYPE_CUSTOM_QUERIES,
            check_name=check_name,
            persist_result=persist_result,
            check_tags=check_tags,
            report_output_format=report_output_format,
            fail_message='Custom queries check failed',
            identity=identity,
        )

    def check_custom_queries_agg(
        self,
        source_query: str,
        target_query: str,
        source_params: Optional[Dict] = None,
        target_params: Optional[Dict] = None,
        max_columns: Optional[List[str]] = None,
        sum_columns: Optional[List[str]] = None,
        include_count: bool = False,
        check_name: Optional[str] = None,
        persist_result: Optional[DataReference] = None,
        check_tags: Optional[Dict] = None,
        report_output_format: str = ct.REPORT_OUTPUT_FORMAT_TEXT,
        hash_columns: Optional[List[str]] = None,
        hash_pct: Optional[int] = None,
    ) -> CheckResult:
        """
        Compare MAX, SUM, and optional COUNT(*) of two custom queries.

        The adapter wraps each query as
        ``SELECT max(col) AS max_col, sum(col) AS sum_col, count(*) AS cnt
        FROM (<query>) x_subq``.

        Date filters belong in the queries, via ``source_params`` and
        ``target_params``. The check succeeds only when every aggregate
        matches (``final_score`` 100); otherwise it fails (``final_score`` 0).

        Returns:
            ``CheckResult`` including ``run_id``, ``status``, ``report``,
            ``stats``, and ``details``.
        """
        self._require_target_engine()
        source_params = source_params or {}
        target_params = target_params or {}
        if not max_columns and not sum_columns and not include_count:
            raise ValueError('max_columns, sum_columns, or include_count is required')
        hash_pct = normalize_hash_pct(hash_pct)
        hash_columns = normalize_column_names(hash_columns or [])
        if hash_pct and not hash_columns:
            raise ValueError('hash_columns is required when hash_pct is set')

        return self._run_check(
            impl=run_custom_queries_agg,
            impl_kwargs=dict(
                source_query=source_query,
                target_query=target_query,
                source_params=source_params,
                target_params=target_params,
                max_columns=max_columns or [],
                sum_columns=sum_columns or [],
                include_count=include_count,
                hash_columns=hash_columns,
                hash_pct=hash_pct,
            ),
            check_type=ct.CHECK_TYPE_CUSTOM_QUERIES_AGG,
            check_name=check_name,
            persist_result=persist_result,
            check_tags=check_tags,
            report_output_format=report_output_format,
            fail_message='Custom query aggregate check failed',
            source_query=source_query,
            source_params=source_params,
            target_query=target_query,
            target_params=target_params,
        )

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

    def _run_check(
        self,
        *,
        impl: Callable,
        impl_kwargs: Optional[Dict] = None,
        check_type: str,
        check_name: Optional[str],
        persist_result: Optional[DataReference],
        check_tags: Optional[Dict],
        report_output_format: str,
        fail_message: str,
        stats_table=None,
        identity: Optional[Dict] = None,
        **identity_fields,
    ) -> CheckResult:
        persist_result = normalize_persist_result(persist_result)
        validate_report_output_format(report_output_format)
        self._start_check_run(check_type, check_name)
        try:
            self.check_stats['checked'] += 1
            outcome = impl(self, **(impl_kwargs or {}))
            if len(outcome) == 3:
                status, report, stats = outcome
                details = None
            else:
                status, report, stats, details = outcome
            fields = dict(identity_fields)
            if identity is not None:
                fields.update(identity)
            result = self._finalize_check(
                status=status,
                report=report,
                stats=stats,
                details=details,
                check_type=check_type,
                persist_result=persist_result,
                report_output_format=report_output_format,
                check_tags=check_tags,
                **fields,
            )
            self._update_stats(result.status, stats_table)
            return result
        except Exception:
            app_logger.exception(fail_message)
            fields = dict(identity_fields)
            if identity is not None:
                fields.update(identity)
            result = self._finalize_check(
                status=ct.CHECK_FAILED,
                report=None,
                stats=None,
                details=None,
                check_type=check_type,
                persist_result=persist_result,
                report_output_format=report_output_format,
                check_tags=check_tags,
                **fields,
            )
            self._update_stats(result.status, stats_table)
            return result

    def _finalize_check(
        self,
        *,
        status: str,
        report: Optional[str],
        stats: Optional[CheckStats],
        details: Optional[CheckDetails],
        check_type: str,
        persist_result: Optional[DataReference] = None,
        report_output_format: str,
        check_tags: Optional[Dict] = None,
        source_table: Optional[str] = None,
        target_table: Optional[str] = None,
        source_query: Optional[str] = None,
        source_params: Optional[Dict] = None,
        target_query: Optional[str] = None,
        target_params: Optional[Dict] = None,
    ) -> CheckResult:
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
        persist_ok = self.result_persister.persist(result, persist_result)
        if not persist_ok:
            status = ct.CHECK_FAILED
            result.status = status
        app_logger.info(
            f'Check run finished: run_id={self._active_run_id} status={status}'
        )
        result.report = format_check_result(result, report_output_format)
        return result

    def _side_engine(self, query_side: str) -> Engine:
        if query_side == 'source':
            return self.source_engine
        if query_side == 'target':
            return self._require_target_engine()
        raise ValueError(f'Unknown query side: {query_side}')

    def _adapter(self, query_side: str) -> BaseDatabaseAdapter:
        db_type = self.source_db_type if query_side == 'source' else self.target_db_type
        return self._get_adapter(db_type)

    def _run_sql(
        self,
        query_side: str,
        query: str,
        params: Optional[Dict] = None,
    ) -> pd.DataFrame:
        """Execute already-built SQL on the source or target engine."""
        return self._execute_query(
            (query, params or {}),
            self._side_engine(query_side),
            self.timezone,
            query_side=query_side,
        )

    def _run_converted(
        self,
        query_side: str,
        query: str,
        params: Dict,
        adapter,
        metadata: pd.DataFrame,
        timezone: Optional[str] = None,
    ) -> pd.DataFrame:
        """Execute SQL and apply adapter type conversion."""
        tz = timezone or self.timezone
        frame = self._run_sql(query_side, query, params)
        return adapter.convert_types(frame, metadata, tz)

    def _log_columns_meta(self, label: str, columns_meta: pd.DataFrame) -> None:
        app_logger.info(f'{label}:\n')
        app_logger.info(columns_meta.to_string(index=False))

    def _check_dataframes_timed(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_columns: List[str],
        max_examples: Optional[int],
    ):
        self._run_timings.mark_dataset_check_start()
        try:
            return compare_dataframes(source_df, target_df, key_columns, max_examples)
        finally:
            self._run_timings.mark_dataset_check_end()

    def _execute_query(
        self,
        query: Union[str, Tuple[str, Dict]],
        engine: Engine,
        timezone: str = None,
        query_side: Optional[str] = None,
    ) -> pd.DataFrame:
        """Execute an SQL query using the appropriate adapter."""
        if query_side:
            self._run_timings.mark_query_start(query_side)
        try:
            db_type = DBMSType.from_engine(engine)
            adapter = self._get_adapter(db_type)
            df = adapter._execute_query(query, engine, timezone)
            validate_dataframe_size(df, self.max_dataframe_size_gb)
            return df
        finally:
            if query_side:
                self._run_timings.mark_query_end(query_side)

    def _get_metadata_cols_for_custom_query(
        self, query, engine: Engine
    ) -> pd.DataFrame:
        adapter = self._get_adapter(DBMSType.from_engine(engine))
        columns_meta = adapter.get_metadata_for_custom_query(query, engine)
        if columns_meta.empty:
            raise ValueError(f'Failed to get metadata for custom query: {query}')
        return columns_meta

    def _get_metadata_cols(
        self, data_ref: DataReference, engine: Engine
    ) -> pd.DataFrame:
        adapter = self._get_adapter(DBMSType.from_engine(engine))
        query, params = adapter.build_metadata_columns_query(data_ref)
        columns_meta = self._execute_query((query, params), engine)
        if columns_meta.empty:
            raise ValueError(f'Failed to get metadata for: {data_ref.full_name}')
        return columns_meta

    def _get_metadata_pk(self, data_ref: DataReference, engine: Engine) -> pd.DataFrame:
        adapter = self._get_adapter(DBMSType.from_engine(engine))
        query, params = adapter.build_primary_key_query(data_ref)
        return self._execute_query((query, params), engine)

    def _get_object_type(self, data_ref: DataReference, engine: Engine):
        adapter = self._get_adapter(DBMSType.from_engine(engine))
        return adapter.get_object_type(data_ref, engine)

    def _get_adapter(self, db_type: DBMSType) -> BaseDatabaseAdapter:
        try:
            return self.adapters[db_type]
        except KeyError:
            raise ValueError(f'No adapter available for {db_type}')

    def _validate_inputs(self, source: DataReference, target: DataReference):
        if not isinstance(source, DataReference):
            raise TypeError('source must be a DataReference')
        if not isinstance(target, DataReference):
            raise TypeError('target must be a DataReference')

    def _require_target_engine(self) -> Engine:
        if self.target_engine is None:
            raise ValueError(
                'target_engine is required for check_samples, check_counts_group_by_date, '
                'check_total_counts, check_custom_queries, and check_custom_queries_agg'
            )
        return self.target_engine
