"""Row-level sample comparison between two tables or views."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import pandas as pd

from .. import constants as ct
from ..accumulate import PairCheckAccumulator
from ..chunking import iter_date_chunks
from ..compare import clean_recently_changed_data, prepare_dataframe
from ..exceptions import MetadataError
from ..logger import app_logger
from ..models import DataReference, DBMSType, ObjectType
from ..reporting import generate_sample_report
from ..stats import CheckDetails, CheckStats, status_for_diff_score

if TYPE_CHECKING:
    from ..core import DataQualityChecker


def run_samples(
    checker: DataQualityChecker,
    source_table: DataReference,
    target_table: DataReference,
    date_column: Optional[str],
    update_column: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    chunk_size_days: Optional[int],
    exclude_columns: List[str],
    include_columns: List[str],
    custom_key_columns: Optional[List[str]],
    tolerance_pct: float,
    exclude_recent_hours: Optional[int],
    max_examples: Optional[int],
    hash_pct: Optional[int],
) -> Tuple[str, Optional[str], Optional[CheckStats], Optional[CheckDetails]]:
    source_object_type = checker._get_object_type(source_table, checker.source_engine)
    target_object_type = checker._get_object_type(target_table, checker.target_engine)
    app_logger.info(
        f'object type source: {source_object_type} vs target {target_object_type}'
    )

    source_columns_meta = checker._get_metadata_cols(
        source_table, checker.source_engine
    )
    checker._log_columns_meta('source_columns meta', source_columns_meta)

    target_columns_meta = checker._get_metadata_cols(
        target_table, checker.target_engine
    )
    checker._log_columns_meta('target_columns meta', target_columns_meta)

    intersect = list(set(include_columns) & set(exclude_columns))
    if intersect:
        app_logger.warning(
            f'Intersection columns between Include and exclude: {",".join(intersect)}'
        )

    key_columns = _resolve_key_columns(
        checker,
        source_table,
        target_table,
        source_object_type,
        target_object_type,
        source_columns_meta,
        target_columns_meta,
        custom_key_columns,
    )
    source_columns_meta, target_columns_meta = _apply_column_filters(
        source_columns_meta,
        target_columns_meta,
        key_columns,
        include_columns,
        exclude_columns,
    )

    common_cols_df, source_only_cols, target_only_cols = _analyze_columns_meta(
        source_columns_meta, target_columns_meta
    )
    common_cols = common_cols_df['column_name'].tolist()
    if not common_cols:
        raise MetadataError(
            'No one column to compare, need to check tables or reduce the '
            f'exclude_columns list: {",".join(exclude_columns)}'
        )

    return _run_samples_iterative(
        checker,
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
        hash_pct=hash_pct,
    )


def _resolve_key_columns(
    checker: DataQualityChecker,
    source_table: DataReference,
    target_table: DataReference,
    source_object_type,
    target_object_type,
    source_columns_meta: pd.DataFrame,
    target_columns_meta: pd.DataFrame,
    custom_key_columns: Optional[List[str]],
) -> List[str]:
    if custom_key_columns:
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
        return custom_key_columns

    source_pk = (
        checker._get_metadata_pk(source_table, checker.source_engine)
        if source_object_type == ObjectType.TABLE
        else pd.DataFrame({'pk_column_name': []})
    )
    target_pk = (
        checker._get_metadata_pk(target_table, checker.target_engine)
        if target_object_type == ObjectType.TABLE
        else pd.DataFrame({'pk_column_name': []})
    )
    if source_pk['pk_column_name'].tolist() != target_pk['pk_column_name'].tolist():
        app_logger.warning(
            f'Primary keys differ: source={source_pk["pk_column_name"].tolist()}, '
            f'target={target_pk["pk_column_name"].tolist()}'
        )
    key_columns = (
        source_pk['pk_column_name'].tolist() or target_pk['pk_column_name'].tolist()
    )
    if not key_columns:
        raise MetadataError(
            'Primary key not found in the source neither in the target and not provided'
        )
    return key_columns


def _apply_column_filters(
    source_columns_meta: pd.DataFrame,
    target_columns_meta: pd.DataFrame,
    key_columns: List[str],
    include_columns: List[str],
    exclude_columns: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if include_columns:
        if not set(include_columns) & set(key_columns):
            app_logger.warning(
                'The primary key was not included in the column list. '
                'The key column was included in the resulting query automatically. '
                f'PK:{key_columns}'
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
                'The primary key has been excluded from the column list. '
                'However, the key column must be present in the resulting query. '
                f'PK:{key_columns}'
            )
        exclude_columns = list(set(exclude_columns) - set(key_columns))
        source_columns_meta = source_columns_meta[
            ~source_columns_meta['column_name'].isin(exclude_columns)
        ]
        target_columns_meta = target_columns_meta[
            ~target_columns_meta['column_name'].isin(exclude_columns)
        ]

    return source_columns_meta, target_columns_meta


def _analyze_columns_meta(
    source_columns_meta: pd.DataFrame, target_columns_meta: pd.DataFrame
) -> Tuple[pd.DataFrame, list, list]:
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
    return (
        common_columns,
        list(source_set - target_set),
        list(target_set - source_set),
    )


def _fetch_table_data(
    checker: DataQualityChecker,
    engine,
    data_ref: DataReference,
    columns_meta: pd.DataFrame,
    common_columns: List[str],
    date_column: Optional[str],
    update_column: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    exclude_recent_hours: Optional[int],
    query_side: str,
    key_columns: Optional[List[str]] = None,
    hash_pct: Optional[int] = None,
) -> Tuple[pd.DataFrame, str, Dict]:
    adapter = checker._get_adapter(DBMSType.from_engine(engine))
    app_logger.info(DBMSType.from_engine(engine))
    query, params = adapter.build_data_query_common(
        data_ref,
        common_columns,
        date_column,
        update_column,
        start_date,
        end_date,
        exclude_recent_hours,
        columns_meta,
        checker.timezone,
        key_columns,
        hash_pct,
    )
    df = checker._run_converted(query_side, query, params, adapter, columns_meta)
    return df, query, params


def _run_samples_iterative(
    checker: DataQualityChecker,
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
    hash_pct: Optional[int],
) -> Tuple[str, Optional[str], Optional[CheckStats], Optional[CheckDetails]]:
    examples_limit = max_examples or ct.DEFAULT_MAX_EXAMPLES
    acc = PairCheckAccumulator(examples_limit)
    source_query, source_params = None, None
    target_query, target_params = None, None

    date_chunks = iter_date_chunks(date_column, start_date, end_date, chunk_size_days)
    for chunk_start, chunk_end in date_chunks:
        source_data, source_query, source_params = _fetch_table_data(
            checker,
            checker.source_engine,
            source_table,
            source_columns_meta,
            common_cols,
            date_column,
            update_column,
            chunk_start,
            chunk_end,
            exclude_recent_hours,
            query_side='source',
            key_columns=key_columns,
            hash_pct=hash_pct,
        )
        target_data, target_query, target_params = _fetch_table_data(
            checker,
            checker.target_engine,
            target_table,
            target_columns_meta,
            common_cols,
            date_column,
            update_column,
            chunk_start,
            chunk_end,
            exclude_recent_hours,
            query_side='target',
            key_columns=key_columns,
            hash_pct=hash_pct,
        )

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

        chunk_stats, chunk_details = checker._check_dataframes_timed(
            source_data, target_data, key_columns, examples_limit
        )
        acc.add(chunk_stats, chunk_details)

    stats, details = acc.build(
        evaluated_columns=common_cols,
        skipped_source_columns=source_only_cols,
        skipped_target_columns=target_only_cols,
    )
    if not stats:
        return ct.CHECK_SKIPPED, None, None, None

    report = generate_sample_report(
        source_table.full_name,
        target_table.full_name,
        stats,
        details,
        checker.timezone,
        checker._active_run_id,
        checker._active_run_started_at,
        source_query,
        source_params,
        target_query,
        target_params,
        date_chunks=date_chunks,
        primary_key=key_columns,
        hash_pct=hash_pct,
        **checker._report_context,
    )
    status = status_for_diff_score(stats.final_diff_score, tolerance_pct)
    return status, report, stats, details
