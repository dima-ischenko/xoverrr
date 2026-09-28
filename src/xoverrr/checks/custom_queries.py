"""Pairwise comparison of two custom SQL queries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import pandas as pd

from .. import constants as ct
from ..accumulate import PairCheckAccumulator
from ..chunking import resolve_custom_query_chunks
from ..compare import clean_recently_changed_data, prepare_dataframe
from ..logger import app_logger
from ..reporting import generate_sample_report
from ..stats import CheckDetails, CheckStats, status_for_diff_score

if TYPE_CHECKING:
    from ..core import DataQualityChecker


def run_custom_queries(
    checker: DataQualityChecker,
    source_query: str,
    target_query: str,
    source_params: Dict,
    target_params: Dict,
    custom_keys: List[str],
    exclude_cols: List[str],
    chunk_size_days: Optional[int],
    tolerance_pct: float,
    max_examples: Optional[int],
    hash_pct: Optional[int],
    identity: Dict,
) -> Tuple[str, Optional[str], Optional[CheckStats], Optional[CheckDetails]]:
    app_logger.info('Getting metadata for source query')
    source_metadata = checker._get_metadata_cols_for_custom_query(
        (source_query, source_params), checker.source_engine
    )
    app_logger.info('Getting metadata for target query')
    target_metadata = checker._get_metadata_cols_for_custom_query(
        (target_query, target_params), checker.target_engine
    )

    source_adapter = checker._adapter('source')
    target_adapter = checker._adapter('target')
    wrapped_source_query = source_query
    wrapped_target_query = target_query
    if hash_pct:
        wrapped_source_query = source_adapter.wrap_query_with_hash_sample(
            source_query,
            custom_keys,
            hash_pct,
            source_metadata,
            checker.timezone,
        )
        wrapped_target_query = target_adapter.wrap_query_with_hash_sample(
            target_query,
            custom_keys,
            hash_pct,
            target_metadata,
            checker.timezone,
        )
        identity['source_query'] = wrapped_source_query
        identity['target_query'] = wrapped_target_query

    date_chunks = resolve_custom_query_chunks(
        source_params, target_params, chunk_size_days
    )
    if len(date_chunks) == 1:
        stats, details = _execute_custom_chunk(
            checker,
            source_query=wrapped_source_query,
            source_params=date_chunks[0][0],
            target_query=wrapped_target_query,
            target_params=date_chunks[0][1],
            source_adapter=source_adapter,
            target_adapter=target_adapter,
            source_metadata=source_metadata,
            target_metadata=target_metadata,
            custom_primary_key=custom_keys,
            exclude_columns=exclude_cols,
            max_examples=max_examples,
        )
    else:
        stats, details = _run_custom_iterative(
            checker,
            source_query=wrapped_source_query,
            target_query=wrapped_target_query,
            chunk_ranges=date_chunks,
            source_adapter=source_adapter,
            target_adapter=target_adapter,
            source_metadata=source_metadata,
            target_metadata=target_metadata,
            custom_primary_key=custom_keys,
            exclude_columns=exclude_cols,
            max_examples=max_examples,
        )

    if not stats:
        return ct.CHECK_SKIPPED, None, None, None

    status = status_for_diff_score(stats.final_diff_score, tolerance_pct)
    draft_report = generate_sample_report(
        None,
        None,
        stats,
        details,
        checker.timezone,
        checker._active_run_id,
        checker._active_run_started_at,
        wrapped_source_query,
        source_params,
        wrapped_target_query,
        target_params,
        date_chunks=date_chunks,
        primary_key=custom_keys,
        hash_pct=hash_pct,
        **checker._report_context,
    )
    return status, draft_report, stats, details


def _execute_custom_chunk(
    checker: DataQualityChecker,
    source_query: str,
    source_params: Dict,
    target_query: str,
    target_params: Dict,
    source_adapter,
    target_adapter,
    source_metadata: pd.DataFrame,
    target_metadata: pd.DataFrame,
    custom_primary_key: List[str],
    exclude_columns: Optional[List[str]],
    max_examples: Optional[int],
) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
    source_data = checker._run_converted(
        'source', source_query, source_params, source_adapter, source_metadata
    )
    target_data = checker._run_converted(
        'target', target_query, target_params, target_adapter, target_metadata
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
    return checker._check_dataframes_timed(
        source_data_filtered,
        target_data_filtered,
        custom_primary_key,
        max_examples,
    )


def _run_custom_iterative(
    checker: DataQualityChecker,
    source_query: str,
    target_query: str,
    chunk_ranges: List[Tuple[Dict, Dict]],
    source_adapter,
    target_adapter,
    source_metadata: pd.DataFrame,
    target_metadata: pd.DataFrame,
    custom_primary_key: List[str],
    exclude_columns: Optional[List[str]],
    max_examples: Optional[int],
) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
    examples_limit = max_examples or ct.DEFAULT_MAX_EXAMPLES
    acc = PairCheckAccumulator(examples_limit)
    for source_chunk_params, target_chunk_params in chunk_ranges:
        chunk_stats, chunk_details = _execute_custom_chunk(
            checker,
            source_query=source_query,
            source_params=source_chunk_params,
            target_query=target_query,
            target_params=target_chunk_params,
            source_adapter=source_adapter,
            target_adapter=target_adapter,
            source_metadata=source_metadata,
            target_metadata=target_metadata,
            custom_primary_key=custom_primary_key,
            exclude_columns=exclude_columns,
            max_examples=examples_limit,
        )
        acc.add(chunk_stats, chunk_details)
    return acc.build()
