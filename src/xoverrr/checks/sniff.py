"""Source-only sniff-query checks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import pandas as pd

from .. import constants as ct
from ..accumulate import SniffCheckAccumulator
from ..chunking import resolve_source_query_chunks
from ..compare import evaluate_check_sniff_query_data
from ..logger import app_logger
from ..reporting import generate_check_sniff_query_report
from ..stats import (CheckDetails, CheckStats, build_sniff_issue_stats,
                     status_for_diff_score)

if TYPE_CHECKING:
    from ..core import DataQualityChecker


def run_sniff_query(
    checker: DataQualityChecker,
    source_query: str,
    source_params: Dict,
    chunk_size_days: Optional[int],
    tolerance_pct: float,
    max_examples: Optional[int],
) -> Tuple[str, Optional[str], Optional[CheckStats], Optional[CheckDetails]]:
    app_logger.info('Getting metadata for sniff query')
    source_metadata = checker._get_metadata_cols_for_custom_query(
        (source_query, source_params), checker.source_engine
    )
    source_adapter = checker._adapter('source')
    source_chunks = resolve_source_query_chunks(source_params, chunk_size_days)

    if len(source_chunks) == 1:
        stats, details = _execute_source_chunk(
            checker,
            source_query=source_query,
            source_params=source_chunks[0],
            source_adapter=source_adapter,
            source_metadata=source_metadata,
            max_examples=max_examples,
        )
    else:
        stats, details = _run_sniff_iterative(
            checker,
            source_query=source_query,
            source_chunks=source_chunks,
            source_adapter=source_adapter,
            source_metadata=source_metadata,
            max_examples=max_examples,
        )

    date_chunks = [
        (chunk.get('start_date'), chunk.get('end_date'))
        for chunk in source_chunks
        if chunk.get('start_date') is not None and chunk.get('end_date') is not None
    ] or None

    if not stats:
        return ct.CHECK_SKIPPED, None, None, None

    status = status_for_diff_score(stats.final_diff_score, tolerance_pct)
    draft_report = generate_check_sniff_query_report(
        stats,
        details,
        checker.timezone,
        checker._active_run_id,
        checker._active_run_started_at,
        source_query,
        source_params,
        date_chunks=date_chunks,
        library_version=checker._report_context['library_version'],
        source_db_type=checker._report_context['source_db_type'],
    )
    return status, draft_report, stats, details


def _execute_source_chunk(
    checker: DataQualityChecker,
    source_query: str,
    source_params: Dict,
    source_adapter,
    source_metadata: pd.DataFrame,
    max_examples: Optional[int],
) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
    source_data = checker._run_converted(
        'source',
        source_query,
        source_params,
        source_adapter,
        source_metadata,
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

    checker._run_timings.mark_dataset_check_start()
    try:
        return evaluate_check_sniff_query_data(
            source_data,
            max_examples=max_examples or ct.DEFAULT_MAX_EXAMPLES,
        )
    finally:
        checker._run_timings.mark_dataset_check_end()


def _run_sniff_iterative(
    checker: DataQualityChecker,
    source_query: str,
    source_chunks: List[Dict],
    source_adapter,
    source_metadata: pd.DataFrame,
    max_examples: Optional[int],
) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
    examples_limit = max_examples or ct.DEFAULT_MAX_EXAMPLES
    acc = SniffCheckAccumulator(examples_limit)
    for source_chunk_params in source_chunks:
        chunk_stats, chunk_details = _execute_source_chunk(
            checker,
            source_query=source_query,
            source_params=source_chunk_params,
            source_adapter=source_adapter,
            source_metadata=source_metadata,
            max_examples=examples_limit,
        )
        acc.add(chunk_stats, chunk_details)
    return acc.build()
