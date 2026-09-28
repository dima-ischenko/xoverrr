"""Grouped-by-date COUNT(*) comparison."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

import pandas as pd

from .. import constants as ct
from ..chunking import iter_date_chunks
from ..compare import cross_fill_missing_dates
from ..logger import app_logger
from ..models import DataReference
from ..reporting import generate_count_report
from ..stats import CheckDetails, CheckStats, status_for_diff_score
from .total_counts import count_volume_scores

if TYPE_CHECKING:
    from ..core import DataQualityChecker


def run_counts_group_by_date(
    checker: DataQualityChecker,
    source_table: DataReference,
    target_table: DataReference,
    date_column: str,
    start_date: Optional[str],
    end_date: Optional[str],
    chunk_size_days: Optional[int],
    tolerance_pct: float,
    max_examples: int,
) -> Tuple[str, Optional[str], Optional[CheckStats], Optional[CheckDetails]]:
    source_adapter = checker._adapter('source')
    target_adapter = checker._adapter('target')

    source_columns_meta = checker._get_metadata_cols(
        source_table, checker.source_engine
    )
    checker._log_columns_meta('source_columns meta', source_columns_meta)

    target_columns_meta = checker._get_metadata_cols(
        target_table, checker.target_engine
    )
    checker._log_columns_meta('target_columns meta', target_columns_meta)

    source_chunks = []
    target_chunks = []
    source_query, source_params = None, None
    target_query, target_params = None, None

    date_chunks = iter_date_chunks(date_column, start_date, end_date, chunk_size_days)

    for chunk_start, chunk_end in date_chunks:
        source_query, source_params = source_adapter.build_count_query_common(
            source_table,
            date_column,
            chunk_start,
            chunk_end,
            source_columns_meta,
            checker.timezone,
        )
        source_chunks.append(checker._run_sql('source', source_query, source_params))

        target_query, target_params = target_adapter.build_count_query_common(
            target_table,
            date_column,
            chunk_start,
            chunk_end,
            target_columns_meta,
            checker.timezone,
        )
        target_chunks.append(checker._run_sql('target', target_query, target_params))

    source_counts = pd.concat(source_chunks, ignore_index=True)
    target_counts = pd.concat(target_chunks, ignore_index=True)
    source_counts = source_counts.groupby('dt', as_index=False)['cnt'].sum()
    target_counts = target_counts.groupby('dt', as_index=False)['cnt'].sum()

    source_counts_filled, target_counts_filled = cross_fill_missing_dates(
        source_counts, target_counts
    )

    merged = source_counts_filled.merge(target_counts_filled, on='dt')
    total_count_source = source_counts_filled['cnt'].sum()
    total_count_target = target_counts_filled['cnt'].sum()

    if (total_count_source, total_count_target) == (0, 0):
        app_logger.warning('nothing to compare to you')
        return ct.CHECK_SKIPPED, None, None, None

    result_diff_in_counters = abs(merged['cnt_x'] - merged['cnt_y']).sum()
    result_equal_in_counters = merged[['cnt_x', 'cnt_y']].min(axis=1).sum()

    stats, details = checker._check_dataframes_timed(
        source_df=source_counts_filled,
        target_df=target_counts_filled,
        key_columns=['dt'],
        max_examples=max_examples,
    )
    stats.final_diff_score, stats.final_score = count_volume_scores(
        result_diff_in_counters, result_equal_in_counters
    )
    status = status_for_diff_score(stats.final_diff_score, tolerance_pct)
    report = generate_count_report(
        source_table.full_name,
        target_table.full_name,
        stats,
        details,
        total_count_source,
        total_count_target,
        result_diff_in_counters,
        result_equal_in_counters,
        checker.timezone,
        checker._active_run_id,
        checker._active_run_started_at,
        source_query,
        source_params,
        target_query,
        target_params,
        **checker._report_context,
    )
    return status, report, stats, details
