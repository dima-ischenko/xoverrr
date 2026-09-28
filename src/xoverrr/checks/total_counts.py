"""Whole-table COUNT(*) comparison."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

import pandas as pd

from .. import constants as ct
from ..chunking import iter_date_chunks
from ..logger import app_logger
from ..models import DataReference
from ..reporting import generate_total_count_report
from ..stats import CheckStats, status_for_diff_score

if TYPE_CHECKING:
    from ..core import DataQualityChecker


def count_volume_scores(diff_count, equal_count) -> Tuple[float, float]:
    """Return ``(final_diff_score, final_score)`` from count totals."""
    total = float(diff_count) + float(equal_count)
    if not total:
        return 0.0, 100.0
    final_diff_score = 100.0 * float(diff_count) / total
    return final_diff_score, 100.0 - final_diff_score


def build_total_count_stats(source_count: int, target_count: int) -> CheckStats:
    """Build CheckStats for a whole-table COUNT(*) comparison."""
    diff_count = abs(source_count - target_count)
    equal_count = min(source_count, target_count)
    final_diff_score, final_score = count_volume_scores(diff_count, equal_count)
    return CheckStats(
        total_source_rows=source_count,
        total_target_rows=target_count,
        dup_source_rows=0,
        dup_target_rows=0,
        only_source_rows=0,
        only_target_rows=0,
        comparable_rows=0,
        passed_rows=0,
        dup_source_rows_pct=0.0,
        dup_target_rows_pct=0.0,
        source_only_rows_pct=0.0,
        target_only_rows_pct=0.0,
        issue_rows_pct=0.0,
        max_issue_pct=0.0,
        median_issue_pct=0.0,
        final_diff_score=final_diff_score,
        final_score=final_score,
    )


def _first_count_value(df: Optional[pd.DataFrame]) -> int:
    if df is None or df.empty:
        return 0
    return int(df.iloc[0, 0])


def run_total_counts(
    checker: DataQualityChecker,
    source_table: DataReference,
    target_table: DataReference,
    date_column: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    chunk_size_days: Optional[int],
    tolerance_pct: float,
) -> Tuple[str, Optional[str], Optional[CheckStats]]:
    source_adapter = checker._adapter('source')
    target_adapter = checker._adapter('target')

    source_columns_meta = None
    target_columns_meta = None
    if date_column:
        source_columns_meta = checker._get_metadata_cols(
            source_table, checker.source_engine
        )
        target_columns_meta = checker._get_metadata_cols(
            target_table, checker.target_engine
        )

    date_chunks = iter_date_chunks(date_column, start_date, end_date, chunk_size_days)

    source_count = 0
    target_count = 0
    source_query, source_params = None, None
    target_query, target_params = None, None

    for chunk_start, chunk_end in date_chunks:
        source_query, source_params = source_adapter.build_total_count_query(
            source_table,
            date_column,
            chunk_start,
            chunk_end,
            source_columns_meta,
            checker.timezone,
        )
        source_count += _first_count_value(
            checker._run_sql('source', source_query, source_params)
        )

        target_query, target_params = target_adapter.build_total_count_query(
            target_table,
            date_column,
            chunk_start,
            chunk_end,
            target_columns_meta,
            checker.timezone,
        )
        target_count += _first_count_value(
            checker._run_sql('target', target_query, target_params)
        )

    if (source_count, target_count) == (0, 0):
        app_logger.warning('nothing to compare to you')
        return ct.CHECK_SKIPPED, None, None

    stats = build_total_count_stats(source_count, target_count)
    status = status_for_diff_score(stats.final_diff_score, tolerance_pct)
    report = generate_total_count_report(
        source_table.full_name,
        target_table.full_name,
        stats,
        checker.timezone,
        checker._active_run_id,
        checker._active_run_started_at,
        source_query,
        source_params,
        target_query,
        target_params,
        date_chunks=date_chunks,
        **checker._report_context,
    )
    return status, report, stats
