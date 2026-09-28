"""Source-only sniff-query checks."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import pandas as pd

from .. import constants as ct
from ..chunking import resolve_source_query_chunks
from ..compare import prepare_dataframe
from ..logger import app_logger
from ..reporting import generate_check_sniff_query_report
from ..stats import (CheckDetails, CheckStats, normalize_column_names,
                     quality_scores, status_for_diff_score)

if TYPE_CHECKING:
    from ..core import DataQualityChecker


def build_sniff_issue_stats(
    total_rows: int,
    passed_rows: int,
    issue_rows: int,
) -> CheckStats:
    """Build CheckStats for source-only check_sniff_query checks."""
    if total_rows == 0:
        return CheckStats(
            total_source_rows=0,
            total_target_rows=0,
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
            final_diff_score=0.0,
            final_score=100.0,
        )

    issue_rows_pct = (issue_rows / total_rows) * 100
    final_diff_score, final_score = quality_scores(issue_rows_pct)
    return CheckStats(
        total_source_rows=total_rows,
        total_target_rows=0,
        dup_source_rows=0,
        dup_target_rows=0,
        only_source_rows=0,
        only_target_rows=0,
        comparable_rows=total_rows,
        passed_rows=passed_rows,
        dup_source_rows_pct=0.0,
        dup_target_rows_pct=0.0,
        source_only_rows_pct=0.0,
        target_only_rows_pct=0.0,
        issue_rows_pct=issue_rows_pct,
        max_issue_pct=issue_rows_pct,
        median_issue_pct=issue_rows_pct,
        final_diff_score=final_diff_score,
        final_score=final_score,
    )


def sniff_issue_row_count(stats: CheckStats) -> int:
    """Issue (failed) row count for check_sniff_query stats."""
    return max(0, stats.total_source_rows - stats.passed_rows)


def resolve_check_sniff_query_passed_column(columns: List[str]) -> str:
    """
    Resolve the sniff-query pass/fail flag column.

    Row-level and scalar checks both use ``xsniff_passed``
    (``y`` = passed, ``n`` = failed).
    """
    normalized_columns = normalize_column_names(columns)
    if ct.XSNIFF_PASSED_COLUMN not in normalized_columns:
        raise ValueError(
            f"Sniff query requires '{ct.XSNIFF_PASSED_COLUMN}' column; "
            f'got columns: {", ".join(normalized_columns)}'
        )
    return ct.XSNIFF_PASSED_COLUMN


def evaluate_check_sniff_query_data(
    df: pd.DataFrame,
    max_examples: int = ct.DEFAULT_MAX_EXAMPLES,
) -> Tuple[CheckStats, CheckDetails]:
    """
    Classify rows from a check_sniff_query using ``xsniff_passed``.

    ``y`` means passed, ``n`` means failed.
    """
    prepared_df = prepare_dataframe(df)
    passed_column = resolve_check_sniff_query_passed_column(
        prepared_df.columns.tolist()
    )

    is_failed = prepared_df[passed_column] == ct.XSNIFF_PASSED_VALUE_NO
    issue_rows = int(is_failed.sum())
    total_rows = len(prepared_df)
    passed_rows = total_rows - issue_rows
    stats = build_sniff_issue_stats(total_rows, passed_rows, issue_rows)

    evaluated_columns = [
        column for column in prepared_df.columns if column != passed_column
    ]
    issue_row_examples = prepared_df.loc[is_failed].head(max_examples)
    issue_breakdown = (
        prepared_df[passed_column]
        .value_counts(dropna=False)
        .rename_axis('status_value')
        .reset_index(name='count')
    )

    details = CheckDetails(
        issue_breakdown=issue_breakdown,
        issue_examples=pd.DataFrame(),
        dup_source_keys_examples=tuple(),
        dup_target_keys_examples=tuple(),
        source_only_keys_examples=tuple(),
        target_only_keys_examples=tuple(),
        issue_row_examples=issue_row_examples,
        evaluated_columns=evaluated_columns,
    )
    return stats, details


class SniffCheckAccumulator:
    """Merge source-only sniff-query chunk stats into one result."""

    def __init__(self, examples_limit: int):
        self.examples_limit = examples_limit
        self.total_rows = 0
        self.passed_rows = 0
        self.issue_rows = 0
        self.status_counter = defaultdict(int)
        self.issue_example_frames: List[pd.DataFrame] = []
        self.example_columns: List[str] = []
        self.has_data = False

    def add(
        self,
        chunk_stats: Optional[CheckStats],
        chunk_details: Optional[CheckDetails],
    ) -> None:
        if not chunk_stats:
            return
        self.has_data = True
        self.total_rows += chunk_stats.total_source_rows
        self.passed_rows += chunk_stats.passed_rows
        self.issue_rows += sniff_issue_row_count(chunk_stats)

        if chunk_details is None:
            return

        if not chunk_details.issue_breakdown.empty:
            for row in chunk_details.issue_breakdown.itertuples(index=False):
                self.status_counter[row.status_value] += int(row.count)

        if chunk_details.evaluated_columns:
            self.example_columns = chunk_details.evaluated_columns

        if (
            chunk_details.issue_row_examples is not None
            and not chunk_details.issue_row_examples.empty
            and sum(len(frame) for frame in self.issue_example_frames)
            < self.examples_limit
        ):
            self.issue_example_frames.append(chunk_details.issue_row_examples)

    def build(self) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
        if not self.has_data:
            return None, None

        stats = build_sniff_issue_stats(
            self.total_rows, self.passed_rows, self.issue_rows
        )
        status_value_counts = (
            pd.DataFrame(
                [
                    {'status_value': value, 'count': count}
                    for value, count in sorted(self.status_counter.items(), key=str)
                ]
            )
            if self.status_counter
            else pd.DataFrame(columns=['status_value', 'count'])
        )
        merged_issue_row_examples = (
            pd.concat(self.issue_example_frames, ignore_index=True).head(
                self.examples_limit
            )
            if self.issue_example_frames
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
            evaluated_columns=self.example_columns,
        )
        return stats, details


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
