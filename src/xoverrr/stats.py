"""Check statistics, details, and input normalisation."""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .constants import CHECK_FAILED, CHECK_SUCCESS


def build_check_stats(
    total_source_rows: int,
    total_target_rows: int,
    dup_source_rows: int,
    dup_target_rows: int,
    only_source_rows: int,
    only_target_rows: int,
    comparable_rows: int,
    passed_rows: int,
    issue_counts: Optional[List[int]] = None,
) -> 'CheckStats':
    issue_counts = issue_counts or []

    if comparable_rows == 0:
        return CheckStats(
            total_source_rows=total_source_rows,
            total_target_rows=total_target_rows,
            dup_source_rows=dup_source_rows,
            dup_target_rows=dup_target_rows,
            only_source_rows=only_source_rows,
            only_target_rows=only_target_rows,
            comparable_rows=0,
            passed_rows=passed_rows,
            dup_source_rows_pct=100,
            dup_target_rows_pct=100,
            source_only_rows_pct=100,
            target_only_rows_pct=100,
            issue_rows_pct=100,
            max_issue_pct=100,
            median_issue_pct=100,
            final_diff_score=100,
            final_score=0,
        )

    dup_source_rows_pct = (dup_source_rows / total_source_rows) * 100
    dup_target_rows_pct = (dup_target_rows / total_target_rows) * 100
    source_only_rows_pct = (only_source_rows / comparable_rows) * 100
    target_only_rows_pct = (only_target_rows / comparable_rows) * 100
    issue_rows_pct = (1 - passed_rows / comparable_rows) * 100

    issue_pcts = [(cnt / comparable_rows) * 100 for cnt in issue_counts]
    max_issue_pct = float(np.max(issue_pcts)) if issue_pcts else 0.0
    median_issue_pct = float(np.median(issue_pcts)) if issue_pcts else 0.0

    final_diff_score = (
        dup_source_rows_pct * 0.1
        + dup_target_rows_pct * 0.1
        + source_only_rows_pct * 0.15
        + target_only_rows_pct * 0.15
        + issue_rows_pct * 0.5
    )

    return CheckStats(
        total_source_rows=total_source_rows,
        total_target_rows=total_target_rows,
        dup_source_rows=dup_source_rows,
        dup_target_rows=dup_target_rows,
        only_source_rows=only_source_rows,
        only_target_rows=only_target_rows,
        comparable_rows=comparable_rows,
        passed_rows=passed_rows,
        dup_source_rows_pct=dup_source_rows_pct,
        dup_target_rows_pct=dup_target_rows_pct,
        source_only_rows_pct=source_only_rows_pct,
        target_only_rows_pct=target_only_rows_pct,
        issue_rows_pct=issue_rows_pct,
        max_issue_pct=max_issue_pct,
        median_issue_pct=median_issue_pct,
        final_diff_score=final_diff_score,
        final_score=100 - final_diff_score,
    )


def normalize_hash_pct(hash_pct: Optional[int]) -> Optional[int]:
    """Return ``None`` when sampling is off, otherwise an integer 1–100."""
    if hash_pct is None or hash_pct == 0:
        return None
    try:
        value = int(hash_pct)
    except (TypeError, ValueError) as exc:
        raise ValueError('hash_pct must be an integer from 1 to 100') from exc
    if value < 1 or value > 100:
        raise ValueError('hash_pct must be an integer from 1 to 100')
    return value


def normalize_column_names(columns: List[str]) -> List[str]:
    """
    Normalize column names to lowercase for a consistent check.

    Parameters:
        columns: List of column names to normalize

    Returns:
        List of lowercased column names
    """
    return [col.lower() for col in columns] if columns else []


@dataclass
class CheckStats:
    """Statistics for a single check."""

    total_source_rows: int
    total_target_rows: int

    dup_source_rows: int
    dup_target_rows: int

    only_source_rows: int
    only_target_rows: int
    comparable_rows: int
    passed_rows: int
    # Percentage metrics
    dup_source_rows_pct: float
    dup_target_rows_pct: float

    source_only_rows_pct: float
    target_only_rows_pct: float
    issue_rows_pct: float
    max_issue_pct: float
    median_issue_pct: float
    final_diff_score: float
    final_score: float


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


@dataclass
class CheckDetails:
    """Examples and per-column details for a single check."""

    issue_breakdown: pd.DataFrame
    issue_examples: pd.DataFrame

    dup_source_keys_examples: tuple
    dup_target_keys_examples: tuple

    source_only_keys_examples: tuple
    target_only_keys_examples: tuple

    issue_row_examples: pd.DataFrame
    evaluated_columns: List[str]
    skipped_source_columns: List[str] = field(default_factory=list)
    skipped_target_columns: List[str] = field(default_factory=list)


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
        final_diff_score=issue_rows_pct,
        final_score=100 - issue_rows_pct,
    )


def sniff_issue_row_count(stats: CheckStats) -> int:
    """Issue (failed) row count for check_sniff_query stats."""
    return max(0, stats.total_source_rows - stats.passed_rows)


def status_for_diff_score(final_diff_score: float, tolerance_pct: float) -> str:
    """Return success/failed from a diff score and tolerance."""
    if final_diff_score > tolerance_pct:
        return CHECK_FAILED
    return CHECK_SUCCESS
