"""Accumulate per-chunk check stats and examples."""

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .stats import (CheckDetails, CheckStats, build_check_stats,
                    build_sniff_issue_stats, sniff_issue_row_count)


def merge_examples_set(target_set: set, source_items, max_examples: int) -> None:
    if not source_items:
        return
    for item in source_items:
        if len(target_set) >= max_examples:
            break
        target_set.add(item)


class PairCheckAccumulator:
    """Merge pairwise (source vs target) chunk stats into one result."""

    def __init__(self, examples_limit: int):
        self.examples_limit = examples_limit
        self.total_source_rows = 0
        self.total_target_rows = 0
        self.dup_source_rows = 0
        self.dup_target_rows = 0
        self.only_source_rows = 0
        self.only_target_rows = 0
        self.comparable_rows = 0
        self.passed_rows = 0
        self.issue_counter = defaultdict(int)
        self.has_data = False
        self.dup_source_examples: set = set()
        self.dup_target_examples: set = set()
        self.source_only_examples: set = set()
        self.target_only_examples: set = set()
        self.discrepant_chunks: List[pd.DataFrame] = []
        self.discrepancy_examples_rows: List[Dict] = []
        self.discrepancy_examples_by_col = defaultdict(int)

    def add(
        self,
        chunk_stats: Optional[CheckStats],
        chunk_details: Optional[CheckDetails],
    ) -> None:
        if not chunk_stats:
            return
        self.has_data = True
        self.total_source_rows += chunk_stats.total_source_rows
        self.total_target_rows += chunk_stats.total_target_rows
        self.dup_source_rows += chunk_stats.dup_source_rows
        self.dup_target_rows += chunk_stats.dup_target_rows
        self.only_source_rows += chunk_stats.only_source_rows
        self.only_target_rows += chunk_stats.only_target_rows
        self.comparable_rows += chunk_stats.comparable_rows
        self.passed_rows += chunk_stats.passed_rows

        if chunk_details is None:
            return

        if not chunk_details.issue_breakdown.empty:
            for row in chunk_details.issue_breakdown.itertuples(index=False):
                self.issue_counter[row.column_name] += int(row.issue_count)

        merge_examples_set(
            self.dup_source_examples,
            chunk_details.dup_source_keys_examples,
            self.examples_limit,
        )
        merge_examples_set(
            self.dup_target_examples,
            chunk_details.dup_target_keys_examples,
            self.examples_limit,
        )
        merge_examples_set(
            self.source_only_examples,
            chunk_details.source_only_keys_examples,
            self.examples_limit,
        )
        merge_examples_set(
            self.target_only_examples,
            chunk_details.target_only_keys_examples,
            self.examples_limit,
        )

        if (
            chunk_details.issue_row_examples is not None
            and not chunk_details.issue_row_examples.empty
            and len(self.discrepant_chunks) < self.examples_limit
        ):
            needed = self.examples_limit * 2
            current_cnt = sum(len(frame) for frame in self.discrepant_chunks)
            if current_cnt < needed:
                remain = needed - current_cnt
                self.discrepant_chunks.append(
                    chunk_details.issue_row_examples.head(remain)
                )

        if (
            chunk_details.issue_examples is not None
            and not chunk_details.issue_examples.empty
        ):
            for row in chunk_details.issue_examples.to_dict('records'):
                col = row['column_name']
                if self.discrepancy_examples_by_col[col] < self.examples_limit:
                    self.discrepancy_examples_rows.append(row)
                    self.discrepancy_examples_by_col[col] += 1

    def build(
        self,
        evaluated_columns: Optional[List[str]] = None,
        skipped_source_columns: Optional[List[str]] = None,
        skipped_target_columns: Optional[List[str]] = None,
    ) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
        if not self.has_data:
            return None, None

        stats = build_check_stats(
            total_source_rows=self.total_source_rows,
            total_target_rows=self.total_target_rows,
            dup_source_rows=self.dup_source_rows,
            dup_target_rows=self.dup_target_rows,
            only_source_rows=self.only_source_rows,
            only_target_rows=self.only_target_rows,
            comparable_rows=self.comparable_rows,
            passed_rows=self.passed_rows,
            issue_counts=list(self.issue_counter.values()),
        )
        issue_breakdown = (
            pd.DataFrame(
                sorted(
                    self.issue_counter.items(),
                    key=lambda item: item[1],
                    reverse=True,
                ),
                columns=['column_name', 'issue_count'],
            )
            if self.issue_counter
            else pd.DataFrame(columns=['column_name', 'issue_count'])
        )
        details = CheckDetails(
            issue_breakdown=issue_breakdown,
            issue_examples=(
                pd.DataFrame(self.discrepancy_examples_rows)
                if self.discrepancy_examples_rows
                else pd.DataFrame()
            ),
            dup_source_keys_examples=tuple(self.dup_source_examples),
            dup_target_keys_examples=tuple(self.dup_target_examples),
            source_only_keys_examples=tuple(self.source_only_examples),
            target_only_keys_examples=tuple(self.target_only_examples),
            issue_row_examples=(
                pd.concat(self.discrepant_chunks, ignore_index=True)
                if self.discrepant_chunks
                else pd.DataFrame()
            ),
            evaluated_columns=evaluated_columns or [],
            skipped_source_columns=skipped_source_columns or [],
            skipped_target_columns=skipped_target_columns or [],
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
