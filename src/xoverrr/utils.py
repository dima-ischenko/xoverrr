from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .constants import (
    DATETIME_FORMAT,
    DEFAULT_MAX_EXAMPLES,
    FLAG_VALUE_NO,
    FLAG_VALUE_YES,
    NULL_REPLACEMENT,
    XSNIFF_PASSED_COLUMN,
    XSNIFF_PASSED_VALUE_NO,
    XRECENTLY_CHANGED_COLUMN,
)
from .logger import app_logger


def format_report_collection(value) -> str:
    """Format optional collections for human-readable report lines."""
    if value is None:
        return ''
    if isinstance(value, (set, frozenset)):
        if not value:
            return ''
        return ', '.join(str(item) for item in sorted(value, key=str))
    if isinstance(value, (tuple, list)):
        if not value:
            return ''
        return ', '.join(str(item) for item in value)
    return str(value)


def append_report_run_header(
    lines: List[str],
    run_id: str,
    run_started_at: str,
    library_version: Optional[str] = None,
    source_db_type: Optional[str] = None,
    target_db_type: Optional[str] = None,
) -> None:
    lines.append('=' * 80)
    lines.append(run_started_at)
    lines.append(f'run_id: {run_id}')
    if library_version is not None:
        lines.append(f'lib version: {library_version}')
    if source_db_type is not None:
        lines.append(f'source db type: {source_db_type}')
    if target_db_type is not None:
        lines.append(f'target db type: {target_db_type}')


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
    max_issue_pct = (
        float(np.max(issue_pcts)) if issue_pcts else 0.0
    )
    median_issue_pct = (
        float(np.median(issue_pcts)) if issue_pcts else 0.0
    )

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
    """Class for storing check statistics"""

    total_source_rows: int
    total_target_rows: int

    dup_source_rows: int
    dup_target_rows: int

    only_source_rows: int
    only_target_rows: int
    comparable_rows: int
    passed_rows: int
    # pct metrics
    dup_source_rows_pct: float
    dup_target_rows_pct: float

    source_only_rows_pct: float
    target_only_rows_pct: float
    issue_rows_pct: float
    #
    max_issue_pct: float
    median_issue_pct: float
    #
    final_diff_score: float
    final_score: float


@dataclass
class CheckDetails:
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


def resolve_check_sniff_query_passed_column(columns: List[str]) -> str:
    """
    Resolve the sniff-query pass/fail flag column.

    Row-level and scalar checks both use ``xsniff_passed``
    (``y`` = passed, ``n`` = failed).
    """
    normalized_columns = normalize_column_names(columns)
    if XSNIFF_PASSED_COLUMN not in normalized_columns:
        raise ValueError(
            f"Sniff query requires '{XSNIFF_PASSED_COLUMN}' column; "
            f"got columns: {', '.join(normalized_columns)}"
        )
    return XSNIFF_PASSED_COLUMN


def evaluate_check_sniff_query_data(
    df: pd.DataFrame,
    max_examples: int = DEFAULT_MAX_EXAMPLES,
) -> Tuple[CheckStats, CheckDetails]:
    """
    Classify rows from a check_sniff_query using ``xsniff_passed``.

    ``y`` means passed, ``n`` means failed.
    """
    prepared_df = prepare_dataframe(df)
    passed_column = resolve_check_sniff_query_passed_column(prepared_df.columns.tolist())

    is_failed = prepared_df[passed_column] == XSNIFF_PASSED_VALUE_NO
    issue_rows = int(is_failed.sum())
    total_rows = len(prepared_df)
    passed_rows = total_rows - issue_rows
    stats = build_sniff_issue_stats(total_rows, passed_rows, issue_rows)

    evaluated_columns = [
        column for column in prepared_df.columns if column != passed_column
    ]
    issue_row_examples = prepared_df.loc[is_failed, evaluated_columns].head(max_examples)
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


def compare_dataframes_meta(
    df1: pd.DataFrame, df2: pd.DataFrame, primary_keys: List[str] = None
) -> List[str]:
    """
    Compare two pandas DataFrames and find common and different columns.

    Parameters:
    -----------
    df1, df2 : pd.DataFrame
        DataFrames to compare
    primary_keys : List[str], optional
        List of primary key columns to exclude from comparison

    Returns:
    --------
    - common_columns: List of common columns (ordered as in df1)
    """
    if primary_keys is None:
        primary_keys = []

    # Get all columns excluding primary keys
    df1_cols = [col for col in df1.columns if col not in primary_keys]
    df2_cols = [col for col in df2.columns if col not in primary_keys]

    # Convert to sets for efficient comparison
    df1_set = set(df1_cols)
    df2_set = set(df2_cols)

    # Find common columns (preserve order from df1)
    common_columns = [col for col in df1_cols if col in df2_set]

    return common_columns


def analyze_column_discrepancies(
    df, primary_key_columns, value_columns, common_keys_cnt, examples_count=3
):

    metrics = {'max_pct': 0.0, 'median_pct': 0.0}
    diff_counters = defaultdict(int)
    diff_examples = {col: [] for col in value_columns}

    rows = list(df.itertuples(index=False))

    pk_indices = [df.columns.get_loc(col) for col in primary_key_columns]

    # scan through pairs
    for i in range(0, len(rows) - 1, 2):
        src_row = rows[i]
        trg_row = rows[i + 1]

        # for compound key use tuple, otherwise just value
        if len(pk_indices) > 1:
            pk_value = tuple(src_row[idx] for idx in pk_indices)
        else:
            idx = pk_indices[0]
            pk_value = src_row[idx]

        for col in value_columns:
            src_val = getattr(src_row, col)
            trg_val = getattr(trg_row, col)
            if src_val != trg_val:
                diff_counters[col] += 1
                if len(diff_examples[col]) < examples_count:
                    diff_examples[col].append(
                        {'pk': pk_value, 'src_val': src_val, 'trg_val': trg_val}
                    )

    # filter out cols without examples
    diff_examples = {k: v for k, v in diff_examples.items() if v}
    if diff_counters:
        values = (np.array(list(diff_counters.values())) / common_keys_cnt) * 100
        max_pct, median_pct = float(values.max()), float(np.median(values))
        metrics['max_pct'] = max_pct
        metrics['median_pct'] = median_pct

    # transform to dataframes
    # 1
    diff_records = []
    for column_name, records in diff_examples.items():
        for record in records:
            transformed_record = {
                'primary_key': record['pk'],
                'column_name': column_name,
                'source_value': record['src_val'],
                'target_value': record['trg_val'],
            }
            diff_records.append(transformed_record)

    df_diff_examples = pd.DataFrame(diff_records)
    # 2
    df_diff_counters = pd.DataFrame(
        list(diff_counters.items()),  # преобразуем в список кортежей
        columns=['column_name', 'issue_count'],  # переименовываем колонки
    )

    return metrics, df_diff_examples, df_diff_counters


def compare_dataframes(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    key_columns: List[str],
    max_examples: int = DEFAULT_MAX_EXAMPLES,
) -> tuple[CheckStats, CheckDetails]:
    """
    Efficient comparison of two dataframes by primary key when discrepancies ratio quite small,
    to analyze the difference in primary keys values and column values

    Looks like it can be simplified and optimized by
    1) outer merge join + indicator metrics(left_only, right_only, both) or/and
    2) by vectors

    Parameters:
        source_df : pd.DataFrame
            Source dataframe
        target_df : pd.DataFrame
            Target dataframe for comparison
        key_columns : List[str]
            List of primary key columns
        max_examples : int, optional
            Maximum number of discrepancy examples per column

    Returns:
    --------
    Dict with
        1) CheckStats object with check statistics
        2) CheckDetails Object with additional details, like the examples and per column diff data
    """
    app_logger.info('start')

    # Input data validation
    if source_df.empty and target_df.empty:
        return None, None
    _validate_input_data(source_df, target_df, key_columns)

    # Check for duplicate primary keys and handle them
    source_dup = source_df[source_df.duplicated(subset=key_columns, keep=False)]
    target_dup = target_df[target_df.duplicated(subset=key_columns, keep=False)]

    source_dup_keys = (
        _create_keys_set(source_dup, key_columns) if not source_dup.empty else set()
    )
    target_dup_keys = (
        _create_keys_set(target_dup, key_columns) if not target_dup.empty else set()
    )

    source_dup_keys_examples = format_keys(source_dup_keys, max_examples)
    target_dup_keys_examples = format_keys(target_dup_keys, max_examples)

    # Remove duplicates from both dataframes for clean comparison
    source_clean = source_df.drop_duplicates(subset=key_columns, keep='first')
    target_clean = target_df.drop_duplicates(subset=key_columns, keep='first')

    # Count duplicates for metrics
    source_dup_cnt = len(source_df) - len(source_clean)
    target_dup_cnt = len(target_df) - len(target_clean)

    non_key_columns = compare_dataframes_meta(source_clean, target_clean, key_columns)

    source_clean = source_clean.assign(xflg='src')
    target_clean = target_clean.assign(xflg='trg')

    xor_combined_df = (
        pd.concat([source_clean, target_clean], ignore_index=True)
        .drop_duplicates(subset=key_columns + non_key_columns, keep=False)
        .assign(
            xcount_pairs=lambda df: df.groupby(key_columns)[key_columns[0]].transform(
                'size'
            )
        )
    )

    # symmetrical difference between two datasets, sorted
    xor_combined_sorted = xor_combined_df.sort_values(
        by=key_columns + ['xflg'], ascending=[False] * len(key_columns) + [True]
    )

    mask = xor_combined_sorted['xcount_pairs'] > 1
    xor_df_multi = xor_combined_sorted[mask]

    mask_source = xor_combined_sorted['xflg'] == 'src'
    mask_target = xor_combined_sorted['xflg'] == 'trg'
    xor_df_source_only = xor_combined_sorted[~mask & mask_source]
    xor_df_target_only = xor_combined_sorted[~mask & mask_target]

    xor_source_only_keys = _create_keys_set(xor_df_source_only, key_columns)
    xor_target_only_keys = _create_keys_set(xor_df_target_only, key_columns)

    xor_common_keys_cnt = int(len(xor_df_multi) / 2) if not xor_df_multi.empty else 0
    xor_source_only_keys_cnt = len(xor_source_only_keys)
    xor_target_only_keys_cnt = len(xor_target_only_keys)

    # take n pairs that is why examples x2
    xor_df_multi_example = (
        xor_df_multi.head(max_examples * 2).drop(columns=['xcount_pairs'])
        if not xor_df_multi.empty
        else pd.DataFrame()
    )

    xor_source_only_keys_examples = format_keys(xor_source_only_keys, max_examples)
    xor_target_only_keys_examples = format_keys(xor_target_only_keys, max_examples)

    # get number of records that present in two datasets based on primary key
    common_keys_cnt = int(
        (
            len(source_clean)
            - xor_source_only_keys_cnt
            + len(target_clean)
            - xor_target_only_keys_cnt
        )
        / 2
    )

    if not common_keys_cnt:
        # Special case when there is no matched primary keys at all
        check_stats = build_check_stats(
            total_source_rows=len(source_df),
            total_target_rows=len(target_df),
            dup_source_rows=source_dup_cnt,
            dup_target_rows=target_dup_cnt,
            only_source_rows=xor_source_only_keys_cnt,
            only_target_rows=xor_target_only_keys_cnt,
            comparable_rows=0,
            passed_rows=0,
            issue_counts=[],
        )

        check_details = CheckDetails(
            issue_breakdown=pd.DataFrame(),
            issue_examples=pd.DataFrame(),
            dup_source_keys_examples=source_dup_keys_examples,
            dup_target_keys_examples=target_dup_keys_examples,
            evaluated_columns=non_key_columns,
            source_only_keys_examples=xor_source_only_keys_examples,
            target_only_keys_examples=xor_target_only_keys_examples,
            issue_row_examples=pd.DataFrame(),
        )
        app_logger.info('end')

        return check_stats, check_details

    # get number of that totally equal in two datasets
    total_matched_records_cnt = common_keys_cnt - xor_common_keys_cnt

    _, diff_col_examples, diff_col_counters = analyze_column_discrepancies(
        xor_df_multi, key_columns, non_key_columns, common_keys_cnt, max_examples
    )

    check_stats = build_check_stats(
        total_source_rows=len(source_df),
        total_target_rows=len(target_df),
        dup_source_rows=source_dup_cnt,
        dup_target_rows=target_dup_cnt,
        only_source_rows=xor_source_only_keys_cnt,
        only_target_rows=xor_target_only_keys_cnt,
        comparable_rows=common_keys_cnt,
        passed_rows=total_matched_records_cnt,
        issue_counts=diff_col_counters['issue_count'].tolist(),
    )

    check_details = CheckDetails(
        issue_breakdown=diff_col_counters,
        issue_examples=diff_col_examples,
        dup_source_keys_examples=source_dup_keys_examples,
        dup_target_keys_examples=target_dup_keys_examples,
        source_only_keys_examples=xor_source_only_keys_examples,
        target_only_keys_examples=xor_target_only_keys_examples,
        issue_row_examples=xor_df_multi_example,
        evaluated_columns=non_key_columns,
    )

    app_logger.info('end')
    return check_stats, check_details


def _validate_input_data(
    source_df: pd.DataFrame, target_df: pd.DataFrame, key_columns: List[str]
) -> None:
    """Input data validation"""
    if not all(col in source_df.columns for col in key_columns):
        missing = [col for col in key_columns if col not in source_df.columns]
        raise ValueError(f'Key columns missing in source: {missing}')

    if not all(col in target_df.columns for col in key_columns):
        missing = [col for col in key_columns if col not in target_df.columns]
        raise ValueError(f'Key columns missing in target: {missing}')


def _create_keys_set(df: pd.DataFrame, key_columns: List[str]) -> set:
    """Creates key set for fast comparison"""
    return set(df[key_columns].itertuples(index=False, name=None))


def _legacy_generate_sample_report(
    source_table: str,
    target_table: str,
    stats: CheckStats,
    details: CheckDetails,
    timezone: str,
    run_id: str,
    run_started_at: str,
    source_query: str = None,
    source_params: Dict = None,
    target_query: str = None,
    target_params: Dict = None,
    date_chunks: Optional[List[Tuple[str, str]]] = None,
    library_version: Optional[str] = None,
    source_db_type: Optional[str] = None,
    target_db_type: Optional[str] = None,
) -> str:
    """Generate check report (logger output looks uuugly)"""
    rl = []
    append_report_run_header(
        rl,
        run_id,
        run_started_at,
        library_version=library_version,
        source_db_type=source_db_type,
        target_db_type=target_db_type,
    )
    rl.append('SAMPLES CHECK REPORT: ')
    if source_table and target_table:
        rl.append(f'{source_table}')
        rl.append('VS')
        rl.append(f'{target_table}')
        rl.append('=' * 80)

    if date_chunks and len(date_chunks) > 1:
        rl.append(f'\nchunks processed ({len(date_chunks)} intervals):')
        for start, end in date_chunks:
            rl.append(f'  {start} → {end}')

    if source_query and target_query:
        rl.append(f'timezone: {timezone}')
        rl.append(f'    {source_query}')
        if source_params:
            rl.append(f'    params: {source_params}')
        rl.append('-' * 40)
        rl.append(f'    {target_query}')
        if target_params:
            rl.append(f'    params: {target_params}')

    rl.append('-' * 40)
    rl.append('\nSUMMARY:')
    rl.append(f'  Source rows: {stats.total_source_rows}')
    rl.append(f'  Target rows: {stats.total_target_rows}')
    rl.append(f'  Duplicated source rows: {stats.dup_source_rows}')
    rl.append(f'  Duplicated target rows: {stats.dup_target_rows}')
    rl.append(f'  Only source rows: {stats.only_source_rows}')
    rl.append(f'  Only target rows: {stats.only_target_rows}')
    rl.append(f'  Comparable rows: {stats.comparable_rows}')
    rl.append(f'  Passed rows: {stats.passed_rows}')
    rl.append('-' * 40)
    rl.append(f'  Source only rows %: {stats.source_only_rows_pct:.5f}')
    rl.append(f'  Target only rows %: {stats.target_only_rows_pct:.5f}')
    rl.append(f'  Duplicated source rows %: {stats.dup_source_rows_pct:.5f}')
    rl.append(f'  Duplicated target rows %: {stats.dup_target_rows_pct:.5f}')
    rl.append(f'  Issue rows %: {stats.issue_rows_pct:.5f}')
    rl.append(f'  Final discrepancies score: {stats.final_diff_score:.5f}')
    rl.append(f'  Final data quality score: {stats.final_score:.5f}')
    rl.append(
        f'  Source-only key examples: {format_report_collection(details.source_only_keys_examples)}'
    )
    rl.append(
        f'  Target-only key examples: {format_report_collection(details.target_only_keys_examples)}'
    )
    rl.append(
        f'  Duplicated source key examples: {format_report_collection(details.dup_source_keys_examples)}'
    )
    rl.append(
        f'  Duplicated target key examples: {format_report_collection(details.dup_target_keys_examples)}'
    )
    rl.append(
        f'  Evaluated columns: {format_report_collection(details.evaluated_columns)}'
    )
    rl.append(
        f'  Skipped source columns: {format_report_collection(details.skipped_source_columns)}'
    )
    rl.append(
        f'  Skipped target columns: {format_report_collection(details.skipped_target_columns)}'
    )

    if stats.max_issue_pct > 0 and not details.issue_breakdown.empty:
        rl.append('\nISSUE BREAKDOWN:')
        rl.append(f'  Max issue %: {stats.max_issue_pct:.5f}')
        rl.append('  Issue counts by column:\n')
        rl.append(details.issue_breakdown.to_string(index=False))
        rl.append('  Issue examples:\n')
        rl.append(
            details.issue_examples.to_string(
                index=False, max_colwidth=64, justify='left'
            )
        )

    # Horizontal wide row dumps are hard to use in text reports.
    # Keep the code for a future optional report parameter (e.g. include_issue_row_examples).
    if False and (
        details.issue_row_examples is not None
        and not details.issue_row_examples.empty
    ):
        rl.append('\nISSUE ROW EXAMPLES:')
        rl.append('Sorted by primary key and dataset:')
        rl.append('')
        rl.append(
            details.issue_row_examples.to_string(
                index=False, max_colwidth=64, justify='left'
            )
        )
        rl.append('')

    rl.append('=' * 80)
    return '\n'.join(rl)


def _legacy_generate_count_report(
    source_table: str,
    target_table: str,
    stats: CheckStats,
    details: CheckDetails,
    total_source_count: int,
    total_target_count: int,
    discrepancies_counters_pct: int,
    result_diff_in_counters: int,
    result_equal_in_counters: int,
    timezone: str,
    run_id: str,
    run_started_at: str,
    source_query: str = None,
    source_params: Dict = None,
    target_query: str = None,
    target_params: Dict = None,
    date_chunks: Optional[List[Tuple[str, str]]] = None,
    library_version: Optional[str] = None,
    source_db_type: Optional[str] = None,
    target_db_type: Optional[str] = None,
) -> None:
    """Generates check report (logger output looks uuugly)"""
    rl = []
    append_report_run_header(
        rl,
        run_id,
        run_started_at,
        library_version=library_version,
        source_db_type=source_db_type,
        target_db_type=target_db_type,
    )
    rl.append(f'COUNTS CHECK REPORT:')
    rl.append(f'{source_table}')
    rl.append(f'VS')
    rl.append(f'{target_table}')
    rl.append('=' * 80)

    if date_chunks and len(date_chunks) > 1:
        rl.append(f'\nchunks processed ({len(date_chunks)} intervals):')
        for start, end in date_chunks:
            rl.append(f'  {start} → {end}') 

    if source_query and target_query:
        rl.append(f'timezone: {timezone}')
        rl.append(f'    {source_query}')
        if source_params:
            rl.append(f'    params: {source_params}')
        rl.append('-' * 40)
        rl.append(f'    {target_query}')
        if target_params:
            rl.append(f'    params: {target_params}')

    rl.append('-' * 40)

    rl.append(f'\nSUMMARY:')
    rl.append(f'  Source total count: {total_source_count}')
    rl.append(f'  Target total count: {total_target_count}')
    rl.append(f'  Common total count: {result_equal_in_counters}')
    rl.append(f'  Diff total count: {result_diff_in_counters}')
    rl.append(f'  Discrepancies %: {discrepancies_counters_pct:.5f}%')
    rl.append(f'  Final discrepancies score: {discrepancies_counters_pct:.5f}')
    rl.append(
        f'  Final data quality score: {(100 - discrepancies_counters_pct):.5f}'
    )
    if not details.issue_breakdown.empty:
        rl.append(f'\nISSUE BREAKDOWN:')
        rl.append(details.issue_breakdown.to_string(index=False))

    # Horizontal wide row dumps are hard to use in text reports.
    # Keep the code for a future optional report parameter (e.g. include_issue_row_examples).
    if False and (
        details.issue_row_examples is not None
        and not details.issue_row_examples.empty
    ):
        rl.append(f'\nISSUE ROW EXAMPLES:')
        rl.append('Sorted by primary key and dataset:')
        rl.append(f'\n')
        rl.append(details.issue_row_examples.to_string(index=False))
        rl.append(f'\n')
    rl.append('=' * 80)

    return '\n'.join(rl)


def safe_remove_zeros(x):
    if pd.isna(x):
        return x
    elif isinstance(x, float) and x.is_integer():
        return int(x)
    return x


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame for comparison by handling nulls and empty strings"""
    df = df.map(safe_remove_zeros)

    df = df.fillna(NULL_REPLACEMENT)
    df = df.replace(r'(?i)^(None|nan|NaN|NaT|\s*)$', NULL_REPLACEMENT, regex=True)

    df = df.astype(str)

    return df


def exclude_by_keys(df, key_columns, exclude_set):
    if len(key_columns) == 1:
        exclude_values = [x[0] for x in exclude_set]
        return df[~df[key_columns[0]].isin(exclude_values)]
    else:
        return df[
            ~df.apply(
                lambda row: tuple(row[col] for col in key_columns) in exclude_set,
                axis=1,
            )
        ]


def clean_recently_changed_data(
    df1: pd.DataFrame, df2: pd.DataFrame, primary_keys: List[str]
):
    """
    Mutually removes rows with recently changed records

    Parameters:
        df1, df2: pandas.DataFrame
        primary_keys: list

    Returns:
        tuple: (df1_processed, df2_processed)
    """
    app_logger.info(
        f'Before exclusion: source={len(df1)}, target={len(df2)}'
    )

    if df1.empty and df2.empty:
        app_logger.info('Both dataframes are empty, skipping exclusion')
        return df1, df2


    has_flag_df1 = XRECENTLY_CHANGED_COLUMN in df1.columns
    has_flag_df2 = XRECENTLY_CHANGED_COLUMN in df2.columns

    if not has_flag_df1 and not has_flag_df2:
        app_logger.info(
            f'{XRECENTLY_CHANGED_COLUMN} column not found in either dataframe'
        )
        return df1, df2

    excluded_keys = set()

    if has_flag_df1 and not df1.empty:
        filtered_df1 = df1[df1[XRECENTLY_CHANGED_COLUMN] == FLAG_VALUE_YES]
        if not filtered_df1.empty:
            excluded_keys.update(_create_keys_set(filtered_df1, primary_keys))

    if has_flag_df2 and not df2.empty:
        filtered_df2 = df2[df2[XRECENTLY_CHANGED_COLUMN] == FLAG_VALUE_YES]
        if not filtered_df2.empty:
            excluded_keys.update(_create_keys_set(filtered_df2, primary_keys))

    if not excluded_keys:
        app_logger.info('No recently changed records to exclude')
        df1_processed = (
            df1.drop(XRECENTLY_CHANGED_COLUMN, axis=1, errors='ignore')
            if has_flag_df1
            else df1.copy()
        )
        df2_processed = (
            df2.drop(XRECENTLY_CHANGED_COLUMN, axis=1, errors='ignore')
            if has_flag_df2
            else df2.copy()
        )
    else:
        df1_processed = exclude_by_keys(df1, primary_keys, excluded_keys)
        df2_processed = exclude_by_keys(df2, primary_keys, excluded_keys)

        if has_flag_df1:
            df1_processed = df1_processed.drop(
                XRECENTLY_CHANGED_COLUMN, axis=1, errors='ignore'
            )
        if has_flag_df2:
            df2_processed = df2_processed.drop(
                XRECENTLY_CHANGED_COLUMN, axis=1, errors='ignore'
            )

    app_logger.info(
        f'After exclusion: source={len(df1_processed)}, target={len(df2_processed)}'
    )

    return df1_processed, df2_processed


def find_count_discrepancies(
    source_counts: pd.DataFrame, target_counts: pd.DataFrame
) -> pd.DataFrame:
    """Find discrepancies in daily row counts between source and target"""
    source_counts['flg'] = 'source'
    target_counts['flg'] = 'target'

    # Find mismatches in counts per date
    all_counts = pd.concat([source_counts, target_counts])
    discrepancies = all_counts.drop_duplicates(
        subset=['dt', 'cnt'], keep=False
    ).sort_values(by=['dt', 'flg'], ascending=[False, True])

    return discrepancies


def create_result_message(
    source_total: int,
    target_total: int,
    discrepancies: pd.DataFrame,
    check_type: str,
) -> str:
    """Create standardized result message"""
    if discrepancies.empty:
        return f'{check_type} match: Source={source_total}, Target={target_total}'

    issue_count = len(discrepancies)
    diff = source_total - target_total
    diff_msg = f' (Δ={diff})' if diff != 0 else ''

    return (
        f'{check_type} mismatch: Source={source_total}, Target={target_total}{diff_msg}, '
        f'{issue_count} discrepancies found'
    )


def filter_columns(
    df: pd.DataFrame, columns: List[str], exclude: Optional[List[str]] = None
) -> pd.DataFrame:
    """Filter DataFrame columns with optional exclusions"""
    if exclude:
        columns = [col for col in columns if col not in exclude]
    return df[columns]


def cross_fill_missing_dates(df1, df2, date_column='dt', value_column='cnt'):
    """
    Fill missing dates between tow dataframes
    """

    df1_indexed = df1.set_index(date_column)
    df2_indexed = df2.set_index(date_column)

    all_dates = df1_indexed.index.union(df2_indexed.index)

    df1_full = df1_indexed.reindex(all_dates, fill_value=0)
    df2_full = df2_indexed.reindex(all_dates, fill_value=0)

    df1_full = df1_full.reset_index()
    df2_full = df2_full.reset_index()

    return df1_full, df2_full


def format_keys(keys, max_examples):
    if keys:
        keys = {next(iter(x)) if len(x) == 1 else x for x in list(keys)[:max_examples]}
        return keys if keys != set() else ()
    return ()


def get_dataframe_size_gb(df: pd.DataFrame) -> float:
    """Calculate DataFrame size in GB"""
    if df.empty:
        return 0.0
    return df.memory_usage(deep=True).sum() / 1024 / 1024 / 1024


def validate_dataframe_size(df: pd.DataFrame, max_size_gb: float) -> None:
    """Validate DataFrame size and raise exception if exceeds limit"""
    if df is None:
        return

    size_gb = get_dataframe_size_gb(df)

    if size_gb > max_size_gb:
        raise ValueError(
            f'DataFrame size {size_gb:.2f} GB exceeds limit of {max_size_gb} GB. '
            f'Shape: {df.shape}'
        )
