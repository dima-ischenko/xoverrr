"""
Report generation module for xoverrr check results.

Provides functions to format check statistics and details into
human-readable reports and structured data formats (JSON, dict).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pandas as pd

from .constants import DATETIME_FORMAT, REPORT_OUTPUT_FORMAT_JSON, REPORT_OUTPUT_FORMATS, REPORT_OUTPUT_FORMAT_TEXT
from .utils import CheckDetails, CheckStats, append_report_run_header, format_report_collection, sniff_issue_row_count

if TYPE_CHECKING:
    from .persistence import CheckRunTimings


@dataclass
class CheckResult:
    """
    Unified container for all check output data.
    
    This class combines status, statistics, details, and metadata
    into a single serializable object suitable for dashboards and APIs.
    """
    timestamp: str
    run_id: str
    check_type: str  # CHECK_TYPE_SAMPLES, CHECK_TYPE_COUNTS, ...
    status: str
    check_name: Optional[str] = None
    check_tags: Optional[Dict[str, Any]] = None
    report: Optional[str] = None
    source_table: Optional[str] = None
    target_table: Optional[str] = None
    timezone: Optional[str] = None
    stats: Optional[CheckStats] = None
    details: Optional[CheckDetails] = None
    source_query: Optional[str] = None
    source_params: Optional[Dict] = None
    target_query: Optional[str] = None
    target_params: Optional[Dict] = None
    timings: Optional[CheckRunTimings] = None

    def __post_init__(self):
        from .persistence import validate_run_id

        object.__setattr__(self, 'run_id', validate_run_id(self.run_id))
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the entire result to a JSON-serializable dictionary.
        
        Returns:
            Dictionary representation suitable for json.dumps()
        """
        def _convert_value(value: Any) -> Any:
            """Recursively convert values to JSON-serializable types"""
            if value is None:
                return None
            elif isinstance(value, pd.DataFrame):
                return value.to_dict(orient='records') if not value.empty else []
            elif isinstance(value, (set, frozenset)):
                return list(value)
            elif isinstance(value, tuple):
                return list(value)
            elif hasattr(value, 'to_dict'):
                return value.to_dict()
            elif hasattr(value, '__dict__'):
                return {
                    k: _convert_value(v) 
                    for k, v in value.__dict__.items() 
                    if not k.startswith('_')
                }
            elif isinstance(value, (datetime, pd.Timestamp)):
                return value.isoformat()
            elif isinstance(value, dict):
                return {k: _convert_value(v) for k, v in value.items()}
            elif isinstance(value, (list, tuple)):
                return [_convert_value(item) for item in value]
            else:
                # Try to convert to native Python type
                try:
                    if hasattr(value, 'item'):  # numpy types
                        return value.item()
                except (AttributeError, ValueError):
                    pass
                return value
        
        result = {
            'timestamp': self.timestamp,
            'check_type': self.check_type,
            'status': self.status,
            'check_name': self.check_name,
            'check_tags': self.check_tags,
            'report': self.report,
            'source_table': self.source_table,
            'target_table': self.target_table,
            'timezone': self.timezone,
        }
        
        if self.stats:
            result['stats'] = _convert_value(self.stats)
        
        if self.details:
            result['details'] = _convert_value(self.details)
        
        # Query info (optional)
        if self.source_query:
            result['source_query'] = self.source_query
        if self.source_params:
            result['source_params'] = self.source_params
        if self.target_query:
            result['target_query'] = self.target_query
        if self.target_params:
            result['target_params'] = self.target_params
            
        return result
    
    def to_json(self, indent: int = 2, ensure_ascii: bool = False) -> str:
        """
        Convert to JSON string.
        
        Args:
            indent: JSON indentation spaces
            ensure_ascii: If False, allows non-ASCII characters
            
        Returns:
            JSON string representation
        """
        import json
        
        return json.dumps(
            self.to_dict(),
            indent=indent,
            ensure_ascii=ensure_ascii,
            default=str
        )


def validate_report_output_format(report_output_format: str) -> None:
    normalized_format = (
        report_output_format or REPORT_OUTPUT_FORMAT_JSON
    ).lower()
    if normalized_format not in REPORT_OUTPUT_FORMATS:
        raise ValueError(
            "report_output_format must be either "
            f"'{REPORT_OUTPUT_FORMAT_JSON}' or "
            f"'{REPORT_OUTPUT_FORMAT_TEXT}'"
        )


def build_check_result(
    *,
    run_id: str,
    timestamp: str,
    timezone: str,
    status: str,
    report: Optional[str],
    stats: Optional[CheckStats],
    details: Optional[CheckDetails],
    check_type: str,
    check_name: Optional[str] = None,
    check_tags: Optional[Dict[str, Any]] = None,
    source_table: Optional[str] = None,
    target_table: Optional[str] = None,
    source_query: Optional[str] = None,
    source_params: Optional[Dict] = None,
    target_query: Optional[str] = None,
    target_params: Optional[Dict] = None,
    timings: Optional[CheckRunTimings] = None,
) -> CheckResult:
    return CheckResult(
        timestamp=timestamp,
        run_id=run_id,
        check_type=check_type,
        status=status,
        check_name=check_name,
        check_tags=check_tags,
        report=report,
        source_table=source_table,
        target_table=target_table,
        timezone=timezone,
        stats=stats,
        details=details,
        source_query=source_query,
        source_params=source_params,
        target_query=target_query,
        target_params=target_params,
        timings=timings,
    )


def format_check_result(
    result: CheckResult,
    report_output_format: str = REPORT_OUTPUT_FORMAT_JSON,
) -> Optional[str]:
    if report_output_format == REPORT_OUTPUT_FORMAT_JSON:
        return result.to_json()
    return result.report


def generate_sample_report(
    source_table: Optional[str],
    target_table: Optional[str],
    stats: CheckStats,
    details: CheckDetails,
    timezone: str,
    run_id: str,
    run_started_at: str,
    source_query: Optional[str] = None,
    source_params: Optional[Dict] = None,
    target_query: Optional[str] = None,
    target_params: Optional[Dict] = None,
    date_chunks: Optional[List[Tuple[str, str]]] = None,
    library_version: Optional[str] = None,
    source_db_type: Optional[str] = None,
    target_db_type: Optional[str] = None,
) -> str:
    """
    Generate a human-readable text report for a sample check.
    
    Args:
        source_table: Source table name (None for custom queries)
        target_table: Target table name (None for custom queries)
        stats: Check statistics
        details: Discrepancy details with examples
        timezone: Timezone used for the check
        source_query: Source SQL query (for custom queries)
        source_params: Source query parameters
        target_query: Target SQL query
        target_params: Target query parameters
        date_chunks: Optional chunk intervals used for the check
        
    Returns:
        Formatted text report
    """
    lines = []
    append_report_run_header(
        lines,
        run_id,
        run_started_at,
        library_version=library_version,
        source_db_type=source_db_type,
        target_db_type=target_db_type,
    )
    if source_table and target_table:
        lines.append('SAMPLES CHECK REPORT:')
        lines.append(f'{source_table}')
        lines.append('VS')
        lines.append(f'{target_table}')
    else:
        lines.append('SAMPLES CHECK REPORT:')
    
    lines.append('=' * 80)

    if date_chunks and len(date_chunks) > 1:
        lines.append(f'\nchunks processed ({len(date_chunks)} intervals):')
        for start, end in date_chunks:
            lines.append(f'  {start} → {end}')

    if source_query and target_query:
        lines.append(f'timezone: {timezone}')
        lines.append(f'    {source_query}')
        if source_params:
            lines.append(f'    params: {source_params}')
        lines.append('-' * 40)
        lines.append(f'    {target_query}')
        if target_params:
            lines.append(f'    params: {target_params}')

    lines.append('-' * 40)

    # Summary statistics
    lines.append('\nSUMMARY:')
    lines.append(f'  Source rows: {stats.total_source_rows}')
    lines.append(f'  Target rows: {stats.total_target_rows}')
    lines.append(f'  Duplicated source rows: {stats.dup_source_rows}')
    lines.append(f'  Duplicated target rows: {stats.dup_target_rows}')
    lines.append(f'  Only source rows: {stats.only_source_rows}')
    lines.append(f'  Only target rows: {stats.only_target_rows}')
    lines.append(f'  Comparable rows: {stats.comparable_rows}')
    lines.append(f'  Passed rows: {stats.passed_rows}')
    lines.append('-' * 40)
    
    # Percentages
    lines.append(f'  Source only rows %: {stats.source_only_rows_pct:.5f}')
    lines.append(f'  Target only rows %: {stats.target_only_rows_pct:.5f}')
    lines.append(f'  Duplicated source rows %: {stats.dup_source_rows_pct:.5f}')
    lines.append(f'  Duplicated target rows %: {stats.dup_target_rows_pct:.5f}')
    lines.append(f'  Issue rows %: {stats.issue_rows_pct:.5f}')
    lines.append(f'  Final discrepancies score: {stats.final_diff_score:.5f}')
    lines.append(f'  Final data quality score: {stats.final_score:.5f}')

    # Key examples
    lines.append(f'  Source-only key examples: {format_report_collection(details.source_only_keys_examples)}')
    lines.append(f'  Target-only key examples: {format_report_collection(details.target_only_keys_examples)}')
    lines.append(f'  Duplicated source key examples: {format_report_collection(details.dup_source_keys_examples)}')
    lines.append(f'  Duplicated target key examples: {format_report_collection(details.dup_target_keys_examples)}')

    lines.append(f'  Evaluated columns: {format_report_collection(details.evaluated_columns)}')
    lines.append(f'  Skipped source columns: {format_report_collection(details.skipped_source_columns)}')
    lines.append(f'  Skipped target columns: {format_report_collection(details.skipped_target_columns)}')

    # Column differences
    if stats.max_issue_pct > 0 and not details.issue_breakdown.empty:
        lines.append('\nISSUE BREAKDOWN:')
        lines.append(f'  Max issue %: {stats.max_issue_pct:.5f}')
        lines.append('  Issue counts by column:\n')
        lines.append(details.issue_breakdown.to_string(index=False))
        lines.append('\n  Issue examples:\n')
        lines.append(
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
        lines.append('\nISSUE ROW EXAMPLES:')
        lines.append('Sorted by primary key and dataset:\n')
        lines.append(
            details.issue_row_examples.to_string(
                index=False, max_colwidth=64, justify='left'
            )
        )
        lines.append('')

    lines.append('=' * 80)

    return '\n'.join(lines)


def generate_check_sniff_query_report(
    stats: CheckStats,
    details: CheckDetails,
    timezone: str,
    run_id: str,
    run_started_at: str,
    source_query: Optional[str] = None,
    source_params: Optional[Dict] = None,
    date_chunks: Optional[List[Tuple[str, str]]] = None,
    library_version: Optional[str] = None,
    source_db_type: Optional[str] = None,
) -> str:
    """Generate a human-readable text report for sniff checks."""
    lines = []
    append_report_run_header(
        lines,
        run_id,
        run_started_at,
        library_version=library_version,
        source_db_type=source_db_type,
    )
    lines.append('SNIFF QUERY CHECK REPORT:')
    lines.append('=' * 80)

    if date_chunks and len(date_chunks) > 1:
        lines.append(f'\nchunks processed ({len(date_chunks)} intervals):')
        for start, end in date_chunks:
            lines.append(f'  {start} → {end}')

    if source_query:
        lines.append(f'timezone: {timezone}')
        lines.append(f'    {source_query}')
        if source_params:
            lines.append(f'    params: {source_params}')

    lines.append('-' * 40)
    lines.append('\nSUMMARY:')
    lines.append(f'  Checked rows: {stats.total_source_rows}')
    lines.append(f'  Passed rows: {stats.passed_rows}')
    lines.append(f'  Issue rows: {sniff_issue_row_count(stats)}')
    lines.append('-' * 40)
    lines.append(f'  Issue rows %: {stats.issue_rows_pct:.5f}')
    lines.append(f'  Final discrepancies score: {stats.final_diff_score:.5f}')
    lines.append(f'  Final data quality score: {stats.final_score:.5f}')

    if not details.issue_breakdown.empty:
        lines.append('\nISSUE BREAKDOWN:')
        lines.append(details.issue_breakdown.to_string(index=False))

    if (
        details.issue_row_examples is not None
        and not details.issue_row_examples.empty
    ):
        lines.append('\nISSUE ROW EXAMPLES:')
        lines.append(
            details.issue_row_examples.to_string(
                index=False, max_colwidth=64, justify='left'
            )
        )
        lines.append('')

    lines.append('=' * 80)
    return '\n'.join(lines)


def generate_count_report(
    source_table: str,
    target_table: str,
    stats: CheckStats,
    details: CheckDetails,
    total_source_count: int,
    total_target_count: int,
    discrepancies_pct: float,
    diff_count: int,
    equal_count: int,
    timezone: str,
    run_id: str,
    run_started_at: str,
    source_query: Optional[str] = None,
    source_params: Optional[Dict] = None,
    target_query: Optional[str] = None,
    target_params: Optional[Dict] = None,
    library_version: Optional[str] = None,
    source_db_type: Optional[str] = None,
    target_db_type: Optional[str] = None,
) -> str:
    """
    Generate a human-readable text report for a count-based check.
    
    Args:
        source_table: Source table name
        target_table: Target table name
        stats: Check statistics
        details: Discrepancy details
        total_source_count: Total rows in source
        total_target_count: Total rows in target
        discrepancies_pct: Overall discrepancy pct
        diff_count: Sum of absolute differences
        equal_count: Sum of common minimum counts
        timezone: Timezone used for the check
        source_query: Source SQL query (optional)
        source_params: Source query parameters
        target_query: Target SQL query
        target_params: Target query parameters
        
    Returns:
        Formatted text report
    """
    lines = []
    append_report_run_header(
        lines,
        run_id,
        run_started_at,
        library_version=library_version,
        source_db_type=source_db_type,
        target_db_type=target_db_type,
    )
    lines.append('COUNTS CHECK REPORT:')
    lines.append(f'{source_table}')
    lines.append('VS')
    lines.append(f'{target_table}')
    lines.append('=' * 80)

    if source_query and target_query:
        lines.append(f'timezone: {timezone}')
        lines.append(f'    {source_query}')
        if source_params:
            lines.append(f'    params: {source_params}')
        lines.append('-' * 40)
        lines.append(f'    {target_query}')
        if target_params:
            lines.append(f'    params: {target_params}')
    
    lines.append('-' * 40)

    lines.append('\nSUMMARY:')
    lines.append(f'  Source total count: {total_source_count}')
    lines.append(f'  Target total count: {total_target_count}')
    lines.append(f'  Common total count: {equal_count}')
    lines.append(f'  Diff total count: {diff_count}')
    lines.append(f'  Discrepancies %: {discrepancies_pct:.5f}%')
    lines.append(f'  Final discrepancies score: {discrepancies_pct:.5f}')
    lines.append(f'  Final data quality score: {(100 - discrepancies_pct):.5f}')

    if not details.issue_breakdown.empty:
        lines.append('\nISSUE BREAKDOWN:')
        lines.append(details.issue_breakdown.to_string(index=False))

    # Horizontal wide row dumps are hard to use in text reports.
    # Keep the code for a future optional report parameter (e.g. include_issue_row_examples).
    if False and (
        details.issue_row_examples is not None
        and not details.issue_row_examples.empty
    ):
        lines.append('\nISSUE ROW EXAMPLES:')
        lines.append('Sorted by primary key and dataset:\n')
        lines.append(details.issue_row_examples.to_string(index=False))
        lines.append('')

    lines.append('=' * 80)

    return '\n'.join(lines)