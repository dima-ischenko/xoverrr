"""Compatibility re-exports for check stats, comparison, and report helpers."""

from .compare import (analyze_column_discrepancies,
                      clean_recently_changed_data, compare_dataframes,
                      compare_dataframes_meta, cross_fill_missing_dates,
                      evaluate_check_sniff_query_data, exclude_by_keys,
                      format_keys, get_dataframe_size_gb, prepare_dataframe,
                      resolve_check_sniff_query_passed_column,
                      safe_remove_zeros, validate_dataframe_size)
from .reporting import append_report_run_header, format_report_collection
from .stats import (CheckDetails, CheckStats, build_check_stats,
                    build_sniff_issue_stats, build_total_count_stats,
                    count_volume_scores, normalize_column_names,
                    normalize_hash_pct, sniff_issue_row_count,
                    status_for_diff_score)

__all__ = [
    'CheckDetails',
    'CheckStats',
    'analyze_column_discrepancies',
    'append_report_run_header',
    'build_check_stats',
    'build_sniff_issue_stats',
    'build_total_count_stats',
    'clean_recently_changed_data',
    'compare_dataframes',
    'compare_dataframes_meta',
    'count_volume_scores',
    'cross_fill_missing_dates',
    'evaluate_check_sniff_query_data',
    'exclude_by_keys',
    'format_keys',
    'format_report_collection',
    'get_dataframe_size_gb',
    'normalize_column_names',
    'normalize_hash_pct',
    'prepare_dataframe',
    'resolve_check_sniff_query_passed_column',
    'safe_remove_zeros',
    'sniff_issue_row_count',
    'status_for_diff_score',
    'validate_dataframe_size',
]
