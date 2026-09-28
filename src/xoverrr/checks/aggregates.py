"""MAX / SUM / COUNT(*) comparison of tables and/or SQL queries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import pandas as pd

from .. import constants as ct
from ..compare import prepare_dataframe
from ..logger import app_logger
from ..models import DataReference
from ..reporting import generate_aggregates_report
from ..stats import CheckDetails, CheckStats, quality_scores

if TYPE_CHECKING:
    from ..core import DataQualityChecker

AggregateRelation = Union[DataReference, str]


def resolve_aggregate_relation(
    relation: AggregateRelation,
    params: Optional[Dict],
    side: str,
) -> Tuple[str, Dict, Optional[str]]:
    """Return ``(query, params, table_name)`` for one aggregate side."""
    params = dict(params or {})
    if isinstance(relation, DataReference):
        if params:
            raise ValueError(f'{side}_params is only valid when {side} is a SQL query')
        return f'SELECT * FROM {relation.full_name}', {}, relation.full_name
    if isinstance(relation, str):
        query = relation.strip()
        if not query:
            raise ValueError(f'{side} query is empty')
        return query, params, None
    raise TypeError(
        f'{side} must be a DataReference or a SQL string, got {type(relation).__name__}'
    )


def _aggregate_result_metadata(
    inner_metadata: pd.DataFrame,
    max_columns: List[str],
    sum_columns: List[str],
    include_count: bool,
) -> pd.DataFrame:
    """Reuse inner-query types for ``max_*`` / ``sum_*`` / ``cnt`` columns."""
    type_map = {
        str(row.column_name).lower(): row.data_type
        for row in inner_metadata.itertuples(index=False)
        if getattr(row, 'column_name', None) is not None
    }
    rows = []
    for column in max_columns:
        name = column.lower()
        rows.append(
            {
                'column_name': f'max_{name}',
                'data_type': type_map.get(name, 'text'),
            }
        )
    for column in sum_columns:
        name = column.lower()
        rows.append(
            {
                'column_name': f'sum_{name}',
                'data_type': type_map.get(name, 'numeric'),
            }
        )
    if include_count:
        rows.append({'column_name': 'cnt', 'data_type': 'integer'})
    return pd.DataFrame(rows)


def run_aggregates(
    checker: DataQualityChecker,
    source_query: str,
    target_query: str,
    source_params: Dict,
    target_params: Dict,
    max_columns: List[str],
    sum_columns: List[str],
    include_count: bool,
    hash_columns: List[str],
    hash_pct: Optional[int],
    identity: Dict,
    source_table: Optional[str] = None,
    target_table: Optional[str] = None,
) -> Tuple[str, Optional[str], Optional[CheckStats], Optional[CheckDetails]]:
    source_adapter = checker._adapter('source')
    target_adapter = checker._adapter('target')
    source_data, source_sql, source_params = _execute_aggregate(
        checker,
        source_query,
        source_params,
        source_adapter,
        max_columns,
        sum_columns,
        include_count,
        'source',
        hash_columns=hash_columns,
        hash_pct=hash_pct,
        table=source_table,
    )
    target_data, target_sql, target_params = _execute_aggregate(
        checker,
        target_query,
        target_params,
        target_adapter,
        max_columns,
        sum_columns,
        include_count,
        'target',
        hash_columns=hash_columns,
        hash_pct=hash_pct,
        table=target_table,
    )
    identity['source_query'] = source_sql
    identity['source_params'] = source_params
    identity['target_query'] = target_sql
    identity['target_params'] = target_params
    source_data[ct.XAGG_ROW_COLUMN] = '1'
    target_data[ct.XAGG_ROW_COLUMN] = '1'
    stats, details = checker._check_dataframes_timed(
        source_data,
        target_data,
        [ct.XAGG_ROW_COLUMN],
        ct.DEFAULT_MAX_EXAMPLES,
    )

    if not stats:
        return ct.CHECK_SKIPPED, None, None, None

    matched = stats.final_diff_score <= 0
    stats.final_diff_score, stats.final_score = quality_scores(
        0.0 if matched else 100.0
    )
    status = ct.CHECK_SUCCESS if matched else ct.CHECK_FAILED
    draft_report = generate_aggregates_report(
        stats,
        details,
        checker.timezone,
        checker._active_run_id,
        checker._active_run_started_at,
        source_sql,
        source_params,
        target_sql,
        target_params,
        hash_columns=hash_columns,
        hash_pct=hash_pct,
        source_table=source_table,
        target_table=target_table,
        **checker._report_context,
    )
    return status, draft_report, stats, details


def _execute_aggregate(
    checker: DataQualityChecker,
    query: str,
    params: Dict,
    adapter,
    max_columns: List[str],
    sum_columns: List[str],
    include_count: bool,
    query_side: str,
    hash_columns: Optional[List[str]] = None,
    hash_pct: Optional[int] = None,
    table: Optional[str] = None,
) -> Tuple[pd.DataFrame, str, Dict]:
    engine = checker._side_engine(query_side)
    metadata = checker._get_metadata_cols_for_custom_query((query, params), engine)
    inner = query
    from_table = table
    if hash_pct:
        available = {str(name).lower() for name in metadata.get('column_name', [])}
        missing = [column for column in hash_columns or [] if column not in available]
        if missing:
            raise ValueError(
                f'hash_columns not present in {query_side} query: {missing}'
            )
        inner = adapter.wrap_query_with_hash_sample(
            query, hash_columns, hash_pct, metadata, checker.timezone
        )
        from_table = None
    sql = adapter.build_aggregate_sql(
        inner,
        max_columns=max_columns,
        sum_columns=sum_columns,
        include_count=include_count,
        table=from_table,
    )
    app_logger.info(f'{query_side} aggregate query:\n{sql}')
    frame = checker._run_converted(
        query_side,
        sql,
        params,
        adapter,
        _aggregate_result_metadata(metadata, max_columns, sum_columns, include_count),
    )
    frame.columns = [str(column).lower() for column in frame.columns]
    return prepare_dataframe(frame), sql, params
