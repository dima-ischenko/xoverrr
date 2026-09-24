"""In-database sample comparison: SQL rendering and tagged-result parsing."""

from typing import Dict, List, Optional, Tuple

import pandas as pd

from .constants import (DEFAULT_MAX_EXAMPLES, FLAG_VALUE_YES,
                        XRECENTLY_CHANGED_COLUMN)
from .models import DataReference
from .utils import CheckDetails, CheckStats, build_check_stats, format_keys

PK_SEPARATOR_CODE = 31
PK_SEPARATOR = chr(PK_SEPARATOR_CODE)
RECORD_SEPARATOR_CODE = 30
RECORD_SEPARATOR = chr(RECORD_SEPARATOR_CODE)
FIELD_SEPARATOR_CODE = 29
FIELD_SEPARATOR = chr(FIELD_SEPARATOR_CODE)

EX_DUP_SRC = 'ex_dup_src'
EX_DUP_TRG = 'ex_dup_trg'
EX_ONLY_SRC = 'ex_only_src'
EX_ONLY_TRG = 'ex_only_trg'
EX_ISSUE_ROW_SRC = 'ex_issue_row_src'
EX_ISSUE_ROW_TRG = 'ex_issue_row_trg'

SECTION_STATS = 'stats'
SECTION_ISSUE_COL = 'issue_col'
SECTION_DUP_SRC = 'dup_src'
SECTION_DUP_TRG = 'dup_trg'
SECTION_ONLY_SRC = 'only_src'
SECTION_ONLY_TRG = 'only_trg'
SECTION_ISSUE_EX = 'issue_ex'
SECTION_ISSUE_ROW = 'issue_row'

STAT_TOTAL_SOURCE = 'total_source_rows'
STAT_TOTAL_TARGET = 'total_target_rows'
STAT_DUP_SOURCE = 'dup_source_rows'
STAT_DUP_TARGET = 'dup_target_rows'
STAT_ONLY_SOURCE = 'only_source_rows'
STAT_ONLY_TARGET = 'only_target_rows'
STAT_COMPARABLE = 'comparable_rows'
STAT_PASSED = 'passed_rows'

_STAT_KEYS = (
    STAT_TOTAL_SOURCE,
    STAT_TOTAL_TARGET,
    STAT_DUP_SOURCE,
    STAT_DUP_TARGET,
    STAT_ONLY_SOURCE,
    STAT_ONLY_TARGET,
    STAT_COMPARABLE,
    STAT_PASSED,
)


def _as_int(value) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0
    if isinstance(value, str) and value.strip() == '':
        return 0
    return int(value)


def _as_text(value) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)


def parse_pk_value(pk_value, key_count: int):
    text = _as_text(pk_value)
    if text is None:
        return None
    if key_count <= 1:
        return (text,)
    return tuple(text.split(PK_SEPARATOR))


def _split_records(value) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if item is not None and str(item) != '']
    text = str(value)
    if text == '':
        return []
    return [part for part in text.split(RECORD_SEPARATOR) if part]


def _pk_display(pk_value, key_count: int):
    parsed = parse_pk_value(pk_value, key_count)
    if parsed is None:
        return None
    if key_count == 1:
        return parsed[0]
    return parsed


def _build_compare_result(
    stats_map: Dict[str, int],
    issue_counts: Dict[str, int],
    dup_source_keys: set,
    dup_target_keys: set,
    only_source_keys: set,
    only_target_keys: set,
    issue_example_rows: List[Dict],
    issue_row_records: List[Dict],
    key_columns: List[str],
    non_key_columns: List[str],
    max_examples: int,
) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
    del key_columns
    total_source = stats_map[STAT_TOTAL_SOURCE]
    total_target = stats_map[STAT_TOTAL_TARGET]
    if total_source == 0 and total_target == 0:
        return None, None

    issue_breakdown = (
        pd.DataFrame(
            sorted(issue_counts.items(), key=lambda item: item[1], reverse=True),
            columns=['column_name', 'issue_count'],
        )
        if issue_counts
        else pd.DataFrame(columns=['column_name', 'issue_count'])
    )
    issue_examples = (
        pd.DataFrame(issue_example_rows) if issue_example_rows else pd.DataFrame()
    )
    issue_row_examples = (
        pd.DataFrame(issue_row_records) if issue_row_records else pd.DataFrame()
    )
    stats = build_check_stats(
        total_source_rows=total_source,
        total_target_rows=total_target,
        dup_source_rows=stats_map[STAT_DUP_SOURCE],
        dup_target_rows=stats_map[STAT_DUP_TARGET],
        only_source_rows=stats_map[STAT_ONLY_SOURCE],
        only_target_rows=stats_map[STAT_ONLY_TARGET],
        comparable_rows=stats_map[STAT_COMPARABLE],
        passed_rows=stats_map[STAT_PASSED],
        issue_counts=list(issue_counts.values()),
    )
    details = CheckDetails(
        issue_breakdown=issue_breakdown,
        issue_examples=issue_examples,
        dup_source_keys_examples=format_keys(dup_source_keys, max_examples),
        dup_target_keys_examples=format_keys(dup_target_keys, max_examples),
        source_only_keys_examples=format_keys(only_source_keys, max_examples),
        target_only_keys_examples=format_keys(only_target_keys, max_examples),
        issue_row_examples=issue_row_examples,
        evaluated_columns=list(non_key_columns),
    )
    return stats, details


def _parse_tagged_compare_result(
    df: pd.DataFrame,
    key_columns: List[str],
    non_key_columns: List[str],
    max_examples: int,
) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
    stats_map = {key: 0 for key in _STAT_KEYS}
    issue_counts: Dict[str, int] = {}
    dup_source_keys = set()
    dup_target_keys = set()
    only_source_keys = set()
    only_target_keys = set()
    issue_example_rows: List[Dict] = []
    issue_row_records: List[Dict] = []
    key_count = len(key_columns)

    for record in df.to_dict('records'):
        lookup = {str(key).lower(): value for key, value in record.items()}
        section = (_as_text(lookup.get('section')) or '').lower()
        name = _as_text(lookup.get('name'))
        pk_value = lookup.get('pk_value')
        source_value = _as_text(lookup.get('source_value'))
        target_value = _as_text(lookup.get('target_value'))
        xflg = _as_text(lookup.get('xflg'))
        cnt = _as_int(lookup.get('cnt'))
        row_payload = _as_text(lookup.get('row_payload'))

        if section == SECTION_STATS and name in stats_map:
            stats_map[name] = cnt
        elif section == SECTION_ISSUE_COL and name:
            if cnt > 0:
                issue_counts[name] = cnt
        elif section == SECTION_DUP_SRC:
            parsed = parse_pk_value(pk_value, key_count)
            if parsed is not None:
                dup_source_keys.add(parsed)
        elif section == SECTION_DUP_TRG:
            parsed = parse_pk_value(pk_value, key_count)
            if parsed is not None:
                dup_target_keys.add(parsed)
        elif section == SECTION_ONLY_SRC:
            parsed = parse_pk_value(pk_value, key_count)
            if parsed is not None:
                only_source_keys.add(parsed)
        elif section == SECTION_ONLY_TRG:
            parsed = parse_pk_value(pk_value, key_count)
            if parsed is not None:
                only_target_keys.add(parsed)
        elif section == SECTION_ISSUE_EX and name:
            pk_display = _pk_display(pk_value, key_count)
            issue_example_rows.append(
                {
                    'primary_key': pk_display,
                    'column_name': name,
                    'source_value': source_value,
                    'target_value': target_value,
                }
            )
        elif section == SECTION_ISSUE_ROW:
            pk_display = _pk_display(pk_value, key_count)
            issue_row_records.append(
                {
                    'primary_key': pk_display,
                    'xflg': xflg,
                    'row_payload': row_payload,
                }
            )

    return _build_compare_result(
        stats_map,
        issue_counts,
        dup_source_keys,
        dup_target_keys,
        only_source_keys,
        only_target_keys,
        issue_example_rows,
        issue_row_records,
        key_columns,
        non_key_columns,
        max_examples,
    )


def _parse_wide_compare_result(
    df: pd.DataFrame,
    key_columns: List[str],
    non_key_columns: List[str],
    max_examples: int,
) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
    lookup = {str(key).lower(): value for key, value in df.iloc[0].items()}
    stats_map = {key: _as_int(lookup.get(key)) for key in _STAT_KEYS}
    issue_counts: Dict[str, int] = {}
    issue_example_rows: List[Dict] = []
    issue_row_records: List[Dict] = []
    key_count = len(key_columns)

    def keys_from(column: str) -> set:
        keys = set()
        for item in _split_records(lookup.get(column)):
            parsed = parse_pk_value(item, key_count)
            if parsed is not None:
                keys.add(parsed)
        return keys

    for col in non_key_columns:
        cnt = _as_int(lookup.get(f'i_{col}'))
        if cnt > 0:
            issue_counts[col] = cnt
        for item in _split_records(lookup.get(f'ex_issue_{col}')):
            parts = item.split(FIELD_SEPARATOR)
            pk_raw = parts[0] if parts else None
            source_value = parts[1] if len(parts) > 1 else None
            target_value = parts[2] if len(parts) > 2 else None
            issue_example_rows.append(
                {
                    'primary_key': _pk_display(pk_raw, key_count),
                    'column_name': col,
                    'source_value': source_value if source_value != '' else None,
                    'target_value': target_value if target_value != '' else None,
                }
            )

    for xflg, column in (('src', EX_ISSUE_ROW_SRC), ('trg', EX_ISSUE_ROW_TRG)):
        for item in _split_records(lookup.get(column)):
            parts = item.split(FIELD_SEPARATOR, 1)
            pk_raw = parts[0] if parts else None
            payload = parts[1] if len(parts) > 1 else None
            issue_row_records.append(
                {
                    'primary_key': _pk_display(pk_raw, key_count),
                    'xflg': xflg,
                    'row_payload': payload,
                }
            )

    return _build_compare_result(
        stats_map,
        issue_counts,
        keys_from(EX_DUP_SRC),
        keys_from(EX_DUP_TRG),
        keys_from(EX_ONLY_SRC),
        keys_from(EX_ONLY_TRG),
        issue_example_rows,
        issue_row_records,
        key_columns,
        non_key_columns,
        max_examples,
    )


def parse_compare_query_result(
    df: pd.DataFrame,
    key_columns: List[str],
    non_key_columns: List[str],
    max_examples: int = DEFAULT_MAX_EXAMPLES,
) -> Tuple[Optional[CheckStats], Optional[CheckDetails]]:
    """Turn a compare-query result into CheckStats / CheckDetails."""
    if df is None or df.empty:
        return None, None
    columns = {str(col).lower() for col in df.columns}
    if 'section' in columns:
        return _parse_tagged_compare_result(
            df, key_columns, non_key_columns, max_examples
        )
    return _parse_wide_compare_result(df, key_columns, non_key_columns, max_examples)


def _strip_select(query: str) -> str:
    return query.strip().rstrip(';')


def render_compare_query(
    adapter,
    source_table: DataReference,
    target_table: DataReference,
    common_columns: List[str],
    key_columns: List[str],
    source_columns_meta: pd.DataFrame,
    target_columns_meta: pd.DataFrame,
    date_column: Optional[str],
    update_column: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    exclude_recent_hours: Optional[int],
    timezone: Optional[str],
    max_examples: int,
) -> Tuple[str, Dict]:
    """Build a classified FULL OUTER JOIN comparison with tagged sample rows."""
    src_query, src_params = adapter.build_data_query_common(
        source_table,
        list(common_columns),
        date_column,
        update_column,
        start_date,
        end_date,
        exclude_recent_hours,
        source_columns_meta,
        timezone,
    )
    trg_query, trg_params = adapter.build_data_query_common(
        target_table,
        list(common_columns),
        date_column,
        update_column,
        start_date,
        end_date,
        exclude_recent_hours,
        target_columns_meta,
        timezone,
    )
    params: Dict = {}
    params.update(src_params or {})
    params.update(trg_params or {})
    params['max_examples'] = int(max_examples)

    q = adapter.quote_ident
    lit = adapter.sql_string_literal
    null_text = adapter.sql_null_text()
    key_cols = list(key_columns)
    value_cols = [
        col
        for col in common_columns
        if col not in key_cols and col != XRECENTLY_CHANGED_COLUMN
    ]
    include_recent = bool(update_column and exclude_recent_hours)
    pk_list = ', '.join(q(col) for col in key_cols)
    pk_order = ', '.join(
        adapter.sql_nulls_first(q(col)) for col in key_cols + value_cols
    )
    marked_select = ', '.join(q(col) for col in key_cols + value_cols)

    def pk_join(left_alias: str, right_alias: str) -> str:
        return ' AND '.join(
            adapter.sql_join_eq(f'{left_alias}.{q(col)}', f'{right_alias}.{q(col)}')
            for col in key_cols
        )

    def pk_concat(alias: str) -> str:
        quoted = [adapter.sql_cast_text(f'{alias}.{q(col)}') for col in key_cols]
        if len(quoted) == 1:
            return quoted[0]
        sep = adapter.sql_chr(PK_SEPARATOR_CODE)
        parts: List[str] = []
        for i, expr in enumerate(quoted):
            if i:
                parts.append(sep)
            parts.append(expr)
        return adapter.sql_concat(parts)

    def payload_expr(side: str) -> str:
        parts = []
        for col in key_cols + value_cols:
            parts.append(lit(f'{col}='))
            parts.append(adapter.sql_cast_text(f'{side}_{col}'))
            parts.append(lit(';'))
        return adapter.sql_concat(parts) if parts else lit('')

    cmp_selects = [
        """CASE
            WHEN src.pk_rn IS NULL THEN 'ONLY_TRG'
            WHEN trg.pk_rn IS NULL THEN 'ONLY_SRC'
            ELSE 'COMMON'
        END AS key_status""",
        f'{pk_concat("src")} AS src_pk',
        f'{pk_concat("trg")} AS trg_pk',
    ]
    for col in key_cols + value_cols:
        cmp_selects.append(f'src.{q(col)} AS src_{col}')
        cmp_selects.append(f'trg.{q(col)} AS trg_{col}')
    cmp_select_sql = ',\n        '.join(cmp_selects)

    diff_selects = []
    same_pred_parts = []
    for col in value_cols:
        flag = adapter.sql_diff_flag(f'src_{col}', f'trg_{col}')
        diff_selects.append(
            f"""CASE
            WHEN key_status = 'COMMON' THEN {flag}
            ELSE NULL
        END AS diff_{col}"""
        )
        same_pred_parts.append(f'diff_{col} = 0')
    compared_extra = ''
    if diff_selects:
        compared_extra = ',\n        ' + ',\n        '.join(diff_selects)

    if same_pred_parts:
        same_pred = ' AND '.join(same_pred_parts)
        row_status_sql = f"""CASE
            WHEN key_status <> 'COMMON' THEN key_status
            WHEN {same_pred} THEN 'SAME'
            ELSE 'DIFF'
        END AS row_status"""
    else:
        row_status_sql = """CASE
            WHEN key_status <> 'COMMON' THEN key_status
            ELSE 'SAME'
        END AS row_status"""

    recent_ctes = ''
    src_base = 'src_raw'
    trg_base = 'trg_raw'
    if include_recent:
        recent_on = pk_join('n', 'r')
        recent_ctes = f"""
    recent_keys AS (
        SELECT DISTINCT {pk_list}, 1 AS x_rk
        FROM (
            SELECT {pk_list}
            FROM src_raw
            WHERE {q(XRECENTLY_CHANGED_COLUMN)} = '{FLAG_VALUE_YES}'
            UNION ALL
            SELECT {pk_list}
            FROM trg_raw
            WHERE {q(XRECENTLY_CHANGED_COLUMN)} = '{FLAG_VALUE_YES}'
        ) recent_u
    ),
    src_filtered AS (
        SELECT n.*
        FROM src_raw n
        LEFT JOIN recent_keys r
            ON {recent_on}
        WHERE r.x_rk IS NULL
    ),
    trg_filtered AS (
        SELECT n.*
        FROM trg_raw n
        LEFT JOIN recent_keys r
            ON {recent_on}
        WHERE r.x_rk IS NULL
    ),"""
        src_base = 'src_filtered'
        trg_base = 'trg_filtered'

    issue_sums = []
    for col in value_cols:
        issue_sums.append(
            f"""COALESCE(
            SUM(CASE WHEN key_status = 'COMMON' THEN diff_{col} ELSE 0 END),
            0
        ) AS i_{col}"""
        )
    issue_sum_sql = ''
    issue_from_r = ''
    if issue_sums:
        issue_sum_sql = ',\n        ' + ',\n        '.join(issue_sums)
        issue_from_r = ''.join(f', r.i_{col}' for col in value_cols)

    def tagged_select(
        section: str,
        name_sql: str,
        pk_sql: str,
        source_sql: str,
        target_sql: str,
        xflg_sql: str,
        cnt_sql: str,
        payload_sql: str,
        from_sql: str,
    ) -> str:
        return f"""SELECT
    {adapter.sql_cast_text(lit(section))} AS section,
    {adapter.sql_cast_text(name_sql)} AS name,
    {adapter.sql_cast_text(pk_sql)} AS pk_value,
    {adapter.sql_cast_text(source_sql)} AS source_value,
    {adapter.sql_cast_text(target_sql)} AS target_value,
    {adapter.sql_cast_text(xflg_sql)} AS xflg,
    {adapter.sql_cast_bigint(cnt_sql)} AS cnt,
    {adapter.sql_cast_text(payload_sql)} AS row_payload
FROM {from_sql}"""

    union_parts = []
    for metric in _STAT_KEYS:
        union_parts.append(
            tagged_select(
                SECTION_STATS,
                lit(metric),
                null_text,
                null_text,
                null_text,
                null_text,
                f'COALESCE(stats.{metric}, 0)',
                null_text,
                'stats',
            )
        )
    for col in value_cols:
        union_parts.append(
            tagged_select(
                SECTION_ISSUE_COL,
                lit(col),
                null_text,
                null_text,
                null_text,
                null_text,
                f'COALESCE(stats.i_{col}, 0)',
                null_text,
                f'stats WHERE COALESCE(stats.i_{col}, 0) > 0',
            )
        )
    union_parts.extend(
        [
            tagged_select(
                SECTION_DUP_SRC,
                null_text,
                'pk_value',
                null_text,
                null_text,
                null_text,
                '1',
                null_text,
                'dup_src_samples WHERE sample_rn <= :max_examples',
            ),
            tagged_select(
                SECTION_DUP_TRG,
                null_text,
                'pk_value',
                null_text,
                null_text,
                null_text,
                '1',
                null_text,
                'dup_trg_samples WHERE sample_rn <= :max_examples',
            ),
            tagged_select(
                SECTION_ONLY_SRC,
                null_text,
                'pk_value',
                null_text,
                null_text,
                null_text,
                '1',
                null_text,
                'only_src_samples WHERE sample_rn <= :max_examples',
            ),
            tagged_select(
                SECTION_ONLY_TRG,
                null_text,
                'pk_value',
                null_text,
                null_text,
                null_text,
                '1',
                null_text,
                'only_trg_samples WHERE sample_rn <= :max_examples',
            ),
        ]
    )
    for col in value_cols:
        union_parts.append(
            tagged_select(
                SECTION_ISSUE_EX,
                lit(col),
                'pk_value',
                'source_value',
                'target_value',
                null_text,
                '1',
                null_text,
                f'diff_{col}_samples WHERE sample_rn <= :max_examples',
            )
        )
    union_parts.append(
        tagged_select(
            SECTION_ISSUE_ROW,
            null_text,
            'pk_value',
            null_text,
            null_text,
            'xflg',
            '1',
            'row_payload',
            'diff_row_samples WHERE sample_rn <= :max_examples',
        )
    )
    union_sql = '\n\nUNION ALL\n\n'.join(union_parts)

    sample_col_ctes = []
    for col in value_cols:
        sample_col_ctes.append(
            f"""
    diff_{col}_samples AS (
        SELECT
            COALESCE(src_pk, trg_pk) AS pk_value,
            src_{col} AS source_value,
            trg_{col} AS target_value,
            ROW_NUMBER() OVER (ORDER BY COALESCE(src_pk, trg_pk)) AS sample_rn
        FROM classified
        WHERE diff_{col} = 1
    )"""
        )
    sample_col_sql = ',' + ','.join(sample_col_ctes) if sample_col_ctes else ''

    query = f"""
WITH src_raw AS (
    {_strip_select(src_query)}
),
trg_raw AS (
    {_strip_select(trg_query)}
),{recent_ctes}
src_marked AS (
    SELECT
        {marked_select},
        COUNT(*) OVER (PARTITION BY {pk_list}) AS pk_cnt,
        ROW_NUMBER() OVER (
            PARTITION BY {pk_list}
            ORDER BY {pk_order}
        ) AS pk_rn
    FROM {src_base}
),
trg_marked AS (
    SELECT
        {marked_select},
        COUNT(*) OVER (PARTITION BY {pk_list}) AS pk_cnt,
        ROW_NUMBER() OVER (
            PARTITION BY {pk_list}
            ORDER BY {pk_order}
        ) AS pk_rn
    FROM {trg_base}
),
src AS (
    SELECT *
    FROM src_marked
    WHERE pk_rn = 1
),
trg AS (
    SELECT *
    FROM trg_marked
    WHERE pk_rn = 1
),
cmp AS (
    SELECT
        {cmp_select_sql}
    FROM src
    FULL OUTER JOIN trg
        ON {pk_join('src', 'trg')}
),
compared AS (
    SELECT
        cmp.*{compared_extra}
    FROM cmp
),
classified AS (
    SELECT
        compared.*,
        {row_status_sql}
    FROM compared
),
src_counts AS (
    SELECT
        COUNT(*) AS {STAT_TOTAL_SOURCE},
        COALESCE(SUM(CASE WHEN pk_rn = 1 THEN 0 ELSE 1 END), 0) AS {STAT_DUP_SOURCE}
    FROM src_marked
),
trg_counts AS (
    SELECT
        COUNT(*) AS {STAT_TOTAL_TARGET},
        COALESCE(SUM(CASE WHEN pk_rn = 1 THEN 0 ELSE 1 END), 0) AS {STAT_DUP_TARGET}
    FROM trg_marked
),
row_stats AS (
    SELECT
        COUNT(CASE WHEN key_status = 'ONLY_SRC' THEN 1 END) AS {STAT_ONLY_SOURCE},
        COUNT(CASE WHEN key_status = 'ONLY_TRG' THEN 1 END) AS {STAT_ONLY_TARGET},
        COUNT(CASE WHEN key_status = 'COMMON' THEN 1 END) AS {STAT_COMPARABLE},
        COUNT(CASE WHEN row_status = 'SAME' THEN 1 END) AS {STAT_PASSED}{issue_sum_sql}
    FROM classified
),
stats AS (
    SELECT
        sc.{STAT_TOTAL_SOURCE},
        tc.{STAT_TOTAL_TARGET},
        sc.{STAT_DUP_SOURCE},
        tc.{STAT_DUP_TARGET},
        r.{STAT_ONLY_SOURCE},
        r.{STAT_ONLY_TARGET},
        r.{STAT_COMPARABLE},
        r.{STAT_PASSED}{issue_from_r}
    FROM src_counts sc
    CROSS JOIN trg_counts tc
    CROSS JOIN row_stats r
),
dup_src_samples AS (
    SELECT
        {pk_concat('d')} AS pk_value,
        ROW_NUMBER() OVER (ORDER BY {pk_concat('d')}) AS sample_rn
    FROM src_marked d
    WHERE pk_cnt > 1
      AND pk_rn = 1
),
dup_trg_samples AS (
    SELECT
        {pk_concat('d')} AS pk_value,
        ROW_NUMBER() OVER (ORDER BY {pk_concat('d')}) AS sample_rn
    FROM trg_marked d
    WHERE pk_cnt > 1
      AND pk_rn = 1
),
only_src_samples AS (
    SELECT
        COALESCE(src_pk, trg_pk) AS pk_value,
        ROW_NUMBER() OVER (ORDER BY COALESCE(src_pk, trg_pk)) AS sample_rn
    FROM classified
    WHERE row_status = 'ONLY_SRC'
),
only_trg_samples AS (
    SELECT
        COALESCE(src_pk, trg_pk) AS pk_value,
        ROW_NUMBER() OVER (ORDER BY COALESCE(src_pk, trg_pk)) AS sample_rn
    FROM classified
    WHERE row_status = 'ONLY_TRG'
),
diff_row_ranked AS (
    SELECT
        COALESCE(src_pk, trg_pk) AS pk_value,
        ROW_NUMBER() OVER (ORDER BY COALESCE(src_pk, trg_pk)) AS pair_rn
    FROM classified
    WHERE row_status = 'DIFF'
),
diff_row_samples AS (
    SELECT
        pk_value,
        xflg,
        row_payload,
        ROW_NUMBER() OVER (ORDER BY pk_value, xflg) AS sample_rn
    FROM (
        SELECT
            r.pk_value,
            {lit('src')} AS xflg,
            {payload_expr('src')} AS row_payload
        FROM diff_row_ranked r
        JOIN classified j
            ON COALESCE(j.src_pk, j.trg_pk) = r.pk_value
        WHERE r.pair_rn <= :max_examples
        UNION ALL
        SELECT
            r.pk_value,
            {lit('trg')} AS xflg,
            {payload_expr('trg')} AS row_payload
        FROM diff_row_ranked r
        JOIN classified j
            ON COALESCE(j.src_pk, j.trg_pk) = r.pk_value
        WHERE r.pair_rn <= :max_examples
    ) diff_row_union
){sample_col_sql}
SELECT *
FROM (
{union_sql}
) compare_result
{adapter.compare_query_settings_suffix()}
"""
    return query, params
