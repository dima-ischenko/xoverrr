import json
import dataclasses
import uuid
from dataclasses import dataclass
from typing import Dict, Literal, Optional, Union

import pandas as pd
from sqlalchemy.engine import Engine
from .adapters.clickhouse import ClickHouseAdapter
from .adapters.oracle import OracleAdapter
from .adapters.postgres import PostgresAdapter
from .constants import DATETIME_FORMAT
from .logger import app_logger
from .models import DBMSType, DataReference
from .reporting import ComparisonResult
from .constants import STATS_REPORT_FLOAT_DECIMALS
from .utils import ComparisonDiffDetails, ComparisonStats

PERSIST_PRIMARY_KEY = 'run_id'
RUN_ID_LENGTH = 16
QuerySide = Literal['source', 'target']

TIMING_PERSIST_FIELDS = (
    'run_started_at',
    'run_finished_at',
    'source_query_started_at',
    'source_query_finished_at',
    'target_query_started_at',
    'target_query_finished_at',
    'dataset_compare_started_at',
    'dataset_compare_finished_at',
)


def _round_stats_float_for_persist(value) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), STATS_REPORT_FLOAT_DECIMALS)


def validate_run_id(run_id: Optional[str]) -> str:
    """Ensure run_id is present and normalized for persistence/logging."""
    if run_id is None:
        raise ValueError('run_id must be a non-empty string')
    normalized = str(run_id).strip()
    if not normalized:
        raise ValueError('run_id must be a non-empty string')
    return normalized


def build_run_id() -> str:
    """Build a random non-empty run identifier for a comparison run."""
    return uuid.uuid4().hex[:RUN_ID_LENGTH]


@dataclass
class ComparisonRunTimings:
    """Wall-clock timestamps for a single comparison run (DATETIME_FORMAT strings)."""

    run_started_at: Optional[str] = None
    run_finished_at: Optional[str] = None
    source_query_started_at: Optional[str] = None
    source_query_finished_at: Optional[str] = None
    target_query_started_at: Optional[str] = None
    target_query_finished_at: Optional[str] = None
    dataset_compare_started_at: Optional[str] = None
    dataset_compare_finished_at: Optional[str] = None

    @staticmethod
    def now() -> str:
        return pd.Timestamp.now().strftime(DATETIME_FORMAT)

    def mark_query_start(self, side: QuerySide) -> None:
        started_attr = f'{side}_query_started_at'
        if getattr(self, started_attr) is None:
            setattr(self, started_attr, self.now())

    def mark_query_end(self, side: QuerySide) -> None:
        setattr(self, f'{side}_query_finished_at', self.now())

    def mark_dataset_compare_start(self) -> None:
        if self.dataset_compare_started_at is None:
            self.dataset_compare_started_at = self.now()

    def mark_dataset_compare_end(self) -> None:
        self.dataset_compare_finished_at = self.now()

    def finish_run(self) -> None:
        self.run_finished_at = self.now()


# Portable logical column types mapped to DB-specific DDL in adapter PERSIST_TYPE_MAP.
PERSIST_COL_SHORT_STRING = 'short_string'
PERSIST_COL_STRING = 'string'
PERSIST_COL_NAME = 'name'
PERSIST_COL_TABLE_REF = 'table_ref'
PERSIST_COL_TZ_NAME = 'tz_name'
PERSIST_COL_DATETIME = 'datetime'
PERSIST_COL_TEXT = 'text'
PERSIST_COL_INT = 'int'
PERSIST_COL_FLOAT = 'float'

# Persisted column name (avoids reserved TIMEZONE keyword in Oracle).
PERSIST_TIMEZONE_COLUMN = 'comparison_timezone'

BASE_PERSIST_COLUMN_TYPES = {
    'run_id': PERSIST_COL_SHORT_STRING,
    **{field: PERSIST_COL_DATETIME for field in TIMING_PERSIST_FIELDS},
    'comparison_type': PERSIST_COL_STRING,
    'status': PERSIST_COL_STRING,
    'comparison_name': PERSIST_COL_NAME,
    'comparison_tags_json': PERSIST_COL_TEXT,
    'source_table': PERSIST_COL_TABLE_REF,
    'target_table': PERSIST_COL_TABLE_REF,
    PERSIST_TIMEZONE_COLUMN: PERSIST_COL_TZ_NAME,
    'source_query': PERSIST_COL_TEXT,
    'target_query': PERSIST_COL_TEXT,
    'report': PERSIST_COL_TEXT,
}


def _stats_persist_fields(field_type: type) -> list[str]:
    return [
        field.name
        for field in dataclasses.fields(ComparisonStats)
        if field.type is field_type
    ]


def _details_persist_fields() -> list[str]:
    return [field.name for field in dataclasses.fields(ComparisonDiffDetails)]


STATS_INTEGER_FIELDS = _stats_persist_fields(int)
STATS_FLOAT_FIELDS = _stats_persist_fields(float)
DETAILS_JSON_FIELDS = _details_persist_fields()


def _to_json_string(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _normalize_details_for_persist(details: Optional[Dict]) -> Dict:
    normalized = dict(details or {})
    for key in DETAILS_JSON_FIELDS:
        if normalized.get(key) is None:
            normalized[key] = []
    return normalized


def _format_sql_literal(value) -> str:
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _render_query_with_params(
    query: Optional[str], params: Optional[Dict]
) -> Optional[str]:
    if not query:
        return None
    if not params:
        return query

    rendered = query
    for key in sorted(params, key=len, reverse=True):
        rendered = rendered.replace(f':{key}', _format_sql_literal(params[key]))
    return rendered


def _coerce_persist_record(
    record: Dict,
    column_types: Dict[str, str],
    engine: Optional[Engine] = None,
) -> Dict:
    """Convert logical persist values to DB-driver-friendly Python types."""
    coerced = dict(record)
    keep_datetime_as_string = (
        engine is not None and engine.dialect.name == 'sqlite'
    )
    for column, col_type in column_types.items():
        if col_type != PERSIST_COL_DATETIME:
            continue
        value = coerced.get(column)
        if value is None or not isinstance(value, str):
            continue
        timestamp = pd.Timestamp(value)
        if keep_datetime_as_string:
            coerced[column] = timestamp.strftime(DATETIME_FORMAT)
        else:
            coerced[column] = timestamp.to_pydatetime()
    return coerced


def _extract_base_persist_value(payload: Dict, column: str):
    if column == PERSIST_TIMEZONE_COLUMN:
        return payload.get('timezone')
    if column == 'comparison_tags_json':
        return _to_json_string(payload.get('comparison_tags'))
    if column == 'source_query':
        return _render_query_with_params(
            payload.get('source_query'), payload.get('source_params')
        )
    if column == 'target_query':
        return _render_query_with_params(
            payload.get('target_query'), payload.get('target_params')
        )
    if column.endswith('_json'):
        return _to_json_string(payload.get(column.removesuffix('_json')))
    return payload.get(column)


@dataclass(frozen=True)
class PersistResultOptions:
    """Normalized ``persist_result`` argument from comparison methods."""

    enabled: bool
    table_ref: Optional[DataReference] = None


def parse_persist_result_option(
    persist_result: Union[bool, DataReference],
) -> PersistResultOptions:
    """Parse ``persist_result``: ``True``/``False`` or a custom ``DataReference`` table."""
    if isinstance(persist_result, DataReference):
        return PersistResultOptions(enabled=True, table_ref=persist_result)
    return PersistResultOptions(enabled=bool(persist_result))


class ComparisonResultPersister:
    """Persist comparison output to file and/or SQL table."""

    def __init__(
        self,
        results_engine: Optional[Engine] = None,
        results_table: str = 'xoverrr_comparison_results',
        results_schema: Optional[str] = None,
    ):
        self.results_engine = results_engine
        self.results_table = results_table
        self.results_schema = results_schema
        self.adapters = {
            DBMSType.ORACLE: OracleAdapter(),
            DBMSType.POSTGRESQL: PostgresAdapter(),
            DBMSType.CLICKHOUSE: ClickHouseAdapter(),
        }

    def persist(
        self,
        result: ComparisonResult,
        persist_result: bool = False,
        persist_result_ref: Optional[DataReference] = None,
    ) -> None:
        should_persist_db = persist_result and self.results_engine is not None
        if should_persist_db:
            self._persist_to_db(result, persist_result_ref)

    def _persist_to_db(
        self, result: ComparisonResult, persist_result_ref: Optional[DataReference]
    ) -> None:
        try:
            full_payload = result.to_dict()
            record = self._build_db_record(result, full_payload)
            table_ref = self._resolve_table_target(persist_result_ref)
            adapter = self._get_adapter_for_engine(self.results_engine)
            column_types = self._build_column_types()
            adapter.ensure_persistence_table(
                self.results_engine,
                table_ref,
                column_types,
                primary_key=PERSIST_PRIMARY_KEY,
            )
            record = _coerce_persist_record(
                record, column_types, engine=self.results_engine
            )
            adapter.insert_persistence_record(self.results_engine, table_ref, record)
            table_name = persist_result_ref.full_name if persist_result_ref else self.results_table
            app_logger.info(f'Comparison result persisted to {table_name}')
        except Exception as exc:
            app_logger.warning(
                f'Unable to persist comparison result to storage engine: {exc}'
            )

    def _build_db_record(
        self, result: ComparisonResult, full_payload: Dict
    ) -> Dict:
        stats = full_payload.get('stats') or {}
        details = _normalize_details_for_persist(full_payload.get('details'))

        record = {
            column: _extract_base_persist_value(full_payload, column)
            for column in BASE_PERSIST_COLUMN_TYPES
            if column not in ('run_id', *TIMING_PERSIST_FIELDS)
        }
        record['run_id'] = validate_run_id(result.run_id)
        if result.timings:
            for field in TIMING_PERSIST_FIELDS:
                record[field] = getattr(result.timings, field)
        elif result.timestamp:
            record['run_started_at'] = result.timestamp

        for key in STATS_INTEGER_FIELDS:
            record[f'stats_{key}'] = stats.get(key)

        for key in STATS_FLOAT_FIELDS:
            record[f'stats_{key}'] = _round_stats_float_for_persist(stats.get(key))

        for key in DETAILS_JSON_FIELDS:
            record[f'details_{key}_json'] = _to_json_string(details.get(key))

        return record

    def _build_column_types(self) -> Dict[str, str]:
        column_types = dict(BASE_PERSIST_COLUMN_TYPES)
        for field in STATS_INTEGER_FIELDS:
            column_types[f'stats_{field}'] = PERSIST_COL_INT
        for field in STATS_FLOAT_FIELDS:
            column_types[f'stats_{field}'] = PERSIST_COL_FLOAT
        for field in DETAILS_JSON_FIELDS:
            column_types[f'details_{field}_json'] = PERSIST_COL_TEXT
        return column_types

    def _resolve_table_target(
        self, persist_result_ref: Optional[DataReference]
    ) -> DataReference:
        if persist_result_ref:
            return persist_result_ref
        return DataReference(self.results_table, self.results_schema)

    def _get_adapter_for_engine(self, engine: Engine):
        if engine.dialect.name == 'sqlite':
            # Used in unit tests; PostgreSQL SQL syntax is compatible here.
            return self.adapters[DBMSType.POSTGRESQL]
        db_type = DBMSType.from_engine(engine)
        return self.adapters[db_type]
