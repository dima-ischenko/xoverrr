import dataclasses
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Literal, Optional

import pandas as pd
from sqlalchemy.engine import Engine

from .adapters.base import PERSIST_INSERTED_AT_COLUMN
from .adapters.clickhouse import ClickHouseAdapter
from .adapters.oracle import OracleAdapter
from .adapters.postgres import PostgresAdapter
from .constants import DATETIME_FORMAT, STATS_REPORT_FLOAT_DECIMALS
from .logger import app_logger
from .models import DataReference, DBMSType
from .reporting import CheckResult
from .utils import CheckDetails, CheckStats

PERSIST_PRIMARY_KEY = 'run_id'
RUN_ID_LENGTH = 16
QuerySide = Literal['source', 'target']

RUN_TIME_PERSIST_FIELDS = (
    'run_started_at',
    'run_finished_at',
)
DURATION_PERSIST_FIELDS = (
    'source_query_duration_sec',
    'target_query_duration_sec',
    'dataset_check_duration_sec',
)
PERSIST_DURATION_DECIMALS = 2


def _round_stats_float_for_persist(value) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), STATS_REPORT_FLOAT_DECIMALS)


def _round_duration_sec_for_persist(value) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), PERSIST_DURATION_DECIMALS)


def validate_run_id(run_id: Optional[str]) -> str:
    """Ensure run_id is present and normalized for persistence/logging."""
    if run_id is None:
        raise ValueError('run_id must be a non-empty string')
    normalized = str(run_id).strip()
    if not normalized:
        raise ValueError('run_id must be a non-empty string')
    return normalized


def build_run_id() -> str:
    """Build a random non-empty run identifier for a check run."""
    return uuid.uuid4().hex[:RUN_ID_LENGTH]


@dataclass
class CheckRunTimings:
    """Run bounds plus accumulated step durations in seconds."""

    run_started_at: Optional[str] = None
    run_finished_at: Optional[str] = None
    source_query_duration_sec: Optional[float] = None
    target_query_duration_sec: Optional[float] = None
    dataset_check_duration_sec: Optional[float] = None
    _active_started: Dict[str, float] = field(
        default_factory=dict, repr=False, compare=False
    )

    @staticmethod
    def now() -> str:
        return pd.Timestamp.now().strftime(DATETIME_FORMAT)

    def mark_query_start(self, side: QuerySide) -> None:
        self._active_started[f'{side}_query'] = time.perf_counter()

    def mark_query_end(self, side: QuerySide) -> None:
        self._add_elapsed(f'{side}_query', f'{side}_query_duration_sec')

    def mark_dataset_check_start(self) -> None:
        self._active_started['dataset_check'] = time.perf_counter()

    def mark_dataset_check_end(self) -> None:
        self._add_elapsed('dataset_check', 'dataset_check_duration_sec')

    def finish_run(self) -> None:
        self.run_finished_at = self.now()

    def _add_elapsed(self, timer_key: str, duration_attr: str) -> None:
        started = self._active_started.pop(timer_key, None)
        if started is None:
            return
        current = getattr(self, duration_attr) or 0.0
        setattr(self, duration_attr, current + (time.perf_counter() - started))


# Portable logical column types mapped to DB-specific DDL in adapter PERSIST_TYPE_MAP.
PERSIST_COL_SHORT_STRING = 'short_string'
PERSIST_COL_STRING = 'string'
PERSIST_COL_NAME = 'name'
PERSIST_COL_TABLE_REF = 'table_ref'
PERSIST_COL_TZ_NAME = 'tz_name'
PERSIST_COL_DATETIME = 'datetime'
PERSIST_COL_DB_NOW = 'db_now'
PERSIST_COL_TEXT = 'text'
PERSIST_COL_INT = 'int'
PERSIST_COL_FLOAT = 'float'

# Persisted column name (avoids reserved TIMEZONE keyword in Oracle).
PERSIST_TIMEZONE_COLUMN = 'check_timezone'


def _stats_persist_fields(field_type: type) -> list[str]:
    return [
        field.name
        for field in dataclasses.fields(CheckStats)
        if field.type is field_type
    ]


def _details_persist_fields() -> list[str]:
    return [field.name for field in dataclasses.fields(CheckDetails)]


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
    keep_datetime_as_string = engine is not None and engine.dialect.name == 'sqlite'
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
    if column == 'check_tags_json':
        return _to_json_string(payload.get('check_tags'))
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


def normalize_persist_result(
    persist_result: Optional[DataReference],
) -> Optional[DataReference]:
    """Accept a results table or ``None`` (do not persist)."""
    if persist_result is None:
        return None
    if isinstance(persist_result, DataReference):
        return persist_result
    raise TypeError('persist_result must be a DataReference or None')


class CheckResultPersister:
    """Persist check results to a SQL table."""

    def __init__(self, results_engine: Optional[Engine] = None):
        self.results_engine = results_engine
        self.adapters = {
            DBMSType.ORACLE: OracleAdapter(),
            DBMSType.POSTGRESQL: PostgresAdapter(),
            DBMSType.CLICKHOUSE: ClickHouseAdapter(),
        }

    def persist(
        self,
        result: CheckResult,
        table_ref: Optional[DataReference] = None,
    ) -> bool:
        """Write ``result`` to ``table_ref``.

        Returns False if a table was given and the write did not succeed.
        """
        table_ref = normalize_persist_result(table_ref)
        if table_ref is None:
            return True
        if self.results_engine is None:
            app_logger.warning(
                'Unable to persist check result: results_engine is not configured'
            )
            return False
        return self._persist_to_db(result, table_ref)

    def _persist_to_db(self, result: CheckResult, table_ref: DataReference) -> bool:
        try:
            column_types = self._build_column_types()
            record = self._build_db_record(result, result.to_dict(), column_types)
            record = _coerce_persist_record(
                record, column_types, engine=self.results_engine
            )
            adapter = self._get_adapter_for_engine(self.results_engine)
            adapter.ensure_persistence_table(
                self.results_engine,
                table_ref,
                column_types,
                primary_key=PERSIST_PRIMARY_KEY,
            )
            adapter.insert_persistence_record(
                self.results_engine, table_ref, record, column_types
            )
            app_logger.info(f'Check result persisted to {table_ref.full_name}')
            return True
        except Exception as exc:
            app_logger.warning(
                f'Unable to persist check result to storage engine: {exc}'
            )
            return False

    def _build_db_record(
        self,
        result: CheckResult,
        full_payload: Dict,
        column_types: Dict[str, str],
    ) -> Dict:
        stats = full_payload.get('stats') or {}
        details = _normalize_details_for_persist(full_payload.get('details'))
        record = {}
        for column, col_type in column_types.items():
            if col_type == PERSIST_COL_DB_NOW or column == PERSIST_INSERTED_AT_COLUMN:
                continue
            if column == 'run_id':
                record[column] = validate_run_id(result.run_id)
            elif column in RUN_TIME_PERSIST_FIELDS:
                if result.timings:
                    record[column] = getattr(result.timings, column)
                elif column == 'run_started_at' and result.timestamp:
                    record[column] = result.timestamp
                else:
                    record[column] = None
            elif column in DURATION_PERSIST_FIELDS:
                if result.timings:
                    record[column] = _round_duration_sec_for_persist(
                        getattr(result.timings, column)
                    )
                else:
                    record[column] = None
            elif column.startswith('stats_'):
                key = column.removeprefix('stats_')
                value = stats.get(key)
                if col_type == PERSIST_COL_FLOAT:
                    record[column] = _round_stats_float_for_persist(value)
                else:
                    record[column] = value
            elif column.startswith('details_') and column.endswith('_json'):
                key = column.removeprefix('details_').removesuffix('_json')
                record[column] = _to_json_string(details.get(key))
            else:
                record[column] = _extract_base_persist_value(full_payload, column)
        return record

    def _build_column_types(self) -> Dict[str, str]:
        """Query-friendly column order: identity, score, run times, then durations."""
        column_types: Dict[str, str] = {}

        def add(name: str, col_type: str) -> None:
            if name not in column_types:
                column_types[name] = col_type

        add('run_id', PERSIST_COL_SHORT_STRING)
        add('check_name', PERSIST_COL_NAME)
        add('status', PERSIST_COL_STRING)
        add('stats_final_score', PERSIST_COL_FLOAT)
        add('stats_final_diff_score', PERSIST_COL_FLOAT)
        add('run_started_at', PERSIST_COL_DATETIME)
        add('run_finished_at', PERSIST_COL_DATETIME)
        add(PERSIST_INSERTED_AT_COLUMN, PERSIST_COL_DB_NOW)
        for field_name in DURATION_PERSIST_FIELDS:
            add(field_name, PERSIST_COL_FLOAT)
        add('check_type', PERSIST_COL_STRING)
        add('source_table', PERSIST_COL_TABLE_REF)
        add('target_table', PERSIST_COL_TABLE_REF)
        add('check_tags_json', PERSIST_COL_TEXT)
        add(PERSIST_TIMEZONE_COLUMN, PERSIST_COL_TZ_NAME)

        for field in STATS_INTEGER_FIELDS:
            add(f'stats_{field}', PERSIST_COL_INT)
        for field in STATS_FLOAT_FIELDS:
            add(f'stats_{field}', PERSIST_COL_FLOAT)

        add('source_query', PERSIST_COL_TEXT)
        add('target_query', PERSIST_COL_TEXT)
        add('report', PERSIST_COL_TEXT)

        for field in DETAILS_JSON_FIELDS:
            add(f'details_{field}_json', PERSIST_COL_TEXT)

        return column_types

    def _get_adapter_for_engine(self, engine: Engine):
        if engine.dialect.name == 'sqlite':
            # Used in unit tests; PostgreSQL SQL syntax is compatible here.
            return self.adapters[DBMSType.POSTGRESQL]
        db_type = DBMSType.from_engine(engine)
        return self.adapters[db_type]
