import json
from typing import Dict, Optional

from sqlalchemy.engine import Engine

from .adapters.clickhouse import ClickHouseAdapter
from .adapters.oracle import OracleAdapter
from .adapters.postgres import PostgresAdapter
from .logger import app_logger
from .models import DBMSType, DataReference
from .reporting import ComparisonResult


def _to_json_string(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


class ComparisonResultPersister:
    """Persist comparison output to file and/or SQL table."""

    STATS_INTEGER_FIELDS = [
        'total_source_rows',
        'total_target_rows',
        'dup_source_rows',
        'dup_target_rows',
        'only_source_rows',
        'only_target_rows',
        'common_pk_rows',
        'total_matched_rows',
    ]
    STATS_FLOAT_FIELDS = [
        'dup_source_percentage_rows',
        'dup_target_percentage_rows',
        'source_only_percentage_rows',
        'target_only_percentage_rows',
        'total_diff_percentage_rows',
        'max_diff_percentage_cols',
        'median_diff_percentage_cols',
        'final_diff_score',
        'final_score',
    ]
    DETAILS_JSON_FIELDS = [
        'mismatches_per_column',
        'discrepancies_per_col_examples',
        'dup_source_keys_examples',
        'dup_target_keys_examples',
        'source_only_keys_examples',
        'target_only_keys_examples',
        'discrepant_data_examples',
        'common_attribute_columns',
        'skipped_source_columns',
        'skipped_target_columns',
    ]

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
            payload = result.to_dict()
            record = self._build_db_record(payload)
            table_ref = self._resolve_table_target(persist_result_ref)
            adapter = self._get_adapter_for_engine(self.results_engine)
            adapter.ensure_persistence_table(
                self.results_engine, table_ref, self._build_column_types()
            )
            adapter.insert_persistence_record(self.results_engine, table_ref, record)
            table_name = persist_result_ref.full_name if persist_result_ref else self.results_table
            app_logger.info(f'Comparison result persisted to {table_name}')
        except Exception as exc:
            app_logger.warning(
                f'Unable to persist comparison result to storage engine: {exc}'
            )

    def _build_db_record(self, payload: Dict) -> Dict:
        stats = payload.get('stats') or {}
        details = payload.get('details') or {}

        record = {
            'timestamp': payload.get('timestamp'),
            'comparison_type': payload.get('comparison_type'),
            'status': payload.get('status'),
            'comparison_name': payload.get('comparison_name'),
            'comparison_tags_json': _to_json_string(payload.get('comparison_tags')),
            'source_table': payload.get('source_table'),
            'target_table': payload.get('target_table'),
            'timezone': payload.get('timezone'),
            'source_query': payload.get('source_query'),
            'source_params_json': _to_json_string(payload.get('source_params')),
            'target_query': payload.get('target_query'),
            'target_params_json': _to_json_string(payload.get('target_params')),
            'report': payload.get('report'),
            'payload_json': _to_json_string(payload),
            'final_data_quality_score': stats.get('final_score'),
            'final_diff_score': stats.get('final_diff_score'),
        }

        for key in self.STATS_INTEGER_FIELDS + self.STATS_FLOAT_FIELDS:
            value = stats.get(key)
            record[f'stats_{key}'] = value

        for key in self.DETAILS_JSON_FIELDS:
            value = details.get(key)
            record[f'details_{key}_json'] = _to_json_string(value)

        return record

    def _build_column_types(self) -> Dict[str, str]:
        column_types = {
            'timestamp': 'string',
            'comparison_type': 'string',
            'status': 'string',
            'comparison_name': 'string',
            'comparison_tags_json': 'text',
            'source_table': 'string',
            'target_table': 'string',
            'timezone': 'string',
            'source_query': 'text',
            'source_params_json': 'text',
            'target_query': 'text',
            'target_params_json': 'text',
            'report': 'text',
            'payload_json': 'text',
            'final_data_quality_score': 'float',
            'final_diff_score': 'float',
        }
        for field in self.STATS_INTEGER_FIELDS:
            column_types[f'stats_{field}'] = 'int'
        for field in self.STATS_FLOAT_FIELDS:
            column_types[f'stats_{field}'] = 'float'
        for field in self.DETAILS_JSON_FIELDS:
            column_types[f'details_{field}_json'] = 'text'
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
