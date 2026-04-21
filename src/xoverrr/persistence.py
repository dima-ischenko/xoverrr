import json
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, Text
from sqlalchemy.engine import Engine

from .logger import app_logger
from .models import DataReference
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
        self._metadata = MetaData()
        self._table_cache: Dict[str, Table] = {}

    def persist(
        self,
        result: ComparisonResult,
        persist_result: bool = False,
        persist_result_ref: Optional[DataReference] = None,
        report_output_path: Optional[str] = None,
        report_output_format: str = 'json',
    ) -> None:
        output_format = (report_output_format or 'json').lower()
        if output_format not in {'json', 'text'}:
            raise ValueError("report_output_format must be either 'json' or 'text'")

        should_persist_db = persist_result and self.results_engine is not None
        should_write_file = bool(report_output_path)

        if not should_persist_db and not should_write_file:
            return

        if should_write_file:
            self._persist_to_file(
                result=result,
                report_output_path=report_output_path,
                report_output_format=output_format,
            )

        if should_persist_db:
            self._persist_to_db(result, persist_result_ref)

    def _persist_to_file(
        self,
        result: ComparisonResult,
        report_output_path: str,
        report_output_format: str,
    ) -> None:
        output_path = Path(report_output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if report_output_format == 'text':
            output_path.write_text(result.report or '', encoding='utf-8')
            app_logger.info(f'Text comparison report persisted: {output_path}')
            return

        output_path.write_text(result.to_json(), encoding='utf-8')
        app_logger.info(f'JSON comparison report persisted: {output_path}')

    def _persist_to_db(
        self, result: ComparisonResult, persist_result_ref: Optional[DataReference]
    ) -> None:
        try:
            payload = result.to_dict()
            record = self._build_db_record(payload)
            table = self._get_results_table(persist_result_ref)
            table.create(self.results_engine, checkfirst=True)
            with self.results_engine.begin() as conn:
                conn.execute(table.insert().values(**record))
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

    def _get_results_table(self, persist_result_ref: Optional[DataReference]) -> Table:
        table_name = persist_result_ref.name if persist_result_ref else self.results_table
        table_schema = (
            persist_result_ref.schema if persist_result_ref else self.results_schema
        )
        cache_key = f'{table_schema}.{table_name}' if table_schema else table_name
        if cache_key not in self._table_cache:
            self._table_cache[cache_key] = self._build_results_table(
                table_name=table_name,
                table_schema=table_schema,
            )
        return self._table_cache[cache_key]

    def _build_results_table(self, table_name: str, table_schema: Optional[str]) -> Table:
        columns = [
            Column('timestamp', String(32), nullable=False),
            Column('comparison_type', String(64), nullable=False),
            Column('status', String(32), nullable=False),
            Column('comparison_name', String(255)),
            Column('comparison_tags_json', Text),
            Column('source_table', String(512)),
            Column('target_table', String(512)),
            Column('timezone', String(64)),
            Column('source_query', Text),
            Column('source_params_json', Text),
            Column('target_query', Text),
            Column('target_params_json', Text),
            Column('report', Text),
            Column('payload_json', Text),
            Column('final_data_quality_score', Float),
            Column('final_diff_score', Float),
        ]

        for field in self.STATS_INTEGER_FIELDS:
            columns.append(Column(f'stats_{field}', Integer))
        for field in self.STATS_FLOAT_FIELDS:
            columns.append(Column(f'stats_{field}', Float))
        for field in self.DETAILS_JSON_FIELDS:
            columns.append(Column(f'details_{field}_json', Text))

        return Table(
            table_name,
            self._metadata,
            *columns,
            schema=table_schema,
            extend_existing=True,
        )
