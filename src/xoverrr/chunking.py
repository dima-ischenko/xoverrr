"""Date-window validation and query chunking."""

from typing import Dict, List, Optional, Tuple

import pandas as pd

from .constants import DATE_FORMAT


def has_date_bound(value) -> bool:
    return value is not None and str(value).strip() != ''


def normalize_date_bound(value) -> Optional[str]:
    if not has_date_bound(value):
        return None
    return str(value).strip()


def unpack_date_range(
    date_range: Optional[Tuple[Optional[str], Optional[str]]],
) -> Tuple[Optional[str], Optional[str]]:
    if date_range is None:
        return None, None
    start_date, end_date = date_range
    return normalize_date_bound(start_date), normalize_date_bound(end_date)


def validate_date_window_args(
    date_column: Optional[str],
    date_range: Optional[Tuple[Optional[str], Optional[str]]],
    chunk_size_days: Optional[int],
) -> None:
    start_date = end_date = None
    if date_range is not None:
        if not isinstance(date_range, (tuple, list)) or len(date_range) != 2:
            raise ValueError('date_range must be (start_date, end_date)')
        start_date, end_date = unpack_date_range(date_range)
        if start_date is None and end_date is None:
            raise ValueError('date_range requires start_date and/or end_date')
    if chunk_size_days is not None:
        if date_range is None:
            raise ValueError('date_range is required when chunk_size_days is set')
        if start_date is None or end_date is None:
            raise ValueError(
                'date_range requires both start_date and end_date'
                ' when chunk_size_days is set'
            )
    if date_column:
        return
    if date_range is not None or chunk_size_days is not None:
        raise ValueError(
            'date_column is required when date_range or chunk_size_days is set'
        )


def iter_date_chunks(
    date_column: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    chunk_size_days: Optional[int],
) -> List[Tuple[Optional[str], Optional[str]]]:
    if chunk_size_days is not None and chunk_size_days <= 0:
        raise ValueError('chunk_size_days must be greater than 0')

    if not (
        chunk_size_days
        and date_column
        and has_date_bound(start_date)
        and has_date_bound(end_date)
    ):
        return [(start_date, end_date)]

    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    if start_ts > end_ts:
        raise ValueError(
            f'date_range start {start_date} is greater than end {end_date}'
        )

    chunks: List[Tuple[str, str]] = []
    current = start_ts
    while current <= end_ts:
        chunk_end = min(current + pd.Timedelta(days=chunk_size_days - 1), end_ts)
        chunks.append(
            (
                current.strftime(DATE_FORMAT),
                chunk_end.strftime(DATE_FORMAT),
            )
        )
        current = chunk_end + pd.Timedelta(days=1)
    return chunks


def resolve_custom_query_chunks(
    source_params: Dict,
    target_params: Dict,
    chunk_size_days: Optional[int],
) -> List[Tuple[Dict, Dict]]:
    source_params = source_params or {}
    target_params = target_params or {}
    source_start = source_params.get('start_date')
    source_end = source_params.get('end_date')
    target_start = target_params.get('start_date')
    target_end = target_params.get('end_date')

    if not (
        chunk_size_days
        and source_start is not None
        and source_end is not None
        and target_start is not None
        and target_end is not None
    ):
        return [(dict(source_params), dict(target_params))]

    source_chunks = iter_date_chunks('date', source_start, source_end, chunk_size_days)
    target_chunks = iter_date_chunks('date', target_start, target_end, chunk_size_days)
    if len(source_chunks) != len(target_chunks):
        raise ValueError(
            'source and target custom query date ranges produce different chunk counts'
        )

    chunk_ranges: List[Tuple[Dict, Dict]] = []
    for (s_start, s_end), (t_start, t_end) in zip(source_chunks, target_chunks):
        source_chunk_params = dict(source_params)
        target_chunk_params = dict(target_params)
        source_chunk_params['start_date'] = s_start
        source_chunk_params['end_date'] = s_end
        target_chunk_params['start_date'] = t_start
        target_chunk_params['end_date'] = t_end
        chunk_ranges.append((source_chunk_params, target_chunk_params))
    return chunk_ranges


def resolve_source_query_chunks(
    source_params: Dict,
    chunk_size_days: Optional[int],
) -> List[Dict]:
    source_params = dict(source_params or {})
    source_start = source_params.get('start_date')
    source_end = source_params.get('end_date')

    if not (chunk_size_days and source_start is not None and source_end is not None):
        return [source_params]

    source_chunks = iter_date_chunks('date', source_start, source_end, chunk_size_days)
    chunk_params: List[Dict] = []
    for start, end in source_chunks:
        params = dict(source_params)
        params['start_date'] = start
        params['end_date'] = end
        chunk_params.append(params)
    return chunk_params
