import pytest

from xoverrr.constants import DEFAULT_MAX_DATAFRAME_SIZE_GB
from xoverrr.core import DataQualityChecker


def _comparator_without_init() -> DataQualityChecker:
    return DataQualityChecker.__new__(DataQualityChecker)


def test_iter_date_chunks_returns_single_range_without_chunking():
    checker = _comparator_without_init()

    chunks = checker._iter_date_chunks(
        date_column='created_at',
        start_date='2024-01-01',
        end_date='2024-01-31',
        chunk_size_days=None,
    )

    assert chunks == [('2024-01-01', '2024-01-31')]


def test_iter_date_chunks_splits_range_by_days():
    checker = _comparator_without_init()

    chunks = checker._iter_date_chunks(
        date_column='created_at',
        start_date='2024-01-01',
        end_date='2024-01-31',
        chunk_size_days=10,
    )

    assert chunks == [
        ('2024-01-01', '2024-01-10'),
        ('2024-01-11', '2024-01-20'),
        ('2024-01-21', '2024-01-30'),
        ('2024-01-31', '2024-01-31'),
    ]


def test_iter_date_chunks_raises_on_non_positive_chunk_size():
    checker = _comparator_without_init()

    with pytest.raises(ValueError, match='chunk_size_days must be greater than 0'):
        checker._iter_date_chunks(
            date_column='created_at',
            start_date='2024-01-01',
            end_date='2024-01-31',
            chunk_size_days=0,
        )


def test_iter_date_chunks_without_date_column_returns_single_chunk():
    checker = _comparator_without_init()

    chunks = checker._iter_date_chunks(
        date_column=None,
        start_date=None,
        end_date=None,
        chunk_size_days=None,
    )

    assert chunks == [(None, None)]


def test_validate_date_window_args_allows_missing_date_column():
    checker = _comparator_without_init()
    checker._validate_date_window_args(
        date_column=None, date_range=None, chunk_size_days=None
    )


@pytest.mark.parametrize(
    'date_range, chunk_size_days',
    [
        (('2024-01-01', '2024-01-31'), None),
        (('2024-01-01', '2024-01-31'), 7),
    ],
)
def test_validate_date_window_args_requires_date_column_for_filters(
    date_range, chunk_size_days
):
    checker = _comparator_without_init()

    with pytest.raises(ValueError, match='date_column is required'):
        checker._validate_date_window_args(
            date_column=None,
            date_range=date_range,
            chunk_size_days=chunk_size_days,
        )


def test_iter_date_chunks_open_ended_range_is_single_chunk():
    checker = _comparator_without_init()

    chunks = checker._iter_date_chunks(
        date_column='created_at',
        start_date='2024-01-01',
        end_date=None,
        chunk_size_days=None,
    )

    assert chunks == [('2024-01-01', None)]


@pytest.mark.parametrize(
    'date_range',
    [
        ('2024-01-01', None),
        (None, '2024-12-31'),
        ('2024-01-01', ''),
        ('', '2024-12-31'),
        ('2024-01-01', '  '),
        ('   ', '2024-12-31'),
    ],
)
def test_validate_date_window_args_allows_one_sided_range_without_chunking(
    date_range,
):
    checker = _comparator_without_init()
    checker._validate_date_window_args(
        date_column='created_at',
        date_range=date_range,
        chunk_size_days=None,
    )


@pytest.mark.parametrize(
    'date_range',
    [
        ('2024-01-01', None),
        (None, '2024-12-31'),
        ('2024-01-01', ''),
    ],
)
def test_validate_date_window_args_requires_both_bounds_for_chunking(date_range):
    checker = _comparator_without_init()

    with pytest.raises(
        ValueError,
        match='date_range requires both start_date and end_date when chunk_size_days is set',
    ):
        checker._validate_date_window_args(
            date_column='created_at',
            date_range=date_range,
            chunk_size_days=7,
        )


def test_validate_date_window_args_rejects_empty_range():
    checker = _comparator_without_init()

    with pytest.raises(
        ValueError, match='date_range requires start_date and/or end_date'
    ):
        checker._validate_date_window_args(
            date_column='created_at',
            date_range=(None, None),
            chunk_size_days=None,
        )


def test_validate_date_window_args_requires_date_range_for_chunking():
    checker = _comparator_without_init()

    with pytest.raises(
        ValueError, match='date_range is required when chunk_size_days is set'
    ):
        checker._validate_date_window_args(
            date_column='created_at',
            date_range=None,
            chunk_size_days=7,
        )


def test_validate_date_window_args_accepts_complete_range():
    checker = _comparator_without_init()
    checker._validate_date_window_args(
        date_column='created_at',
        date_range=('2024-01-01', '2024-12-31'),
        chunk_size_days=7,
    )


def test_validate_date_window_args_rejects_incomplete_tuple():
    checker = _comparator_without_init()

    with pytest.raises(ValueError, match='date_range must be \\(start_date, end_date\\)'):
        checker._validate_date_window_args(
            date_column='created_at',
            date_range=('2024-01-01',),
            chunk_size_days=None,
        )
