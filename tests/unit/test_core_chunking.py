import pytest

from xoverrr.core import DataQualityComparator


def _comparator_without_init() -> DataQualityComparator:
    return DataQualityComparator.__new__(DataQualityComparator)


def test_iter_date_chunks_returns_single_range_without_chunking():
    comparator = _comparator_without_init()

    chunks = comparator._iter_date_chunks(
        date_column='created_at',
        start_date='2024-01-01',
        end_date='2024-01-31',
        chunk_size_days=None,
    )

    assert chunks == [('2024-01-01', '2024-01-31')]


def test_iter_date_chunks_splits_range_by_days():
    comparator = _comparator_without_init()

    chunks = comparator._iter_date_chunks(
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
    comparator = _comparator_without_init()

    with pytest.raises(ValueError, match='chunk_size_days must be greater than 0'):
        comparator._iter_date_chunks(
            date_column='created_at',
            start_date='2024-01-01',
            end_date='2024-01-31',
            chunk_size_days=0,
        )
