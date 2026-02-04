from .constants import (COMPARISON_FAILED, COMPARISON_SKIPPED,
                        COMPARISON_SUCCESS)
from .core import DataQualityComparator, DataReference

__all__ = [
    'DataQualityComparator',
    'DataReference',
    'COMPARISON_SUCCESS',
    'COMPARISON_FAILED',
    'COMPARISON_SKIPPED',
]

__version__ = '1.1.6'
