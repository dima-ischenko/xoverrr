# Date and time formats
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = f'{DATE_FORMAT} %H:%M:%S'

# Default values
NULL_REPLACEMENT = 'N/A'
DEFAULT_MAX_EXAMPLES = 3
DEFAULT_MAX_SAMPLE_SIZE_GB = 3  # Max size of dataframe to compare

# SQL patterns
RESERVED_WORDS = ['date', 'comment', 'file', 'number', 'mode', 'successful']

DEFAULT_TZ = 'UTC'

# Comparison result statuses
COMPARISON_SUCCESS = 'success'
COMPARISON_FAILED = 'failed'
COMPARISON_SKIPPED = 'skipped'

# Comparison types
COMPARISON_TYPE_COUNT = 'count'
COMPARISON_TYPE_SAMPLE = 'sample'
COMPARISON_TYPE_CUSTOM_QUERY = 'custom_query'
COMPARISON_TYPE_SNIFF_QUERY = 'sniff_query'

# Shared y/n flag convention for x-prefixed comparison columns.
FLAG_VALUE_YES = 'y'
FLAG_VALUE_NO = 'n'

# Recently changed exclusion column (compare_sample / compare_custom_query).
XRECENTLY_CHANGED_COLUMN = 'xrecently_changed'

# Pass/fail flag column for sniff_query (row-level or scalar).
# ``y`` = passed, ``n`` = failed.
XSNIFF_PASSED_COLUMN = 'xsniff_passed'
XSNIFF_PASSED_VALUE_YES = FLAG_VALUE_YES
XSNIFF_PASSED_VALUE_NO = FLAG_VALUE_NO

# Report output formats
REPORT_OUTPUT_FORMAT_JSON = 'json'
REPORT_OUTPUT_FORMAT_TEXT = 'text'
REPORT_OUTPUT_FORMATS = frozenset({
    REPORT_OUTPUT_FORMAT_JSON,
    REPORT_OUTPUT_FORMAT_TEXT,
})

# Float precision used in text reports and persisted stats columns.
STATS_REPORT_FLOAT_DECIMALS = 5
