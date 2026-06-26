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

# Report output formats
REPORT_OUTPUT_FORMAT_JSON = 'json'
REPORT_OUTPUT_FORMAT_TEXT = 'text'
REPORT_OUTPUT_FORMATS = frozenset({
    REPORT_OUTPUT_FORMAT_JSON,
    REPORT_OUTPUT_FORMAT_TEXT,
})

# Float precision used in text reports and persisted stats columns.
STATS_REPORT_FLOAT_DECIMALS = 5
