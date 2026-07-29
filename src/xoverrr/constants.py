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

# Check result statuses
CHECK_SUCCESS = 'success'
CHECK_FAILED = 'failed'
CHECK_SKIPPED = 'skipped'

# Check types
CHECK_TYPE_COUNT = 'count'
CHECK_TYPE_SAMPLE = 'sample'
CHECK_TYPE_CUSTOM_QUERY = 'custom_query'
CHECK_TYPE_SNIFF_QUERY = 'sniff_query'

# Shared y/n flag convention for x-prefixed check columns.
FLAG_VALUE_YES = 'y'
FLAG_VALUE_NO = 'n'

# Recently changed exclusion column (check_sample / check_query).
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
