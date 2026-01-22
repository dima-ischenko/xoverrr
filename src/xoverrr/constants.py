# Date and time formats
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = f"{DATE_FORMAT} %H:%M:%S"

# Default values
NULL_REPLACEMENT = "N/A"
DEFAULT_MAX_EXAMPLES = 3
DEFAULT_MAX_SAMPLE_SIZE_GB = 3  # Max size of dataframe to compare

# SQL patterns
RESERVED_WORDS = ['date', 'comment', 'file', 'number', 'mode', 'successful']

DEFAULT_TZ = 'UTC'

# Comparison result statuses
COMPARISON_SUCCESS = 'success'
COMPARISON_FAILED = 'failed'
COMPARISON_SKIPPED = 'skipped'