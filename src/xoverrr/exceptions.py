class DQCheckException(Exception):
    """Base exception for data-quality check errors."""

    pass


class MetadataError(DQCheckException):
    """Raised when metadata cannot be read or interpreted."""

    pass


class QueryExecutionError(DQCheckException):
    """Raised when a query fails to execute."""

    pass


class TypeConversionError(DQCheckException):
    """Raised when a type conversion fails."""

    pass
