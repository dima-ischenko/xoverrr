class DQCheckException(Exception):
    """Base exception for data quality check errors"""

    pass


class MetadataError(DQCheckException):
    """Exception raised for metadata-related errors"""

    pass


class QueryExecutionError(DQCheckException):
    """Exception raised for query execution failures"""

    pass


class TypeConversionError(DQCheckException):
    """Exception raised for type conversion failures"""

    pass
