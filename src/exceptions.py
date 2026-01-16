class DQCompareException(Exception):
    """Base exception for data quality comparison errors"""
    pass

class MetadataError(DQCompareException):
    """Exception raised for metadata-related errors"""
    pass

class QueryExecutionError(DQCompareException):
    """Exception raised for query execution failures"""
    pass

class TypeConversionError(DQCompareException):
    """Exception raised for type conversion failures"""
    pass