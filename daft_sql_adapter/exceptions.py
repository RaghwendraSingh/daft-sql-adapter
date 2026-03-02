"""Custom exceptions for daft-sql-adapter."""


class DaftSqlAdapterError(Exception):
    """Base exception for all adapter errors."""

    pass


class ConfigError(DaftSqlAdapterError):
    """Raised when configuration or credentials are invalid or missing."""

    pass


class TranspileError(DaftSqlAdapterError):
    """Raised when Spark SQL cannot be transpiled to PostgreSQL."""

    def __init__(self, message: str, original_sql: str | None = None):
        super().__init__(message)
        self.original_sql = original_sql


class SqlDispatchError(DaftSqlAdapterError):
    """Raised when SQL classification or CTAS parsing fails."""

    pass


class RunnerError(DaftSqlAdapterError):
    """Raised when execution (Unity, Daft Session, or write) fails."""

    pass


class WriteError(DaftSqlAdapterError):
    """Raised when writing to Delta or Iceberg fails."""

    pass
