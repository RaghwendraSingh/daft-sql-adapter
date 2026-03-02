"""Table writers for Delta and Iceberg (CTAS output)."""

from daft_sql_adapter.writers.base import TableWriter, WriteResult
from daft_sql_adapter.writers.delta_writer import DeltaTableWriter
from daft_sql_adapter.writers.iceberg_writer import IcebergTableWriter
from daft_sql_adapter.writers.registry import get_writer

__all__ = [
    "TableWriter",
    "WriteResult",
    "DeltaTableWriter",
    "IcebergTableWriter",
    "get_writer",
]
