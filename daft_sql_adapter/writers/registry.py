"""Registry for format-specific writers (factory)."""

from daft_sql_adapter.exceptions import WriteError
from daft_sql_adapter.writers.base import TableWriter
from daft_sql_adapter.writers.delta_writer import DeltaTableWriter
from daft_sql_adapter.writers.iceberg_writer import IcebergTableWriter


def get_writer(format_name: str, iceberg_catalog=None) -> TableWriter:
    """Return a TableWriter for the given format (delta | iceberg)."""
    fmt = (format_name or "").strip().lower()
    if fmt == "delta":
        return DeltaTableWriter()
    if fmt == "iceberg":
        return IcebergTableWriter(catalog=iceberg_catalog)
    raise WriteError(f"Unsupported output format: {format_name}. Use 'delta' or 'iceberg'.")
