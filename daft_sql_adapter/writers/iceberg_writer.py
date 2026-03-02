"""Write Daft DataFrame to Iceberg table via PyIceberg catalog."""

from __future__ import annotations

from daft_sql_adapter.exceptions import WriteError
from daft_sql_adapter.writers.base import WriteResult


class IcebergTableWriter:
    """
    Writes a DataFrame to an Iceberg table.
    Requires a PyIceberg table (from a catalog). For new tables at a path,
    the caller must create the table via catalog.create_table(..., location=path) first.
    """

    def __init__(self, catalog=None):
        """
        Optionally pass a PyIceberg catalog for creating new tables.
        If None, path_or_identifier must be a table object or load_table() result.
        """
        self._catalog = catalog

    def write(
        self,
        df,  # DataFrame
        path_or_identifier: str,
        mode: str = "overwrite",
    ) -> WriteResult:
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as e:
            raise WriteError(
                "Iceberg write requires pyiceberg. Install with: pip install pyiceberg"
            ) from e

        identifier = path_or_identifier.strip()
        if not identifier:
            raise WriteError("Iceberg table identifier or path cannot be empty")

        catalog = self._catalog
        if catalog is None:
            catalog = load_catalog("default")

        try:
            table = catalog.load_table(identifier)
        except Exception as e:
            raise WriteError(
                f"Iceberg table not found or cannot be created: {identifier}. {e}"
            ) from e

        try:
            df.write_iceberg(table, mode=mode)
        except AttributeError as e:
            raise WriteError(
                "Iceberg write requires Daft with Iceberg support. "
                "Install with: pip install pyiceberg and ensure daft supports write_iceberg."
            ) from e
        except Exception as e:
            raise WriteError(f"Failed to write Iceberg table {identifier}: {e}") from e

        return WriteResult(path_or_identifier=identifier, format="iceberg")
