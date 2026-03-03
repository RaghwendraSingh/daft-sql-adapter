"""Load Unity Catalog tables into a Daft Session or execution backend (full names)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft_sql_adapter.exceptions import RunnerError

if TYPE_CHECKING:
    from daft import DataFrame
    from daft.unity_catalog import UnityCatalog

    from daft_sql_adapter.backend.protocol import ExecutionBackend


class TableLoader:
    """Loads a list of Unity Catalog tables and registers them for SQL execution."""

    def __init__(self, unity: UnityCatalog) -> None:
        self._unity = unity

    def load_into_backend(
        self,
        backend: "ExecutionBackend",
        table_names: list[str],
    ) -> None:
        """
        Load each table from Unity Catalog and register it in the backend
        under its full name (catalog.schema.table) so SQL can reference it.
        """
        import daft

        for name in table_names:
            full_name = name.strip()
            if not full_name:
                continue
            try:
                unity_table = self._unity.load_table(full_name)
                df: DataFrame = daft.read_deltalake(unity_table)
                backend.register_table(full_name, df)
            except Exception as e:
                raise RunnerError(f"Failed to load table {full_name}: {e}") from e
