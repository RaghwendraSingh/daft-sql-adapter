"""Protocol for execution backends: register tables and run SQL returning Daft DataFrame."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from daft import DataFrame


@runtime_checkable
class ExecutionBackend(Protocol):
    """Session-like execution: register tables, run SQL, return Daft DataFrame."""

    def sql_dialect(self) -> Literal["postgres", "spark"]:
        """SQL dialect expected by run_sql: 'postgres' (Daft native) or 'spark' (daft.pyspark)."""
        ...

    def register_table(self, name: str, df: "DataFrame") -> None:
        """Register a Daft DataFrame as a table/view under the given name."""
        ...

    def run_sql(self, sql: str) -> "DataFrame":
        """Execute SQL in the backend's expected dialect (see sql_dialect()). Returns a Daft DataFrame."""
        ...
