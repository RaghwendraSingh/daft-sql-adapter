"""Protocol for execution backends: register tables and run SQL returning Daft DataFrame."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from daft import DataFrame


@runtime_checkable
class ExecutionBackend(Protocol):
    """Session-like execution: register tables, run SQL, return Daft DataFrame."""

    def register_table(self, name: str, df: "DataFrame") -> None:
        """Register a Daft DataFrame as a table/view under the given name."""
        ...

    def run_sql(self, sql: str) -> "DataFrame":
        """Execute the given SQL (PostgreSQL dialect) and return a Daft DataFrame."""
        ...
