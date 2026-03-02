"""Abstract table writer for CTAS (Open/Closed principle)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from daft_sql_adapter.exceptions import WriteError

if TYPE_CHECKING:
    from daft import DataFrame


@dataclass(frozen=True)
class WriteResult:
    """Result of a successful table write."""

    path_or_identifier: str
    format: str  # "delta" | "iceberg"


class TableWriter(Protocol):
    """Abstraction for writing a DataFrame to an external table (DIP)."""

    def write(
        self,
        df: DataFrame,
        path_or_identifier: str,
        mode: str = "overwrite",
    ) -> WriteResult:
        """
        Write the DataFrame to the given path or table identifier.
        Raise WriteError on failure.
        """
        ...
