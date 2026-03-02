"""Write Daft DataFrame to Delta Lake table at a path."""

from __future__ import annotations

from daft_sql_adapter.exceptions import WriteError
from daft_sql_adapter.writers.base import WriteResult


class DeltaTableWriter:
    """Writes a DataFrame to a Delta Lake table at the given path."""

    def write(
        self,
        df,  # DataFrame - avoid importing daft at module level for lazy load
        path_or_identifier: str,
        mode: str = "overwrite",
    ) -> WriteResult:
        path = path_or_identifier.strip()
        if not path:
            raise WriteError("Delta output path cannot be empty")
        try:
            df.write_deltalake(path, mode=mode)
        except AttributeError as e:
            raise WriteError(
                "Delta Lake write requires daft with deltalake support. "
                "Install with: pip install 'getdaft[deltalake]'"
            ) from e
        except Exception as e:
            raise WriteError(f"Failed to write Delta table to {path}: {e}") from e
        return WriteResult(path_or_identifier=path, format="delta")
