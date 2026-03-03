"""Default execution backend using daft.session.Session."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from daft import DataFrame


class SessionBackend:
    """Wraps Daft Session: register_table -> create_temp_table, run_sql -> session.sql(). Expects PostgreSQL dialect."""

    def sql_dialect(self) -> Literal["postgres", "spark"]:
        return "postgres"

    def __init__(self) -> None:
        from daft.session import Session

        self._session = Session()

    def register_table(self, name: str, df: "DataFrame") -> None:
        self._session.create_temp_table(name, df)

    def run_sql(self, sql: str) -> "DataFrame":
        return self._session.sql(sql)
