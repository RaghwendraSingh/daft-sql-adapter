"""Transpile Spark SQL to PostgreSQL (Daft dialect) using SQLGlot."""

from __future__ import annotations

from enum import Enum
from typing import Protocol

from daft_sql_adapter.exceptions import TranspileError

try:
    import sqlglot
except ImportError as e:
    raise ImportError(
        "sqlglot is required for Spark SQL transpilation. Install with: pip install sqlglot"
    ) from e


class SqlDialect(str, Enum):
    """Supported SQL dialects for transpilation."""

    SPARK = "spark"
    POSTGRES = "postgres"


class SqlTranspiler(Protocol):
    """Abstraction for SQL transpilation (DIP)."""

    def transpile(self, sql: str, read_dialect: str, write_dialect: str) -> str:
        """Return a single transpiled statement. Raise TranspileError on failure."""
        ...


class SQLGlotTranspiler:
    """Transpile SQL between dialects using SQLGlot."""

    def transpile(
        self,
        sql: str,
        read_dialect: str = SqlDialect.SPARK.value,
        write_dialect: str = SqlDialect.POSTGRES.value,
    ) -> str:
        normalized = sql.strip()
        if not normalized:
            raise TranspileError("SQL string is empty", original_sql=sql)
        try:
            parsed = sqlglot.parse(normalized, read=read_dialect)
            if not parsed:
                raise TranspileError("Failed to parse SQL", original_sql=sql)
            statements = list(parsed)
            if len(statements) != 1:
                raise TranspileError(
                    "Only single-statement SQL is supported",
                    original_sql=sql,
                )
            transpiled = statements[0].sql(dialect=write_dialect)
            if not transpiled or not transpiled.strip():
                raise TranspileError("Transpilation produced empty output", original_sql=sql)
            return transpiled.strip()
        except sqlglot.errors.ParseError as e:
            raise TranspileError(
                f"Parse error: {e}",
                original_sql=sql,
            ) from e
        except Exception as e:
            if isinstance(e, TranspileError):
                raise
            raise TranspileError(
                f"Transpilation failed: {e}",
                original_sql=sql,
            ) from e


def transpile_spark_to_postgres(
    spark_sql: str,
    transpiler: SqlTranspiler | None = None,
) -> str:
    """Convenience: transpile Spark SQL to PostgreSQL."""
    if transpiler is None:
        transpiler = SQLGlotTranspiler()
    return transpiler.transpile(
        spark_sql,
        read_dialect=SqlDialect.SPARK.value,
        write_dialect=SqlDialect.POSTGRES.value,
    )
