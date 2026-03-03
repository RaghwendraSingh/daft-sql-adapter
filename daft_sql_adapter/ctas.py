"""
CREATE TABLE AS SELECT: parse Spark CTAS, run SELECT via backend, write to Delta or Iceberg.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft_sql_adapter.exceptions import RunnerError
from daft_sql_adapter.sql import parse_ctas, transpile_spark_to_postgres
from daft_sql_adapter.writers import WriteResult, get_writer

if TYPE_CHECKING:
    from daft_sql_adapter.backend.protocol import ExecutionBackend


@dataclass
class CtasResult:
    """Result of executing CREATE TABLE AS SELECT."""

    status: int = 0
    path_or_identifier: str = ""
    format: str = ""


def execute_ctas(
    backend: "ExecutionBackend",
    spark_sql: str,
    output_path: str | None,
    output_format: str,
    iceberg_catalog=None,
) -> CtasResult:
    """
    Parse CREATE TABLE AS SELECT, run the SELECT part via backend, write result to Delta or Iceberg.
    output_path: from LOCATION in SQL or from API/CLI (required if LOCATION not in SQL).
    output_format: "delta" | "iceberg".
    """
    parsed = parse_ctas(spark_sql)
    select_sql = parsed.select_sql
    location = parsed.location
    path = (output_path or "").strip() or location

    if not path and output_format.lower() == "delta":
        raise RunnerError(
            "CREATE TABLE AS SELECT requires an output path. "
            "Set LOCATION in the SQL or pass output_path (e.g. --output-path)."
        )
    if not path and output_format.lower() == "iceberg":
        path = parsed.target_name

    # Session backend expects PostgreSQL; Spark backend expects Spark SQL.
    if backend.sql_dialect() == "postgres":
        sql_to_run = transpile_spark_to_postgres(select_sql)
    else:
        sql_to_run = select_sql
    result_df = backend.run_sql(sql_to_run)

    writer = get_writer(output_format, iceberg_catalog=iceberg_catalog)
    write_result: WriteResult = writer.write(result_df, path, mode="overwrite")

    return CtasResult(
        status=0,
        path_or_identifier=write_result.path_or_identifier,
        format=write_result.format,
    )
