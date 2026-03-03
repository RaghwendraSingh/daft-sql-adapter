"""
Orchestrates: load credentials -> Unity -> load tables -> transpile -> classify -> execute.
Single entry point for run_sql (SELECT or CTAS).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from daft_sql_adapter.backend import get_backend
from daft_sql_adapter.catalog import TableLoader, create_unity_catalog
from daft_sql_adapter.config import load_credentials
from daft_sql_adapter.ctas import execute_ctas, CtasResult
from daft_sql_adapter.exceptions import RunnerError, SqlDispatchError
from daft_sql_adapter.pagination import PageMetadata, Paginator
from daft_sql_adapter.sql import classify_query, transpile_spark_to_postgres, QueryType

if TYPE_CHECKING:
    from daft import DataFrame


@dataclass
class SelectResult:
    """Paginated SELECT result: Arrow IPC bytes and metadata."""

    data: bytes
    metadata: PageMetadata


def run_sql(
    spark_sql: str,
    table_names: list[str],
    *,
    page: int = 1,
    page_size: int = 1000,
    output_path: str | None = None,
    output_format: str | None = None,
    iceberg_catalog: Any = None,
    credentials_provider=None,
    unity_catalog=None,
    backend: Literal["session", "spark"] = "session",
    ray_url: str | None = None,
) -> CtasResult | SelectResult:
    """
    Execute Spark SQL against Unity Catalog tables.

    - Transpiles Spark SQL to PostgreSQL, loads tables into the execution backend, runs the query.
    - backend="session" (default): Daft Session (uses Ray when on a Ray cluster).
    - backend="spark": daft.pyspark.SparkSession on Ray; set RAY_URL or ray_url (e.g. ray://head:6379).
    - If the query is CREATE TABLE AS SELECT: executes the SELECT and writes to Delta or Iceberg;
      requires output_path (or LOCATION in SQL) and output_format ("delta" | "iceberg").
    - If the query is SELECT: returns a paginated Arrow IPC result.

    Returns CtasResult for CTAS (status, path_or_identifier, format) or SelectResult (data bytes, metadata).
    """
    credentials = load_credentials(credentials_provider)
    unity = unity_catalog or create_unity_catalog(credentials)
    loader = TableLoader(unity)

    exec_backend = get_backend(backend=backend, ray_url=ray_url)
    if table_names:
        loader.load_into_backend(exec_backend, table_names)

    transpiled = transpile_spark_to_postgres(spark_sql)
    query_type = classify_query(spark_sql)

    if query_type == QueryType.CREATE_TABLE:
        if not output_format:
            raise RunnerError("CREATE TABLE requires output_format ('delta' or 'iceberg')")
        ctas_result = execute_ctas(
            exec_backend,
            spark_sql,
            output_path=output_path,
            output_format=output_format,
            iceberg_catalog=iceberg_catalog,
        )
        return ctas_result

    if query_type == QueryType.SELECT:
        result_df: DataFrame = exec_backend.run_sql(transpiled)
        paginator = Paginator(page=page, page_size=page_size)
        data_bytes, meta = paginator.paginate_and_serialize(result_df)
        return SelectResult(data=data_bytes, metadata=meta)

    raise SqlDispatchError(f"Unsupported query type. Only SELECT and CREATE TABLE AS SELECT are supported.")
