"""
Orchestrates: load credentials -> Unity -> load tables -> transpile -> classify -> execute.
Single entry point for run_sql (SELECT or CTAS).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from daft_sql_adapter.catalog import TableLoader, create_unity_catalog
from daft_sql_adapter.config import load_credentials
from daft_sql_adapter.ctas import execute_ctas, CtasResult
from daft_sql_adapter.exceptions import RunnerError, SqlDispatchError
from daft_sql_adapter.pagination import PageMetadata, Paginator
from daft_sql_adapter.sql import classify_query, transpile_spark_to_postgres, QueryType

if TYPE_CHECKING:
    from daft import DataFrame
    from daft.session import Session


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
) -> CtasResult | SelectResult:
    """
    Execute Spark SQL against Unity Catalog tables.

    - Transpiles Spark SQL to PostgreSQL, loads tables into a Daft Session, runs the query.
    - If the query is CREATE TABLE AS SELECT: executes the SELECT and writes to Delta or Iceberg;
      requires output_path (or LOCATION in SQL) and output_format ("delta" | "iceberg").
    - If the query is SELECT: returns a paginated Arrow IPC result.

    Returns CtasResult for CTAS (status, path_or_identifier, format) or SelectResult (data bytes, metadata).
    """
    credentials = load_credentials(credentials_provider)
    unity = unity_catalog or create_unity_catalog(credentials)
    loader = TableLoader(unity)

    import daft
    from daft.session import Session

    session = Session()
    if table_names:
        loader.load_into_session(session, table_names)

    transpiled = transpile_spark_to_postgres(spark_sql)
    query_type = classify_query(spark_sql)

    if query_type == QueryType.CREATE_TABLE:
        if not output_format:
            raise RunnerError("CREATE TABLE requires output_format ('delta' or 'iceberg')")
        ctas_result = execute_ctas(
            session,
            spark_sql,
            output_path=output_path,
            output_format=output_format,
            iceberg_catalog=iceberg_catalog,
        )
        return ctas_result

    if query_type == QueryType.SELECT:
        result_df: DataFrame = session.sql(transpiled)
        paginator = Paginator(page=page, page_size=page_size)
        data_bytes, meta = paginator.paginate_and_serialize(result_df)
        return SelectResult(data=data_bytes, metadata=meta)

    raise SqlDispatchError(f"Unsupported query type. Only SELECT and CREATE TABLE AS SELECT are supported.")
