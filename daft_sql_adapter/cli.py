"""CLI for daft-sql-adapter: Spark SQL against Unity Catalog."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from daft_sql_adapter.config import load_credentials
from daft_sql_adapter.exceptions import (
    ConfigError,
    DaftSqlAdapterError,
    TranspileError,
    SqlDispatchError,
    RunnerError,
    WriteError,
)
from daft_sql_adapter.runner import run_sql, CtasResult, SelectResult


def _parse_tables(value: str) -> list[str]:
    """Parse --tables 'a.b.c,d.e.f' or repeated --tables into list."""
    if not value.strip():
        return []
    return [t.strip() for t in value.split(",") if t.strip()]


def main() -> int:
    """Entry point for the CLI. Returns exit code."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run Spark SQL against Databricks Unity Catalog (Daft on Ray)."
    )
    parser.add_argument(
        "--sql",
        required=True,
        help="Spark SQL query (SELECT or CREATE TABLE AS SELECT).",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated full Unity table names (catalog.schema.table).",
    )
    parser.add_argument(
        "--page",
        type=int,
        default=1,
        help="Page number for SELECT results (default: 1).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Page size for SELECT results (default: 1000).",
    )
    parser.add_argument(
        "--output",
        help="Output file path for Arrow IPC data (SELECT) or unused for CTAS.",
    )
    parser.add_argument(
        "--metadata",
        help="Output file path for JSON metadata (SELECT: page, page_size, total_count).",
    )
    parser.add_argument(
        "--format",
        choices=["delta", "iceberg"],
        help="Output format for CREATE TABLE AS SELECT (required for CTAS).",
    )
    parser.add_argument(
        "--output-path",
        help="Output path for CTAS (Delta path or Iceberg table identifier). Required if LOCATION not in SQL.",
    )
    parser.add_argument(
        "--config",
        help="Path to config file (optional; else DATABRICKS_HOST and DATABRICKS_TOKEN).",
    )
    args = parser.parse_args()

    if args.config:
        import os
        os.environ["DAFT_SQL_ADAPTER_CONFIG"] = args.config

    table_names = _parse_tables(args.tables)

    try:
        result = run_sql(
            args.sql,
            table_names,
            page=args.page,
            page_size=args.page_size,
            output_path=args.output_path or None,
            output_format=args.format or None,
        )
    except ConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1
    except TranspileError as e:
        print(f"Transpile error: {e}", file=sys.stderr)
        return 1
    except (SqlDispatchError, RunnerError, WriteError) as e:
        print(f"Execution error: {e}", file=sys.stderr)
        return 1
    except DaftSqlAdapterError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if isinstance(result, CtasResult):
        print(result.status)
        if result.path_or_identifier:
            print(f"Written to: {result.path_or_identifier} ({result.format})")
        return 0

    if isinstance(result, SelectResult):
        out_path = args.output
        if out_path:
            Path(out_path).write_bytes(result.data)
        else:
            sys.stdout.buffer.write(result.data)
        meta = result.metadata
        meta_dict = {
            "page": meta.page,
            "page_size": meta.page_size,
            "total_count": meta.total_count,
            "format": meta.format,
        }
        if args.metadata:
            Path(args.metadata).write_text(json.dumps(meta_dict, indent=2), encoding="utf-8")
        else:
            print(json.dumps(meta_dict), file=sys.stderr)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
