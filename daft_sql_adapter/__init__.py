"""
Daft SQL Adapter: run Spark SQL against Databricks Unity Catalog via Daft on Ray.

- Transpiles Spark SQL to PostgreSQL (Daft dialect) using SQLGlot.
- Loads Unity Catalog tables into a Daft Session; supports CREATE TABLE AS SELECT
  (write to Delta or Iceberg) and SELECT (paginated Arrow IPC result).
"""

from daft_sql_adapter.config import load_credentials
from daft_sql_adapter.runner import run_sql, CtasResult, SelectResult

__all__ = [
    "load_credentials",
    "run_sql",
    "CtasResult",
    "SelectResult",
]
