"""
Execution backends: Session (default) and Spark (daft.pyspark on Ray).
Both produce a Daft DataFrame from run_sql() for pagination and writers.
"""

from daft_sql_adapter.backend.factory import get_backend
from daft_sql_adapter.backend.protocol import ExecutionBackend

__all__ = [
    "ExecutionBackend",
    "get_backend",
]
