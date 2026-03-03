"""Factory for execution backends: session (default) or spark (daft.pyspark on Ray)."""

from __future__ import annotations

import os
from typing import Literal

from daft_sql_adapter.backend.protocol import ExecutionBackend
from daft_sql_adapter.backend.session_backend import SessionBackend
from daft_sql_adapter.backend.spark_backend import SparkBackend

_DEFAULT_RAY_URL = "ray://localhost:6379"


def get_backend(
    backend: Literal["session", "spark"] = "session",
    ray_url: str | None = None,
) -> ExecutionBackend:
    """
    Return an execution backend.

    - backend="session": Daft Session (uses Ray when running on a Ray cluster).
    - backend="spark": daft.pyspark.SparkSession connected to Ray at ray_url.
      ray_url defaults to RAY_URL env or "ray://localhost:6379".
    """
    if backend == "session":
        return SessionBackend()
    if backend == "spark":
        url = ray_url or os.environ.get("RAY_URL", _DEFAULT_RAY_URL)
        return SparkBackend(ray_url=url)
    raise ValueError(f"Unknown backend: {backend!r}. Use 'session' or 'spark'.")
