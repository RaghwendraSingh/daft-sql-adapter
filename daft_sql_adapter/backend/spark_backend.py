"""
Spark execution backend: daft.pyspark.SparkSession on Ray.
Registers tables via createDataFrame + createOrReplaceTempView;
run_sql returns a Daft DataFrame (convert from Spark result via toPandas -> from_pandas).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from daft_sql_adapter.exceptions import RunnerError

if TYPE_CHECKING:
    from daft import DataFrame


def _check_spark_available() -> None:
    try:
        from daft.pyspark import SparkSession  # noqa: F401
    except ImportError as e:
        raise RunnerError(
            "Spark backend requires daft[spark]. Install with: pip install 'daft-sql-adapter[spark]'"
        ) from e


class SparkBackend:
    """
    Uses daft.pyspark.SparkSession.builder.remote(ray_url).getOrCreate().
    Expects Spark SQL (no transpilation). Table registration: Daft DataFrame -> to_pandas -> createDataFrame -> createOrReplaceTempView.
    run_sql: spark.sql(spark_sql) -> result to_pandas() -> daft.from_pandas() for unified Daft DataFrame.
    """

    def sql_dialect(self) -> Literal["postgres", "spark"]:
        return "spark"

    def __init__(self, ray_url: str) -> None:
        _check_spark_available()
        from daft.pyspark import SparkSession

        self._spark = SparkSession.builder.remote(ray_url).getOrCreate()

    def register_table(self, name: str, df: "DataFrame") -> None:
        pdf = df.to_pandas()
        spark_df = self._spark.createDataFrame(pdf)
        spark_df.createOrReplaceTempView(name)

    def run_sql(self, sql: str) -> "DataFrame":
        import daft

        spark_df = self._spark.sql(sql)
        # PySpark-like API: toPandas() or .to_pandas(); daft.pyspark may expose one of these
        if hasattr(spark_df, "toPandas"):
            pdf = spark_df.toPandas()
        elif hasattr(spark_df, "to_pandas"):
            pdf = spark_df.to_pandas()
        else:
            raise RunnerError(
                "Spark backend: sql result has no toPandas/to_pandas; cannot convert to Daft DataFrame."
            )
        return daft.from_pandas(pdf)
