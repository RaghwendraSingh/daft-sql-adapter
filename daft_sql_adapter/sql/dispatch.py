"""Classify SQL (CREATE TABLE vs SELECT) and parse CREATE TABLE AS SELECT."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from daft_sql_adapter.exceptions import SqlDispatchError

try:
    import sqlglot
    import sqlglot.expressions as exp
except ImportError:
    sqlglot = None
    exp = None


class QueryType(str, Enum):
    """Supported query types for execution."""

    CREATE_TABLE = "create_table"
    SELECT = "select"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CtasParsed:
    """Parsed CREATE TABLE AS SELECT: target identifier and the SELECT SQL."""

    target_name: str
    location: str | None
    select_sql: str


def classify_query(sql: str) -> QueryType:
    """Classify the given SQL (Spark dialect) as CREATE_TABLE, SELECT, or UNKNOWN."""
    normalized = sql.strip()
    if not normalized:
        return QueryType.UNKNOWN
    upper = normalized.upper()
    if upper.startswith("CREATE TABLE"):
        return QueryType.CREATE_TABLE
    if upper.startswith("SELECT"):
        return QueryType.SELECT
    return QueryType.UNKNOWN


def parse_ctas(spark_sql: str) -> CtasParsed:
    """
    Parse CREATE TABLE ... AS SELECT and return target name, optional LOCATION, and SELECT SQL.
    Uses SQLGlot when available; otherwise a simple regex fallback for LOCATION and AS SELECT.
    """
    normalized = spark_sql.strip()
    if not normalized.upper().startswith("CREATE TABLE"):
        raise SqlDispatchError("Not a CREATE TABLE statement")

    if sqlglot is not None and exp is not None:
        return _parse_ctas_sqlglot(normalized)
    return _parse_ctas_fallback(normalized)


def _parse_ctas_sqlglot(spark_sql: str) -> CtasParsed:
    """Parse CTAS using SQLGlot AST."""
    parsed = sqlglot.parse(spark_sql, read="spark")
    if not parsed:
        raise SqlDispatchError("Failed to parse CREATE TABLE statement")
    stmt = list(parsed)[0]
    if not isinstance(stmt, exp.Create):
        raise SqlDispatchError("Expected CREATE statement")

    name = stmt.this
    if name is not None:
        target_name = name.sql(dialect="spark")
    else:
        target_name = ""

    location = None
    if stmt.expression:
        select_sql = stmt.expression.sql(dialect="spark")
    else:
        for prop in stmt.expressions if hasattr(stmt, "expressions") else []:
            if isinstance(prop, exp.LocationProperty):
                location = prop.args.get("value")
                if hasattr(location, "this"):
                    location = location.this
                break
        select_sql = ""
    if not select_sql and hasattr(stmt, "expression") and stmt.expression:
        select_sql = stmt.expression.sql(dialect="spark")

    for key, value in (stmt.properties or {}).items():
        if key and key.upper() == "LOCATION" and value:
            location = getattr(value, "this", value) or str(value)
            break

    if not select_sql.strip():
        raise SqlDispatchError("CREATE TABLE AS SELECT must include a SELECT query")
    return CtasParsed(target_name=target_name.strip(), location=location, select_sql=select_sql.strip())


def _parse_ctas_fallback(spark_sql: str) -> CtasParsed:
    """Fallback: extract LOCATION and SELECT via regex when SQLGlot is unavailable."""
    location = None
    loc_match = re.search(r"\bLOCATION\s+['\"]([^'\"]+)['\"]", spark_sql, re.IGNORECASE)
    if loc_match:
        location = loc_match.group(1).strip()

    as_select = re.search(r"\s+AS\s+(SELECT\s+.+)", spark_sql, re.IGNORECASE | re.DOTALL)
    if not as_select:
        raise SqlDispatchError("CREATE TABLE AS SELECT must include a SELECT query")
    select_sql = as_select.group(1).strip()

    create_match = re.match(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)", spark_sql, re.IGNORECASE)
    target_name = create_match.group(1).strip() if create_match else "unknown"

    return CtasParsed(target_name=target_name, location=location, select_sql=select_sql)
