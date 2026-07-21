# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reusable DuckDB helpers for Firecube ingestion plugins."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

log = logging.getLogger("firecube.ingestor.utils.duckdb")

_IDENT_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(identifier: str) -> str:
    """Safely quote a DuckDB identifier (table/column).

    DuckDB does not support parameterizing identifiers. To prevent SQL injection,
    we restrict identifiers to a conservative subset and always quote them.
    """
    identifier = str(identifier)
    parts = [p for p in identifier.split(".") if p]
    if not parts:
        raise ValueError("Identifier cannot be empty")
    for part in parts:
        if not _IDENT_PART_RE.match(part):
            raise ValueError(f"Invalid identifier: {identifier!r}")
    return ".".join(f'"{p}"' for p in parts)


def _quote_literal(value: str) -> str:
    """Safely quote a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def open_duckdb(in_memory: bool = True, db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection."""
    import duckdb

    if in_memory:
        log.debug("Opening in-memory DuckDB instance")
        return duckdb.connect(":memory:")
    if not db_path:
        raise ValueError("db_path must be provided when in_memory=False")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.debug("Opening file-backed DuckDB at %s", db_path)
    return duckdb.connect(str(db_path))


def ensure_table(con: duckdb.DuckDBPyConnection, table: str, schema_sql: str) -> None:
    """Ensure a table exists using the provided CREATE TABLE statement."""
    con.execute(schema_sql)
    log.debug("Ensured table %s exists", table)


def truncate_table(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Delete existing rows without dropping the table."""
    con.execute(f"DELETE FROM {_quote_ident(table)}")
    log.debug("Truncated table %s", table)


def deduplicate(
    con: duckdb.DuckDBPyConnection,
    table: str,
    group_by: str,
    value_columns: str | None = None,
) -> dict[str, Any]:
    """
    Deduplicate rows using a DuckDB GROUP BY statement.

    Args:
        con: DuckDB connection
        table: table name to deduplicate
        group_by: columns to group by (comma-separated)
        value_columns: optional SQL projection for value columns (defaults to ANY_VALUE for all)
    """
    table_ident = _quote_ident(table)
    total_rows_row = con.sql(f"SELECT COUNT(*) FROM {table_ident}").fetchone()
    total_rows = total_rows_row[0] if total_rows_row else 0
    if total_rows == 0:
        return {"total_rows": 0, "unique_rows": 0, "duplicate_rows": 0, "duplicate_ratio": 0.0}

    group_cols = [c.strip() for c in str(group_by).split(",") if c.strip()]
    if not group_cols:
        raise ValueError("group_by must include at least one column")
    group_expr = ", ".join(_quote_ident(c) for c in group_cols)

    if value_columns:
        vc = str(value_columns)
        if ";" in vc or "--" in vc or "/*" in vc or "*/" in vc:
            raise ValueError("value_columns contains disallowed SQL tokens")
        projection = f"{group_expr}, {vc}"
    else:
        projection = f"{group_expr}, ANY_VALUE(*)"

    con.execute(
        f"""
        CREATE TEMPORARY TABLE __dedup AS
        SELECT {projection}
        FROM {table_ident}
        GROUP BY {group_expr}
        """
    )
    unique_rows_row = con.sql("SELECT COUNT(*) FROM __dedup").fetchone()
    unique_rows = unique_rows_row[0] if unique_rows_row else 0
    con.execute(f"DELETE FROM {table_ident}")
    table_cols = [
        row[1] for row in con.execute(f"PRAGMA table_info({_quote_literal(table)})").fetchall()
    ]
    if not table_cols:
        raise RuntimeError(f"Unable to determine columns for table {table}")
    col_list = ", ".join(_quote_ident(c) for c in table_cols)
    con.execute(f"INSERT INTO {table_ident} ({col_list}) SELECT {col_list} FROM __dedup")
    con.execute("DROP TABLE __dedup")

    duplicate_rows = total_rows - unique_rows
    ratio = duplicate_rows / total_rows
    log.debug("Deduplicated table %s: %s duplicates", table, duplicate_rows)
    return {
        "total_rows": int(total_rows),
        "unique_rows": int(unique_rows),
        "duplicate_rows": int(duplicate_rows),
        "duplicate_ratio": float(ratio),
    }


def export_parquet(con: duckdb.DuckDBPyConnection, table: str, output_path: Path) -> dict[str, Any]:
    """Export a table to Parquet."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    con.sql(f"COPY {_quote_ident(table)} TO {_quote_literal(str(output_path))} (FORMAT PARQUET)")
    duration = time.time() - start
    size_bytes = output_path.stat().st_size if output_path.exists() else 0
    log.debug("Exported %s to Parquet (%s bytes)", table, size_bytes)
    return {"format": "parquet", "path": output_path, "size_b": size_bytes, "duration_s": duration}


def apply_duckdb_settings(
    con: duckdb.DuckDBPyConnection,
    *,
    max_temp_directory_size: str | None = None,
    memory_limit: str | None = None,
    threads: int | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Best-effort apply common DuckDB PRAGMA/SET settings.

    This helper is intentionally optional: plugins that do not use DuckDB can
    ignore it entirely. Failures are logged and do not raise by default.
    """
    logger = logger or log
    applied: dict[str, Any] = {}

    if max_temp_directory_size:
        try:
            con.execute(
                f"PRAGMA max_temp_directory_size={_quote_literal(str(max_temp_directory_size))}"
            )
            applied["max_temp_directory_size"] = str(max_temp_directory_size)
        except Exception as exc:
            logger.warning(
                "Failed to apply DuckDB max_temp_directory_size=%s: %s",
                max_temp_directory_size,
                exc,
            )

    if memory_limit:
        try:
            con.execute(f"SET memory_limit={_quote_literal(str(memory_limit))}")
            applied["memory_limit"] = str(memory_limit)
        except Exception as exc:
            logger.warning("Failed to apply DuckDB memory_limit=%s: %s", memory_limit, exc)

    if threads is not None:
        try:
            threads_int = int(threads)
            if threads_int > 0:
                con.execute(f"SET threads={threads_int}")
                applied["threads"] = threads_int
        except Exception as exc:
            logger.warning("Failed to apply DuckDB threads=%s: %s", threads, exc)

    return applied


def ensure_table_schema(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    create_sql: str,
    expected_columns: Iterable[str] | None = None,
    logger: logging.Logger | None = None,
) -> bool:
    """Ensure the table columns match the expected schema.

    If `expected_columns` is provided and the existing column order differs,
    the table is dropped and re-created from `create_sql`.

    Returns True when a recreate happened.
    """
    logger = logger or log
    if expected_columns is None:
        return False

    expected_list = [str(c) for c in expected_columns]
    try:
        info = con.execute(f"PRAGMA table_info({_quote_literal(table)})").fetchall()
    except Exception:
        info = []
    existing = [row[1] for row in info] if info else []
    if existing and existing != expected_list:
        con.execute(f"DROP TABLE {_quote_ident(table)}")
        con.execute(create_sql)
        logger.warning("Recreated DuckDB table %s due to schema mismatch", table)
        return True
    return False
