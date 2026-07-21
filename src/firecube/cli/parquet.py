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

"""Parquet-related helper commands for the Firecube CLI."""

from __future__ import annotations

import json
import time
from pathlib import Path

import click
import duckdb

from firecube.cli._ctx import get_storage_config
from firecube.cli._product import resolve_product_identity
from firecube.cli._shared_options import (
    product_uri_option,
    storage_driver_option,
    storage_type_option,
)
from firecube.cli._uri_policy import apply_smart_default, parse_product_uri
from firecube.core import observability
from firecube.core.filesystem import StorageFilesystem
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def parquet(ctx: click.Context) -> None:
    """validate and consolidate Parquet datasets

    Validation and consolidation of Parquet datasets. Use parquet validate to
    check file integrity and parquet consolidate to merge scattered files into
    a single optimized file.
    """
    observability.init_observability("firecube-parquet")
    ctx.ensure_object(dict)


def _list_parquet_files(fs: StorageFilesystem, root: StorageUri) -> list[StorageUri]:
    if not root.path or root.path == "/":
        return []

    try:
        if fs.exists(root) and not fs.isdir(root):
            return [root]
    except Exception:
        pass

    try:
        candidates = fs.find(root)
    except Exception:
        candidates = []

    files = [p for p in candidates if isinstance(p, StorageUri) and p.path.endswith(".parquet")]
    if files:
        return sorted(set(files), key=lambda u: u.to_str())

    if root.path.endswith(".parquet"):
        return [root]

    return []


def _check_parquet_magic(fs: StorageFilesystem, path: StorageUri) -> tuple[bool, str]:
    """Minimal Parquet validity check without DuckDB/PyArrow: 'PAR1' magic bytes.

    Uses ``fs.info(path)["size"]`` plus a positive-offset ``seek(size - 4)`` so
    the check works uniformly across the fsspec and obstore drivers (the
    previous ``seek(-4, 2)`` form relied on fsspec-specific seek semantics).
    """
    try:
        size = int(fs.info(path).get("size") or 0)
    except Exception as exc:
        return False, f"Failed to stat: {exc}"

    if size < 8:
        return False, "File too small to be a Parquet"

    try:
        with fs.open(path, "rb") as handle:
            head = handle.read(4)
            handle.seek(size - 4)
            tail = handle.read(4)
    except Exception as exc:
        return False, f"Failed to open/read: {exc}"

    if head != b"PAR1":
        return False, "Missing PAR1 header"
    if tail != b"PAR1":
        return False, "Missing PAR1 footer"
    return True, ""


@parquet.command(
    "validate",
    epilog="""\b
Examples:
  # validate all Parquet files for a product
  firecube parquet validate -p <product>

See also: firecube parquet consolidate, firecube chunks list
""",
)
@product_uri_option(tier="inspect")
@storage_type_option(required=False)
@storage_driver_option(required=False)
@click.pass_context
def validate(
    ctx: click.Context,
    product: str,
    storage_type: str | None,
    storage_driver: str | None,
) -> None:
    """validate Parquet files

    Checks PAR1 magic bytes at head and tail. Scans all .parquet files under
    the product path and reports any missing or corrupt files. Read-only; does
    not modify any data.
    """
    parsed_uri = parse_product_uri(product)
    storage_type = apply_smart_default(parsed_uri, storage_type)
    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    identity = resolve_product_identity(
        parsed_uri.normalized, format="parquet", product_name=parsed_uri.normalized
    )
    session = StorageSession(
        StorageBinding(
            identity=identity,
            driver=driver_config,
        )
    )

    store_uri = identity.product_uri
    store_uri_str = store_uri.to_str()
    fs = session.fs()

    start = time.time()
    files = _list_parquet_files(fs, store_uri)
    if not files:
        click.echo(
            json.dumps(
                {
                    "product": product,
                    "resolved": {"store_uri": store_uri_str, "root": store_uri_str},
                    "valid": False,
                    "error": "No .parquet files found under target",
                },
                indent=2,
            )
        )
        return

    bad: list[dict[str, str]] = []
    for path in files:
        ok, reason = _check_parquet_magic(fs, path)
        if not ok:
            bad.append({"path": path.to_str(), "reason": reason})

    payload = {
        "product": product,
        "resolved": {"store_uri": store_uri_str, "root": store_uri_str},
        "valid": not bad,
        "files_checked": len(files),
        "invalid_files": bad[:20],
        "duration_s": round(time.time() - start, 3),
    }
    click.echo(json.dumps(payload, indent=2))


@parquet.command(
    "consolidate",
    epilog="""\b
Examples:
  # consolidate a product into a single file with default zstd compression
  firecube parquet consolidate -p <product> -o file:///tmp/consolidated.parquet

\b
  # consolidate with explicit codec
  firecube parquet consolidate -p <product> -o file:///tmp/consolidated.parquet --codec snappy

See also: firecube parquet validate
""",
)
@product_uri_option(tier="inspect")
@click.option(
    "-o",
    "--output",
    required=True,
    type=str,
    help="Output artifact URI (file:///abs/path); s3:// accepted but runtime support coming later",
)
@click.option("--codec", default="zstd", help="compression codec (default: zstd)")
@storage_type_option(required=False)
@storage_driver_option(required=False)
@click.pass_context
def consolidate(
    ctx: click.Context,
    product: str,
    output: str,
    codec: str,
    storage_type: str | None,
    storage_driver: str | None,
) -> None:
    """merge Parquet files into one

    Merges a scattered Parquet dataset into a single optimized file using
    DuckDB. Reads all .parquet files under the product path and writes a
    single consolidated file with the specified codec. The source dataset
    is not modified.
    """
    parsed_uri = parse_product_uri(product)
    storage_type = apply_smart_default(parsed_uri, storage_type)
    parsed_output = parse_product_uri(output)
    if parsed_output.scheme == "s3":
        raise click.ClickException("Remote artifact output not yet supported")
    storage_conf = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_conf)
    identity = resolve_product_identity(
        parsed_uri.normalized, format="parquet", product_name=parsed_uri.normalized
    )
    session = StorageSession(
        StorageBinding(
            identity=identity,
            driver=driver_config,
        )
    )

    store_uri = identity.product_uri.to_str()
    output_path = str(Path(parsed_output.normalized.removeprefix("file://")).resolve())

    con = duckdb.connect()
    session.duckdb.apply(con, output_uri=output_path)

    # Construct input pattern
    source = store_uri
    if not source.endswith(".parquet") and not source.endswith("*"):
        source = f"{source.rstrip('/')}/*.parquet"

    click.echo(f"Consolidating {source} -> {output_path} ...")
    start = time.time()

    try:
        con.execute(f"""
            COPY (SELECT * FROM '{source}')
            TO '{output_path}'
            (FORMAT 'parquet', COMPRESSION '{codec}')
        """)

        duration = time.time() - start
        click.echo(
            json.dumps(
                {
                    "status": "success",
                    "source": product,
                    "resolved_source": source,
                    "output": output_path,
                    "duration_s": duration,
                },
                indent=2,
            )
        )

    except Exception as exc:
        raise click.ClickException(f"Consolidation failed: {exc}") from exc
