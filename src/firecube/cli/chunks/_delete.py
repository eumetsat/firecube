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

from __future__ import annotations

import sys
from pathlib import Path

import click

from firecube.core.errors import ManifestError

from ._common import confirm_deletion, parse_datetime, parse_meta_filters
from ._manager import resolve_cli_product, resolve_manager, storage_config_from_ctx


@click.command(
    "delete",
    epilog="""\b
Examples:
  # preview what would be deleted (always do this first)
  firecube chunks delete --product-name <product> --dry-run

\b
  # delete all chunks for a product
  firecube chunks delete --product-name <product> --yes-i-really-mean-it

\b
  # delete chunks in a date range
  firecube chunks delete --product-name <product> --range 2024-01-01,2024-03-31 --dry-run

\b
  # preview deletions across all products
  firecube chunks delete --all-products --dry-run

\b
  # delete across all products (requires explicit confirmation)
  firecube chunks delete --all-products --yes-i-really-mean-it

See also: firecube chunks list, firecube chunks delete-span,
          firecube chunks snapshots rebuild
""",
)
@click.option(
    "--pattern", multiple=True, help="glob pattern to match chunk keys (can specify multiple)"
)
@click.option("-n", "--product-name", "product_name", help="Filter by product name.")
@click.option(
    "--all-products",
    "all_products",
    is_flag=True,
    default=False,
    help="Apply to all products (mutually exclusive with --product-name).",
)
@click.option("--end-date", "end_date", help="delete chunks created before date (YYYY-MM-DD)")
@click.option("--start-date", "start_date", help="delete chunks created after date (YYYY-MM-DD)")
@click.option("--range", "date_range", help="delete chunks in date range (YYYY-MM-DD,YYYY-MM-DD)")
@click.option("--type", "chunk_type", help="filter by chunk type (chunk, meta)")
@click.option(
    "--meta",
    "meta_filters",
    multiple=True,
    help="filter by tracked chunk metadata key=value (value may be JSON)",
)
@click.option("--workspace", type=click.Path(path_type=Path), help="workspace directory override")
@click.option(
    "--manifest-only", is_flag=True, help="remove tracked chunk records only, keep storage files"
)
@click.option(
    "--storage-only", is_flag=True, help="delete from storage only, keep tracked chunk records"
)
@click.option("--yes-i-really-mean-it", is_flag=True, help="skip confirmation prompts")
@click.option("--dry-run", is_flag=True, help="show what would be deleted without doing it")
@click.option(
    "--include-metadata",
    is_flag=True,
    help="include metadata chunks (zarr.json, etc.) - DANGEROUS!",
)
@click.pass_context
def delete_cmd(
    ctx: click.Context,
    pattern,
    product_name,
    all_products: bool,
    end_date,
    start_date,
    date_range,
    chunk_type,
    meta_filters,
    workspace: Path | None,
    manifest_only: bool,
    storage_only: bool,
    yes_i_really_mean_it: bool,
    dry_run: bool,
    include_metadata: bool,
) -> None:
    """delete chunks and storage data

    delete chunk records and storage data tracked in the .firecube/ control
    plane. CAUTION: this operation is destructive and cannot be undone -- it
    removes both control-plane records and the underlying storage files. use
    --dry-run to preview deletions before committing"""
    if product_name and all_products:
        raise click.UsageError(
            "ConflictingScope: --product-name and --all-products are mutually exclusive. "
            "Provide exactly one."
        )
    if not product_name and not all_products:
        raise click.UsageError(
            "MissingScope: provide --product-name PRODUCT to target a specific product, "
            "or --all-products to target all products."
        )

    if all_products and not dry_run and not yes_i_really_mean_it:
        if not sys.stdin.isatty():
            raise click.UsageError(
                "Confirmation required: --all-products deletes across all products. "
                "Re-run with --yes-i-really-mean-it or run interactively."
            )
        click.confirm("Delete chunks across ALL products?", abort=True)

    if manifest_only and storage_only:
        raise click.ClickException("Cannot specify both --manifest-only and --storage-only")

    if include_metadata and not yes_i_really_mean_it:
        raise click.ClickException(
            "Metadata deletion requested. Re-run with --yes-i-really-mean-it to continue."
        )

    product_name, product_uri = resolve_cli_product(product_name)
    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    meta = parse_meta_filters(meta_filters) if meta_filters else None

    before_dt = parse_datetime(end_date)
    after_dt = parse_datetime(start_date)
    if date_range:
        try:
            start_str, end_str = date_range.split(",", 1)
        except ValueError as exc:
            raise click.ClickException(
                "Invalid date range format. Use YYYY-MM-DD,YYYY-MM-DD"
            ) from exc
        after_dt = parse_datetime(start_str.strip())
        before_dt = parse_datetime(end_str.strip())

    plans = []
    patterns = pattern or [None]
    for p in patterns:
        plan = manager.create_deletion_plan(
            pattern=p,
            product=product_name,
            before=before_dt,
            after=after_dt,
            chunk_type=chunk_type,
            include_metadata=include_metadata,
            meta=meta,
        )
        if plan.chunks:
            plans.append(plan)

    if not plans:
        click.echo("No chunks found matching criteria.")
        return

    all_chunks = []
    affected_products = set()
    all_manifests = set()
    for plan in plans:
        all_chunks.extend(plan.chunks)
        affected_products.update(plan.products_affected)
        all_manifests.update(plan.manifest_files)

    seen = set()
    unique_chunks = []
    for chunk in all_chunks:
        key = (chunk.product, chunk.key)
        if key not in seen:
            seen.add(key)
            unique_chunks.append(chunk)

    combined_plan = manager.create_deletion_plan()
    combined_plan.chunks = unique_chunks
    combined_plan.total_size = sum(chunk.size for chunk in unique_chunks)
    combined_plan.products_affected = affected_products
    combined_plan.manifest_files = all_manifests

    storage_cfg = None
    if not manifest_only:
        storage_cfg = storage_config_from_ctx(ctx)

    if (
        not yes_i_really_mean_it
        and not dry_run
        and not confirm_deletion(
            combined_plan, manifest_only=manifest_only, storage_only=storage_only
        )
    ):
        click.echo("Aborted.")
        return

    result = manager.execute_deletion(
        combined_plan,
        delete_storage=not manifest_only,
        delete_manifest=not storage_only,
        storage_config=storage_cfg,
        dry_run=dry_run,
    )

    if dry_run:
        click.echo("\nDRY RUN - Would delete:")
        click.echo(f"  Chunks: {result['would_delete_chunks']:,}")
        bytes_count = result["would_delete_size_bytes"]
        if bytes_count > 0:
            size_mb = bytes_count / (1024 * 1024)
            size_gb = bytes_count / (1024 * 1024 * 1024)
            click.echo(f"  Size: {size_gb:.1f} GB" if size_gb >= 1 else f"  Size: {size_mb:.1f} MB")
        products = result.get("products_affected", [])
        if products:
            click.echo(f"  Products: {', '.join(products)}")
        return

    click.echo(f"\nDeleted {result['deleted_chunks']:,} chunks")
    bytes_count = result["deleted_size_bytes"]
    if bytes_count > 0:
        size_mb = bytes_count / (1024 * 1024)
        size_gb = bytes_count / (1024 * 1024 * 1024)
        click.echo(f"Size: {size_gb:.1f} GB" if size_gb >= 1 else f"Size: {size_mb:.1f} MB")

    if result["storage_errors"]:
        click.echo(f"\nStorage errors: {len(result['storage_errors'])}")
        for error in result["storage_errors"][:5]:
            click.echo(f"  - {error}")

    if result["manifest_errors"]:
        click.echo(f"\nManifest errors: {len(result['manifest_errors'])}")
        for error in result["manifest_errors"]:
            click.echo(f"  - {error}")


@click.command(
    "delete-span",
    epilog="""\b
Examples:
  # preview span deletion for a product
  firecube chunks delete-span -n <product> --dry-run

\b
  # delete spans for a specific run
  firecube chunks delete-span -n <product> --run-id <run-id> --yes-i-really-mean-it

\b
  # force-delete non-aligned spans
  firecube chunks delete-span -n <product> --run-id <run-id> --force --yes-i-really-mean-it

See also: firecube chunks delete, firecube chunks runs list
""",
)
@click.option(
    "-n", "--product-name", "product_name", required=True, help="target product (e.g. product.zarr)"
)
@click.option("--run-id", help="filter span records by run_id")
@click.option("--batch-id", help="filter span records by batch_id")
@click.option("-g", "--group", "group_name", help="filter by group (e.g. F120)")
@click.option(
    "--meta",
    "meta_filters",
    multiple=True,
    help="filter by tracked span metadata key=value (value may be JSON)",
)
@click.option(
    "--include-replaced",
    is_flag=True,
    help="include spans already marked replaced (useful to retry deletions after a partial failure)",
)
@click.option("--workspace", type=click.Path(path_type=Path), help="workspace directory override")
@click.option(
    "--time-dim",
    "time_dim",
    help=(
        "time dimension name for cubes written with a custom time_dim_name; "
        "only needed when span records predate dim recording and the cube has "
        "no timestamp-state array (must match the cube layout)"
    ),
)
@click.option("--dry-run", is_flag=True, help="show what would be deleted without doing it")
@click.option("--force", is_flag=True, help="allow deletion even when spans are not chunk-aligned")
@click.option("--yes-i-really-mean-it", is_flag=True, help="skip confirmation prompts")
@click.pass_context
def delete_span_cmd(
    ctx: click.Context,
    product_name: str,
    run_id: str | None,
    batch_id: str | None,
    group_name: str | None,
    meta_filters,
    include_replaced: bool,
    workspace: Path | None,
    time_dim: str | None,
    dry_run: bool,
    force: bool,
    yes_i_really_mean_it: bool,
) -> None:
    """delete storage chunks from a span

    delete storage chunks described by specific tracked span records.
    removes data written as part of a particular ingestion span or run.
    CAUTION: data removed from storage cannot be recovered -- use --dry-run
    first"""
    product_name, product_uri = resolve_cli_product(product_name)
    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    meta = parse_meta_filters(meta_filters) if meta_filters else {}
    if run_id:
        meta["run_id"] = run_id
    if batch_id:
        meta["batch_id"] = batch_id
    if group_name:
        meta["group"] = group_name

    spans = manager.list_chunks(
        product=product_name,
        chunk_type="span",
        meta=meta or None,
        include_replaced=include_replaced,
    )

    if not spans:
        click.echo("No span records found matching criteria.")
        return

    if not yes_i_really_mean_it and not dry_run:
        click.echo(
            f"About to delete chunks for {len(spans):,} span records (product={product_name})."
        )
        if not click.confirm("Continue?"):
            click.echo("Aborted.")
            return

    try:
        result = manager.delete_spans(
            spans,
            dry_run=dry_run,
            force=force,
            update_manifest=not dry_run,
            update_state=not dry_run,
            time_dim_name=time_dim,
        )
    except (ManifestError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    if dry_run:
        click.echo(
            f"DRY RUN: would delete {result.get('deleted_keys', 0):,} chunk keys from storage "
            f"across {result.get('deleted_spans', 0):,} spans"
        )
    else:
        click.echo(
            f"Deleted {result.get('deleted_keys', 0):,} chunk keys from storage across "
            f"{result.get('deleted_spans', 0):,} spans"
        )

    errors = result.get("errors") or []
    if errors:
        click.echo(f"\nErrors: {len(errors)}")
        for err in errors[:10]:
            click.echo(f"  - {err}")
