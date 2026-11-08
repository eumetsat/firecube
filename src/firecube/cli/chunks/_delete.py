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
from typing import Any

import click

from firecube.cli._errors import render
from firecube.cli._exit_codes import ExitCode
from firecube.cli._shared_options import all_products_flag, target_uri_option
from firecube.core.controlplane.types import ChunkInfo, DeletionPlan

from ._common import confirm_deletion, parse_datetime, parse_meta_filters, parse_time_range
from ._manager import BoundProduct, require_one_scope, scoped_products, target_product

# The execute_deletion result keys summed, and joined, across the stores of one command.
_COUNTS = (
    "would_delete_chunks",
    "would_delete_size_bytes",
    "deleted_chunks",
    "deleted_size_bytes",
    "records_without_storage",
)
_LISTS = ("products", "storage_errors", "manifest_errors")


def _store_plan(
    product: BoundProduct,
    patterns: list[str | None],
    selection: dict[str, Any],
    *,
    include_metadata: bool,
) -> DeletionPlan | None:
    """Plan the deletion in one store, its keys unique across patterns; `None` when none match.

    Raises:
        UnparseableSpanBoundsError: A `--time-range` filter meets a span whose bounds do not parse.
    """
    chunks: list[ChunkInfo] = []
    manifests: set[str] = set()
    seen: set[str] = set()
    for p in patterns:
        if selection["time_overlaps"] is not None:
            # The deletion plan leaves unparseable spans out silently; this query makes
            # the same selection, date window included, but refuses them.
            product.manager.list_chunks(
                pattern=p, product=product.label, refuse_unparseable=True, **selection
            )
        plan = product.manager.create_deletion_plan(
            pattern=p, product=product.label, include_metadata=include_metadata, **selection
        )
        manifests.update(plan.manifest_files)
        for chunk in plan.chunks:
            if chunk.key not in seen:
                seen.add(chunk.key)
                chunks.append(chunk)
    if not chunks:
        return None
    return DeletionPlan(
        chunks=chunks,
        total_size=sum(chunk.size for chunk in chunks),
        products_affected={product.label},
        manifest_files=manifests,
    )


def _no_match_message(*, record_time_options: list[str], time_range: str | None) -> str:
    """Explain an empty match in terms of the time filter the user passed.

    Record-time filters (``--range``, ``--start-date``, ``--end-date``) and the
    data-time filter (``--time-range``) select on different axes, so a miss on
    one is answered by pointing at the other. With both or neither in play no
    axis note applies.
    """
    base = "No chunks found matching the given criteria."
    if time_range and not record_time_options:
        return (
            f"{base} Note: --time-range filters by data time (span coverage); "
            "use --range, --start-date, or --end-date for record time."
        )
    if record_time_options and not time_range:
        names = "/".join(record_time_options)
        verb = "filters" if len(record_time_options) == 1 else "filter"
        return f"{base} Note: {names} {verb} by record time; use --time-range for data time."
    return base


def _echo_records_without_storage(count: int) -> None:
    """Say that span, run and schema records keep their data in storage."""
    if count:
        click.echo(
            f"Records without storage objects: {count:,} (their data stays in storage; "
            "use firecube chunks delete-span to delete span data)"
        )


@click.command(
    "delete",
    epilog="""\b
Examples:
  # preview what would be deleted (always do this first)
  firecube chunks delete --target file:///data/products/MY_PRODUCT.zarr --dry-run

\b
  # delete all chunks for a product
  firecube chunks delete --target file:///data/products/MY_PRODUCT.zarr --yes-i-really-mean-it

\b
  # delete chunks in a record-time range
  firecube chunks delete --target file:///data/products/MY_PRODUCT.zarr --range 2024-01-01,2024-03-31 --dry-run

\b
  # delete spans overlapping a data-time range
  firecube chunks delete --target file:///data/products/MY_PRODUCT.zarr --time-range 2024-01-01:2024-03-31 --dry-run

\b
  # preview deletions across all products under the [storage] base URI
  firecube chunks delete --all-products --dry-run

\b
  # delete across all products (requires explicit confirmation)
  firecube chunks delete --all-products --yes-i-really-mean-it

See also: firecube chunks list, firecube chunks delete-span, firecube chunks compact rebuild
""",
)
@click.option(
    "--pattern", multiple=True, help="glob pattern to match chunk keys (can specify multiple)"
)
@target_uri_option("inspect", required=False)
@all_products_flag
@click.option(
    "--end-date",
    "end_date",
    help="delete chunks with record time before date (YYYY-MM-DD)",
)
@click.option(
    "--start-date",
    "start_date",
    help="delete chunks with record time after date (YYYY-MM-DD)",
)
@click.option(
    "--range",
    "date_range",
    help=(
        "delete chunks in a record-time range (YYYY-MM-DD,YYYY-MM-DD; "
        "use --time-range for data time)"
    ),
)
@click.option(
    "--time-range",
    "time_range",
    default=None,
    help="spans overlapping data-time range START:END (ISO 8601)",
)
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
    target: str | None,
    all_products: bool,
    end_date,
    start_date,
    time_range,
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
    require_one_scope(target, all_products)

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

    meta = parse_meta_filters(meta_filters) if meta_filters else None

    before_dt = parse_datetime(end_date)
    after_dt = parse_datetime(start_date)
    time_range_dt = parse_time_range(time_range) if time_range is not None else None
    if date_range:
        try:
            start_str, end_str = date_range.split(",", 1)
        except ValueError as exc:
            raise click.ClickException(
                "Invalid date range format. Use YYYY-MM-DD,YYYY-MM-DD"
            ) from exc
        after_dt = parse_datetime(start_str.strip())
        before_dt = parse_datetime(end_str.strip())

    selection: dict[str, Any] = {
        "before": before_dt,
        "after": after_dt,
        "chunk_type": chunk_type,
        "meta": meta,
        "time_overlaps": time_range_dt,
    }
    planned: list[tuple[BoundProduct, DeletionPlan]] = []
    for product in scoped_products(ctx, workspace, target, all_products, product_filesystem=True):
        plan = _store_plan(product, pattern or [None], selection, include_metadata=include_metadata)
        if plan is not None:
            planned.append((product, plan))

    if not planned:
        click.echo(
            _no_match_message(
                record_time_options=[
                    name
                    for name, value in (
                        ("--range", date_range),
                        ("--start-date", start_date),
                        ("--end-date", end_date),
                    )
                    if value
                ],
                time_range=time_range,
            )
        )
        return

    def display(product: BoundProduct) -> str:
        return f"{product.label} ({product.uri.to_str()})" if all_products else product.label

    summary = DeletionPlan(
        chunks=[chunk for _product, plan in planned for chunk in plan.chunks],
        total_size=sum(plan.total_size for _product, plan in planned),
        products_affected={display(product) for product, _plan in planned},
        manifest_files=set(),
    )
    if (
        not yes_i_really_mean_it
        and not dry_run
        and not confirm_deletion(summary, manifest_only=manifest_only, storage_only=storage_only)
    ):
        click.echo("Aborted.")
        return

    result: dict[str, Any] = dict.fromkeys(_COUNTS, 0) | {key: [] for key in _LISTS}
    for product, plan in planned:
        # The filesystem bound to the product deletes its objects; no [storage] base is used.
        outcome = product.manager.execute_deletion(
            plan,
            delete_storage=not manifest_only,
            delete_manifest=not storage_only,
            storage_config=None,
            dry_run=dry_run,
        )
        for key in _COUNTS:
            result[key] += outcome.get(key, 0)
        for key in _LISTS:
            result[key].extend(outcome.get(key, []))
        if outcome.get("products_affected"):
            result["products"].append(display(product))

    if dry_run:
        click.echo("\nDRY RUN - Would delete:")
        click.echo(f"  Chunks: {result['would_delete_chunks']:,}")
        bytes_count = result["would_delete_size_bytes"]
        if bytes_count > 0:
            size_mb = bytes_count / (1024 * 1024)
            size_gb = bytes_count / (1024 * 1024 * 1024)
            click.echo(f"  Size: {size_gb:.1f} GB" if size_gb >= 1 else f"  Size: {size_mb:.1f} MB")
        if result["products"]:
            click.echo(f"  Products: {', '.join(result['products'])}")
        if not manifest_only:
            _echo_records_without_storage(result["records_without_storage"])
        return

    click.echo(f"\nDeleted {result['deleted_chunks']:,} chunks")
    bytes_count = result["deleted_size_bytes"]
    if bytes_count > 0:
        size_mb = bytes_count / (1024 * 1024)
        size_gb = bytes_count / (1024 * 1024 * 1024)
        click.echo(f"Size: {size_gb:.1f} GB" if size_gb >= 1 else f"Size: {size_mb:.1f} MB")
    if not manifest_only:
        _echo_records_without_storage(result["records_without_storage"])

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
  firecube chunks delete-span --target file:///data/products/MY_PRODUCT.zarr --dry-run

\b
  # delete spans for a specific run
  firecube chunks delete-span --target file:///data/products/MY_PRODUCT.zarr --run-id <run-id> --yes-i-really-mean-it

\b
  # force-delete non-aligned spans
  firecube chunks delete-span --target file:///data/products/MY_PRODUCT.zarr --run-id <run-id> --force --yes-i-really-mean-it

See also: firecube chunks delete, firecube chunks runs list
""",
)
@target_uri_option("inspect")
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
    target: str,
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
    product = target_product(ctx, workspace, target)
    manager, product_name = product.manager, product.label
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
            yes_i_really_mean_it=yes_i_really_mean_it,
            update_manifest=not dry_run,
            update_state=not dry_run,
            time_dim_name=time_dim,
        )
    except ValueError as exc:
        raise click.ClickException(render(exc)) from exc

    if dry_run:
        if result.get("region_filled_spans", 0):
            click.echo(
                f"DRY RUN: would NaN-fill {result.get('region_filled_spans', 0):,} spans "
                f"and delete {result.get('deleted_keys', 0):,} chunk keys from storage"
            )
        else:
            click.echo(
                f"DRY RUN: would delete {result.get('deleted_keys', 0):,} chunk keys from storage "
                f"across {result.get('deleted_spans', 0):,} spans"
            )
    else:
        if result.get("region_filled_spans", 0):
            click.echo(
                f"NaN-filled {result.get('region_filled_spans', 0):,} spans and deleted "
                f"{result.get('deleted_keys', 0):,} chunk keys from storage"
            )
        else:
            click.echo(
                f"Deleted {result.get('deleted_keys', 0):,} chunk keys from storage across "
                f"{result.get('deleted_spans', 0):,} spans"
            )

    warnings_out = result.get("warnings") or []
    if warnings_out:
        click.echo(f"\nWarnings: {len(warnings_out)}")
        for w in warnings_out[:10]:
            click.echo(f"  - {render(w)}")

    collateral = result.get("collateral_spans") or []
    if collateral and not dry_run:
        click.echo(f"\nCollateral spans marked replaced: {len(collateral)}")
        for key in collateral[:10]:
            click.echo(f"  - {key}")

    errors = result.get("errors") or []
    if errors:
        click.echo(f"\nErrors: {len(errors)}")
        for err in errors[:10]:
            click.echo(f"  - {render(err)}")
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED)
