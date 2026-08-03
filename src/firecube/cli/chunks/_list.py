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

import json
from pathlib import Path

import click

from ._common import parse_datetime, parse_meta_filters, parse_time_range
from ._manager import resolve_cli_product, resolve_manager


@click.command(
    "list",
    epilog="""\b
Examples:
  # list all chunks across all products
  firecube chunks list

\b
  # list chunks for a specific product
  firecube chunks list --product-name file:///data/products/MY_PRODUCT.zarr

\b
  # filter by date range
  firecube chunks list --start-date 2024-01-01 --end-date 2024-06-30

\b
  # output as JSON for scripting
  firecube chunks list --product-name file:///data/products/MY_PRODUCT.zarr -f json

See also: firecube chunks delete, firecube chunks runs list,
          firecube chunks snapshots status
""",
)
@click.option(
    "--pattern", multiple=True, help="glob pattern to match chunk keys (can specify multiple)"
)
@click.option("-n", "--product-name", "product_name", help="full product URI to inspect")
@click.option("--end-date", "end_date", help="show chunks created before date (YYYY-MM-DD)")
@click.option("--start-date", "start_date", help="show chunks created after date (YYYY-MM-DD)")
@click.option(
    "--time-range",
    "time_range",
    default=None,
    help="filter spans by time range START:END (ISO8601); matches spans overlapping the window",
)
@click.option("--type", "chunk_type", help="filter by chunk type (chunk, meta)")
@click.option(
    "--meta",
    "meta_filters",
    multiple=True,
    help="filter by tracked chunk metadata key=value (value may be JSON)",
)
@click.option(
    "--workspace",
    type=click.Path(path_type=Path),
    help="workspace directory override (local path)",
)
@click.option(
    "-f",
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="output format",
)
@click.option("--limit", type=int, help="limit number of results")
@click.option(
    "--include-span",
    is_flag=True,
    help="include span coverage payload (time_index_ranges, arrays, alignment) in output",
)
@click.pass_context
def list_cmd(
    ctx: click.Context,
    pattern,
    product_name,
    end_date,
    start_date,
    time_range,
    chunk_type,
    meta_filters,
    workspace,
    output_format,
    limit,
    include_span,
) -> None:
    """list tracked chunk records

    list chunk records tracked in the .firecube/ control plane for one or
    all products. filter by product, date range, span, or chunk type.
    supports table and JSON output for scripting"""
    product_name, product_uri = resolve_cli_product(product_name)
    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)

    before_dt = parse_datetime(end_date)
    after_dt = parse_datetime(start_date)
    time_range_dt = parse_time_range(time_range)
    meta = parse_meta_filters(meta_filters) if meta_filters else None

    if pattern:
        chunks = []
        for p in pattern:
            entries = manager.list_chunks(
                pattern=p,
                product=product_name,
                before=before_dt,
                after=after_dt,
                chunk_type=chunk_type,
                meta=meta,
                time_overlaps=time_range_dt,
            )
            chunks.extend(entries)
        deduped = []
        seen = set()
        for chunk in chunks:
            key = (chunk.product, chunk.key)
            if key not in seen:
                seen.add(key)
                deduped.append(chunk)
        all_chunks = deduped
    else:
        all_chunks = manager.list_chunks(
            product=product_name,
            before=before_dt,
            after=after_dt,
            chunk_type=chunk_type,
            meta=meta,
            time_overlaps=time_range_dt,
        )

    if limit:
        all_chunks = all_chunks[:limit]

    def _span_payload(chunk) -> dict | None:
        if not include_span:
            return None
        if not isinstance(chunk.record, dict):
            return None
        span = chunk.record.get("span")
        if not isinstance(span, dict):
            return None
        return span

    def _span_summary(chunk) -> str:
        _ = _span_payload(chunk)
        return str(chunk.timestamps_written) if chunk.timestamps_written else ""

    if output_format == "json":
        data = [
            {
                "product": chunk.product,
                "key": chunk.key,
                "type": chunk.chunk_type,
                "size": chunk.size,
                "size_mb": round(chunk.size_mb, 2),
                "timestamp": chunk.timestamp,
                "datetime": chunk.datetime.isoformat(),
                "manifest": str(chunk.manifest_path),
                "meta": chunk.meta,
                **({"span": span} if (span := _span_payload(chunk)) else {}),
            }
            for chunk in all_chunks
        ]
        click.echo(json.dumps(data, indent=2))
        return

    if output_format == "csv":
        if include_span:
            click.echo("product,key,type,size,size_mb,datetime,manifest,span")
        else:
            click.echo("product,key,type,size,size_mb,datetime,manifest")
        for chunk in all_chunks:
            if include_span:
                span_csv = json.dumps(_span_payload(chunk) or {}, separators=(",", ":"))
                click.echo(
                    f"{chunk.product},{chunk.key},{chunk.chunk_type},{chunk.size},"
                    f"{chunk.size_mb:.2f},{chunk.datetime.isoformat()},{chunk.manifest_path},{span_csv}"
                )
            else:
                click.echo(
                    f"{chunk.product},{chunk.key},{chunk.chunk_type},{chunk.size},"
                    f"{chunk.size_mb:.2f},{chunk.datetime.isoformat()},{chunk.manifest_path}"
                )
        return

    if not all_chunks:
        click.echo("No chunks found matching criteria.")
        return

    sample = all_chunks[:50]
    max_product = max(len(chunk.product) for chunk in sample)
    max_key = min(max(len(chunk.key) for chunk in sample), 60)

    def _meta_str(c) -> str:
        if not c.meta:
            return ""
        try:
            return json.dumps(c.meta, separators=(",", ":"))
        except TypeError:
            return str(c.meta)

    meta_samples = [_meta_str(c) for c in sample]
    max_meta = 0
    if any(meta_samples):
        max_meta = min(max(len(m) for m in meta_samples), 40)

    max_span = 0
    if include_span:
        span_samples = [_span_summary(c) for c in sample]
        if any(span_samples):
            max_span = min(max(len(s) for s in span_samples), 36)

    if max_meta > 0 and max_span > 0:
        click.echo(
            f"{'Product':<{max_product}} "
            f"{'Key':<{max_key}} "
            f"{'Type':<6} "
            f"{'Size (MB)':<10} "
            f"{'Date':<19} "
            f"{'Meta':<{max_meta}} "
            f"{'Span':<{max_span}}"
        )
        underline_len = max_product + max_key + 6 + 10 + 19 + max_meta + max_span + 6
        click.echo("-" * underline_len)
    elif max_meta > 0:
        click.echo(
            f"{'Product':<{max_product}} "
            f"{'Key':<{max_key}} "
            f"{'Type':<6} "
            f"{'Size (MB)':<10} "
            f"{'Date':<19} "
            f"{'Meta':<{max_meta}}"
        )
        underline_len = max_product + max_key + 6 + 10 + 19 + max_meta + 5
        click.echo("-" * underline_len)
    elif max_span > 0:
        click.echo(
            f"{'Product':<{max_product}} "
            f"{'Key':<{max_key}} "
            f"{'Type':<6} "
            f"{'Size (MB)':<10} "
            f"{'Date':<19} "
            f"{'Span':<{max_span}}"
        )
        underline_len = max_product + max_key + 6 + 10 + 19 + max_span + 5
        click.echo("-" * underline_len)
    else:
        click.echo(
            f"{'Product':<{max_product}} "
            f"{'Key':<{max_key}} "
            f"{'Type':<6} "
            f"{'Size (MB)':<10} "
            f"{'Date':<19}"
        )
        underline_len = max_product + max_key + 6 + 10 + 19 + 4
        click.echo("-" * underline_len)

    for chunk in all_chunks:
        key_display = chunk.key if len(chunk.key) <= max_key else f"{chunk.key[: max_key - 3]}..."
        size_mb = f"{chunk.size_mb:.1f}"
        date_str = chunk.datetime.strftime("%Y-%m-%d %H:%M:%S")
        span_text = _span_summary(chunk)
        if len(span_text) > max_span and max_span > 3:
            span_display = span_text[: max_span - 3] + "..."
        else:
            span_display = span_text

        if max_meta > 0 and max_span > 0:
            meta_text = _meta_str(chunk)
            if len(meta_text) > max_meta and max_meta > 3:
                meta_display = meta_text[: max_meta - 3] + "..."
            else:
                meta_display = meta_text
            click.echo(
                f"{chunk.product:<{max_product}} "
                f"{key_display:<{max_key}} "
                f"{chunk.chunk_type:<6} "
                f"{size_mb:<10} "
                f"{date_str:<19} "
                f"{meta_display:<{max_meta}} "
                f"{span_display:<{max_span}}"
            )
        elif max_meta > 0:
            meta_text = _meta_str(chunk)
            if len(meta_text) > max_meta and max_meta > 3:
                meta_display = meta_text[: max_meta - 3] + "..."
            else:
                meta_display = meta_text
            click.echo(
                f"{chunk.product:<{max_product}} "
                f"{key_display:<{max_key}} "
                f"{chunk.chunk_type:<6} "
                f"{size_mb:<10} "
                f"{date_str:<19} "
                f"{meta_display:<{max_meta}}"
            )
        elif max_span > 0:
            click.echo(
                f"{chunk.product:<{max_product}} "
                f"{key_display:<{max_key}} "
                f"{chunk.chunk_type:<6} "
                f"{size_mb:<10} "
                f"{date_str:<19} "
                f"{span_display:<{max_span}}"
            )
        else:
            click.echo(
                f"{chunk.product:<{max_product}} "
                f"{key_display:<{max_key}} "
                f"{chunk.chunk_type:<6} "
                f"{size_mb:<10} "
                f"{date_str}"
            )

    total_size_mb = sum(chunk.size_mb for chunk in all_chunks)
    products = {chunk.product for chunk in all_chunks}
    click.echo(f"\nSummary: {len(all_chunks):,} chunks, {total_size_mb:.1f} MB total")
    if len(products) > 1:
        click.echo(f"Products: {', '.join(sorted(products))}")
