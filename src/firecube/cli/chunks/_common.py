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
import re
from datetime import datetime

import click

_TIME_RANGE_SEPARATOR_RE = re.compile(r":(?=\d{4}-\d{2}-\d{2}(?:$|[T\s]))")


def confirm_deletion(plan, *, manifest_only: bool = False, storage_only: bool = False) -> bool:
    """Confirm a deletion operation (CLI-only)."""
    click.echo("")
    click.echo("DANGEROUS OPERATION")
    click.echo(f"About to delete {plan.count:,} tracked chunk records")
    if plan.total_size > 0:
        if plan.size_gb >= 1:
            click.echo(f"Total size: {plan.size_gb:.1f} GB")
        else:
            click.echo(f"Total size: {plan.size_mb:.1f} MB")
    if plan.products_affected:
        click.echo(f"Products affected: {', '.join(sorted(plan.products_affected))}")

    if manifest_only:
        click.echo("Mode: tracked-records-only (storage objects will be kept)")
    elif storage_only:
        click.echo("Mode: storage-only (tracked chunk records will be kept)")
    else:
        click.echo("Mode: delete from storage + tracked chunk records")

    return bool(click.confirm("Continue?", default=False))


def parse_meta_filters(values: tuple[str, ...]) -> dict:
    """Parse repeated --meta key=value filters (value may be JSON)."""
    meta: dict = {}
    for item in values:
        if "=" not in item:
            raise click.BadParameter(f"Invalid --meta '{item}', expected key=value")
        key, raw = item.split("=", 1)
        key = key.strip()
        raw = raw.strip()
        if not key:
            raise click.BadParameter(f"Invalid --meta '{item}', empty key")
        try:
            meta[key] = json.loads(raw)
        except Exception:
            meta[key] = raw
    return meta


def parse_datetime(value: str | None) -> datetime | None:
    """Parse a YYYY-MM-DD string into a datetime."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise click.BadParameter(f"Invalid date format: {value}. Use YYYY-MM-DD") from exc


def parse_time_range(value: str | None) -> tuple[str, str] | None:
    """Parse 'START:END' into (start_iso, end_iso). Returns None if value is None/empty."""
    if not value:
        return None
    parts = _TIME_RANGE_SEPARATOR_RE.split(value, maxsplit=1)
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
        raise click.BadParameter(f"Expected START:END format, got: {value!r}")
    return parts[0].strip(), parts[1].strip()
