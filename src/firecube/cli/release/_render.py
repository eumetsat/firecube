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

"""Text and JSON rendering of release records, listings and verifications."""

from __future__ import annotations

import datetime
import json
from collections.abc import Mapping
from typing import Any

import click

from firecube.core.controlplane.releases import ReleaseVerification
from firecube.ingestor.api import ReleaseRecord
from firecube.ingestor.runtime.release import ReleaseCreateResult, ReleaseListing


def _iso(timestamp: float) -> str:
    moment = datetime.datetime.fromtimestamp(timestamp, tz=datetime.UTC)
    return moment.isoformat(timespec="seconds")


def _plain(value: Any) -> Any:
    """Return *value* with every mapping, however nested, as a ``dict`` and every sequence as a ``list``."""
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def release_payload(record: ReleaseRecord) -> dict[str, Any]:
    """Return the JSON-ready fields of a release record.

    ``created_at`` keeps the stored Unix seconds, as ``snapshot`` JSON does with
    ``taken_at``; ``created_at_iso`` adds the same instant as an ISO 8601 UTC string.
    """
    return {
        "version": record.version,
        "created_at": record.created_at,
        "created_at_iso": _iso(record.created_at),
        "previous": record.previous,
        "snapshot": record.snapshot,
        "runs_since_previous": list(record.runs_since_previous),
        "producer": None if record.producer is None else _plain(record.producer),
        "producer_status": record.producer_status,
        "note": record.note,
        "created_by_run": record.created_by_run,
    }


def create_payload(result: ReleaseCreateResult) -> dict[str, Any]:
    """Return the JSON-ready fields of a release create result."""
    return {
        "outcome": result.outcome,
        "run_id": result.run_id,
        "release": release_payload(result.release),
        "documents": list(result.documents),
    }


def listing_payload(listing: ReleaseListing) -> dict[str, Any]:
    """Return the JSON-ready fields of a release listing."""
    return {
        "current": listing.current,
        "releases": [release_payload(record) for record in listing.releases],
    }


def verification_payload(verification: ReleaseVerification) -> dict[str, Any]:
    """Return the JSON-ready fields of a release verification."""
    return {
        "version": verification.version,
        "snapshot": verification.snapshot,
        "matches": verification.matches,
        "added": list(verification.added),
        "removed": list(verification.removed),
        "changed": list(verification.changed),
    }


def _producer_text(record: ReleaseRecord) -> str:
    if record.producer is None:
        return record.producer_status
    return f"{record.producer_status} {json.dumps(_plain(record.producer), sort_keys=True)}"


def echo_record(record: ReleaseRecord) -> None:
    """Print the fields of a release record, one per line."""
    click.echo(f"Release:      {record.version}")
    click.echo(f"Created:      {_iso(record.created_at)}")
    click.echo(f"Previous:     {record.previous if record.previous is not None else '-'}")
    click.echo(f"Snapshot:     {record.snapshot}")
    click.echo(f"Run:          {record.created_by_run}")
    click.echo(f"Producer:     {_producer_text(record)}")
    click.echo(f"Note:         {record.note if record.note is not None else '-'}")
    click.echo(f"Runs since:   {len(record.runs_since_previous)}")
    for run_id in record.runs_since_previous:
        click.echo(f"  {run_id}")


def echo_listing(listing: ReleaseListing) -> None:
    """Print one line per release, oldest first, and mark the current one."""
    if not listing.releases:
        click.echo("No releases")
        return
    for record in listing.releases:
        marker = "*" if record.version == listing.current else " "
        click.echo(
            f"{marker} {record.version}  {_iso(record.created_at)}  "
            f"snapshot={record.snapshot}  producer={record.producer_status}"
        )


def echo_verification(verification: ReleaseVerification) -> None:
    """Print whether the live documents match the release, then each differing path."""
    if verification.matches:
        click.echo(
            f"Release {verification.version}: metadata matches snapshot {verification.snapshot}"
        )
        return
    click.echo(
        f"Release {verification.version}: metadata differs from snapshot {verification.snapshot}"
    )
    for label, paths in (
        ("changed", verification.changed),
        ("added", verification.added),
        ("removed", verification.removed),
    ):
        for path in paths:
            click.echo(f"  {label}: {path}")
