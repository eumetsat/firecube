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
from typing import TYPE_CHECKING, Any

import click

from firecube.core.controlplane.releases import ReleaseVerification
from firecube.ingestor.api import ReleaseRecord
from firecube.ingestor.runtime.release import ReleaseCreateResult, ReleaseListing, ReleasePlan

if TYPE_CHECKING:
    from firecube.ingestor.runtime.release_retention import RetentionVerification

_RETENTION_FIELDS = (
    ("kind", "Kind"),
    ("uri", "Archive"),
    ("mode", "Mode"),
    ("previous_release", "Previous release"),
    ("chain_length", "Chain length"),
    ("deleted_key_count", "Deleted keys"),
    ("size_bytes", "Size (bytes)"),
    ("digest", "Digest"),
    ("bucket_versioning", "Bucket versioning"),
    ("object_count", "Objects"),
    ("delete_marker_count", "Delete markers"),
    ("inventory", "Inventory"),
    ("created_by_run", "Run"),
)


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
        "retention": None if record.retention is None else _plain(record.retention),
    }


def create_payload(result: ReleaseCreateResult) -> dict[str, Any]:
    """Return the JSON-ready fields of a release create result."""
    return {
        "outcome": result.outcome,
        "run_id": result.run_id,
        "release": release_payload(result.release),
        "documents": list(result.documents),
    }


def plan_payload(plan: ReleasePlan) -> dict[str, Any]:
    """Return the JSON-ready fields of a release create plan, marked as a dry run."""
    return {
        "dry_run": True,
        "outcome": plan.outcome,
        "version": plan.version,
        "previous": plan.previous,
        "note": plan.note,
        "documents": list(plan.documents),
        "retention": None if plan.retention is None else _plain(plan.retention),
    }


_PLAN_OUTCOMES = {
    "create": "record the release and make it current",
    "resume": "make the recorded release current",
    "already-current": "nothing; the release is current",
}


def echo_plan(plan: ReleasePlan) -> None:
    """Print what a release create would write, one field per line."""
    click.echo("Dry run: nothing is written.")
    click.echo(f"Release:      {plan.version}")
    click.echo(f"Would:        {_PLAN_OUTCOMES[plan.outcome]}")
    click.echo(f"Previous:     {plan.previous if plan.previous is not None else '-'}")
    click.echo(f"Note:         {plan.note if plan.note is not None else '-'}")
    click.echo(f"Documents:    {', '.join(plan.documents) or '-'}")
    _echo_retention_fields(plan.retention)
    if plan.outcome == "create" and plan.retention and "object_count" in plan.retention:
        click.echo("  (current-cut counts; the run also retains the documents it writes)")


def listing_payload(listing: ReleaseListing) -> dict[str, Any]:
    """Return the JSON-ready fields of a release listing."""
    return {
        "current": listing.current,
        "releases": [release_payload(record) for record in listing.releases],
    }


def retention_check_payload(check: RetentionVerification) -> dict[str, Any]:
    """Return the JSON-ready fields of a retention check.

    ``kind`` and ``holds`` are always present; every other field is present only when the
    check of that kind reports it.
    """
    payload: dict[str, Any] = {"kind": check.kind, "holds": check.holds}
    for name in (
        "archive",
        "backup_chain",
        "broken_at",
        "retention",
        "lifecycle",
        "inventory",
        "lost",
        "checked",
        "checked_count",
    ):
        value = getattr(check, name)
        if value is not None:
            payload[name] = value
    if check.lost_keys:
        payload["lost_keys"] = list(check.lost_keys)
    return payload


def verification_payload(
    verification: ReleaseVerification, check: RetentionVerification
) -> dict[str, Any]:
    """Return the JSON-ready fields of a release verification and its retention check."""
    return {
        "version": verification.version,
        "snapshot": verification.snapshot,
        "matches": verification.matches,
        "added": list(verification.added),
        "removed": list(verification.removed),
        "changed": list(verification.changed),
        "retention": retention_check_payload(check),
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
    _echo_retention_fields(record.retention)


def _echo_retention_fields(retention: Mapping[str, Any] | None) -> None:
    """Print the ``Retention:`` block: one line per field the mapping holds, or ``none``."""
    if retention is None:
        click.echo("Retention:    none")
        return
    click.echo("Retention:")
    for key, label in _RETENTION_FIELDS:
        value = retention.get(key)
        if value is not None:
            click.echo(f"  {label + ':':<19}{value}")


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


def echo_verification(verification: ReleaseVerification, check: RetentionVerification) -> None:
    """Print whether the live documents match the release, each differing path, then the retention lines."""
    if verification.matches:
        click.echo(
            f"Release {verification.version}: metadata matches snapshot {verification.snapshot}"
        )
    else:
        click.echo(
            f"Release {verification.version}: metadata differs from snapshot "
            f"{verification.snapshot}"
        )
        for label, paths in (
            ("changed", verification.changed),
            ("added", verification.added),
            ("removed", verification.removed),
        ):
            for path in paths:
                click.echo(f"  {label}: {path}")
    _echo_retention_check(check)


def _echo_retention_check(check: RetentionVerification) -> None:
    """Print the archive and backup chain lines of a zip release, or the version lines of s3."""
    if check.kind is None:
        return
    if check.archive is not None:
        click.echo(f"archive: {check.archive}")
        if check.backup_chain == "broken":
            click.echo(f"backup chain: broken at {check.broken_at}")
        else:
            click.echo(f"backup chain: {check.backup_chain}")
        return
    click.echo(f"retention: {check.retention}")
    click.echo(f"lifecycle: {check.lifecycle}")
    click.echo("inventory: " + (f"lost {check.lost}" if check.inventory == "lost" else "intact"))
    for key in check.lost_keys:
        click.echo(f"  lost: {key}")
    click.echo("checked: " + ("all" if check.checked == "all" else f"sample {check.checked_count}"))
