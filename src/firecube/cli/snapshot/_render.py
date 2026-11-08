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

"""Text and JSON rendering of snapshot records, snapshots and diffs."""

from __future__ import annotations

import datetime
import json
from collections.abc import Mapping
from typing import Any

import click

from firecube.core.controlplane.metadata_snapshots import (
    ManifestEntry,
    NodeDiff,
    Snapshot,
    SnapshotDiff,
    SnapshotPlan,
    SnapshotRecord,
)
from firecube.core.zarr.metadata_documents import ABSENT

_MAX_VALUE_CHARS = 120
_DIGEST_SHOWN = len("sha256:") + 12


def echo_json(payload: Any) -> None:
    """Print *payload* as JSON with sorted keys."""
    click.echo(json.dumps(payload, indent=2, sort_keys=True))


def _iso(timestamp: float) -> str:
    moment = datetime.datetime.fromtimestamp(timestamp, tz=datetime.UTC)
    return moment.isoformat(timespec="seconds")


def record_payload(record: SnapshotRecord) -> dict[str, Any]:
    """Return the JSON-ready fields of a snapshot record."""
    return {
        "snapshot_id": record.snapshot_id,
        "parent": record.parent,
        "manifest": record.manifest,
        "run_id": record.run_id,
        "trigger": record.trigger,
        "taken_at": record.taken_at,
        "consistency": record.consistency,
        "included_kinds": list(record.included_kinds),
    }


def _entry_payload(entry: ManifestEntry) -> dict[str, Any]:
    return {
        "path": entry.path,
        "digest": entry.digest,
        "size": entry.size,
        "kind": entry.kind.value,
    }


def snapshot_payload(snapshot: Snapshot) -> dict[str, Any]:
    """Return the JSON-ready fields of a snapshot: its record and its documents."""
    return {
        **record_payload(snapshot.record),
        "documents": [_entry_payload(entry) for entry in snapshot.documents],
    }


def _pair_payload(pair: tuple[Any, Any]) -> dict[str, Any]:
    """Return ``{"before": ..., "after": ...}``; the key of an absent side is left out."""
    before, after = pair
    payload: dict[str, Any] = {}
    if before is not ABSENT:
        payload["before"] = before
    if after is not ABSENT:
        payload["after"] = after
    return payload


def _pairs_payload(pairs: Mapping[str, tuple[Any, Any]]) -> dict[str, Any]:
    return {key: _pair_payload(pair) for key, pair in pairs.items()}


def _node_payload(node: NodeDiff) -> dict[str, Any]:
    return {
        "path": node.path,
        "attributes_added": dict(node.attributes_added),
        "attributes_removed": dict(node.attributes_removed),
        "attributes_changed": _pairs_payload(node.attributes_changed),
        "dimension_names": None
        if node.dimension_names is None
        else _pair_payload(node.dimension_names),
        "fill_value": None if node.fill_value is None else _pair_payload(node.fill_value),
        "physical": _pairs_payload(node.physical),
        "extensions": _pairs_payload(node.extensions),
    }


def diff_payload(diff: SnapshotDiff) -> dict[str, Any]:
    """Return the JSON-ready fields of a diff; a side a key lacks has no ``before`` or ``after``."""
    return {
        "before": diff.before,
        "after": diff.after,
        "added": list(diff.added),
        "removed": list(diff.removed),
        "changed": list(diff.changed),
        "nodes": [_node_payload(node) for node in diff.nodes],
        "unparsed": list(diff.unparsed),
    }


def echo_record(record: SnapshotRecord) -> None:
    """Print the fields of a snapshot record, one per line."""
    click.echo(f"Snapshot:     {record.snapshot_id}")
    click.echo(f"Parent:       {record.parent or '-'}")
    click.echo(f"Manifest:     {record.manifest}")
    click.echo(f"Taken:        {_iso(record.taken_at)}")
    click.echo(f"Consistency:  {record.consistency}")
    click.echo(f"Trigger:      {record.trigger}")
    click.echo(f"Run:          {record.run_id or '-'}")


def plan_payload(plan: SnapshotPlan, *, label_recorded: bool) -> dict[str, Any]:
    """Return the JSON-ready fields of a snapshot plan, marked as a dry run."""
    return {
        "dry_run": True,
        "parent": plan.parent,
        "manifest": plan.manifest,
        "documents": plan.documents,
        "blobs": plan.blobs,
        "manifest_recorded": plan.manifest_recorded,
        "label_recorded": label_recorded,
    }


def echo_plan(plan: SnapshotPlan, *, label: str, label_recorded: bool) -> None:
    """Print what a snapshot would write, one field per line."""
    click.echo("Dry run: nothing is written.")
    click.echo(f"Label:        {label} ({'recorded' if label_recorded else 'would be recorded'})")
    click.echo(f"Documents:    {plan.documents}")
    click.echo(f"Parent:       {plan.parent or '-'}")
    click.echo(f"Manifest:     {plan.manifest}")
    if plan.manifest_recorded:
        click.echo("Would write:  one record and refs/latest.json; the manifest is recorded")
    else:
        click.echo(
            f"Would write:  up to {plan.blobs} blobs, one manifest, one record and "
            "refs/latest.json; an existing blob is kept"
        )


def echo_record_line(record: SnapshotRecord) -> None:
    """Print one line for a record: id, time, consistency, trigger and run."""
    click.echo(
        f"{record.snapshot_id}  {_iso(record.taken_at)}  {record.consistency}  "
        f"{record.trigger}  run={record.run_id or '-'}"
    )


def echo_snapshot(snapshot: Snapshot) -> None:
    """Print a record followed by its documents."""
    echo_record(snapshot.record)
    click.echo(f"Documents:    {len(snapshot.documents)}")
    for entry in snapshot.documents:
        click.echo(
            f"  {entry.path}  {entry.kind.value}  {entry.size} B  {entry.digest[:_DIGEST_SHOWN]}"
        )


def _value_text(value: Any) -> str:
    if value is ABSENT:
        return "(absent)"
    text = json.dumps(value, sort_keys=True)
    if len(text) > _MAX_VALUE_CHARS:
        return text[: _MAX_VALUE_CHARS - 3] + "..."
    return text


def _echo_pair(label: str, pair: tuple[Any, Any]) -> None:
    click.echo(f"    {label}: {_value_text(pair[0])} -> {_value_text(pair[1])}")


def _echo_node(node: NodeDiff) -> None:
    click.echo(f"  Node: {node.path or '(root)'}")
    for key, value in node.attributes_added.items():
        click.echo(f"    attribute added: {key} = {_value_text(value)}")
    for key, value in node.attributes_removed.items():
        click.echo(f"    attribute removed: {key} (was {_value_text(value)})")
    for key, pair in node.attributes_changed.items():
        _echo_pair(f"attribute changed: {key}", pair)
    if node.dimension_names is not None:
        _echo_pair("dimension_names", node.dimension_names)
    if node.fill_value is not None:
        _echo_pair("fill_value", node.fill_value)
    for key, pair in node.physical.items():
        _echo_pair(f"physical: {key}", pair)
    for key, pair in node.extensions.items():
        _echo_pair(f"extension: {key}", pair)


def echo_diff(diff: SnapshotDiff) -> None:
    """Print the path-level and the parsed differences of two snapshots."""
    click.echo(f"Before:  {diff.before}")
    click.echo(f"After:   {diff.after}")
    if not (diff.added or diff.removed or diff.changed or diff.nodes or diff.unparsed):
        click.echo("No differences")
        return
    for sign, paths in (("+", diff.added), ("-", diff.removed), ("~", diff.changed)):
        for path in paths:
            click.echo(f"{sign} {path}")
    if diff.nodes:
        click.echo("Nodes:")
        for node in diff.nodes:
            _echo_node(node)
    if diff.unparsed:
        click.echo("Not parsed (path-level diff only):")
        for path in diff.unparsed:
            click.echo(f"  {path}")
