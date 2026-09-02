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

"""Metadata seeding for staged-write mode.

When firecube writes through a local temp store before publishing to
the final target (staged mode), the temp store starts empty and has no
Zarr metadata.  This module copies zarr.json files from the existing
final target into the temp store so that ``append_time_groups()`` reads
the correct write cursor and shape — preventing metadata corruption where
temp-store shape overwrites the cumulative target shape.

Best-effort for fresh ingests (no final target yet); strict (raises
``StagedMetadataError``) when resuming into an existing target.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from firecube.core.storage.uri import StorageUri
from firecube.ingestor.errors import StagedMetadataError

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession

log = logging.getLogger(__name__)


def seed_staged_store_metadata(
    *,
    temp_store_uri: str,
    final_target_uri: str,
    groups: list[str] | None = None,
    strict: bool = True,
    session: StorageSession,
    coordinate_arrays: list[str] | None = None,
) -> dict[str, Any]:
    """Copy zarr.json metadata files (and optionally named coordinate-array chunks) from final_target_uri into temp_store_uri.

    Seeds the temp store with correct shape/dimension metadata so that
    append_time_groups() starts write_cursor at the right position. By
    default copies only zarr.json files (metadata).

    When ``coordinate_arrays`` is a non-empty list of coord-array names,
    also copies the chunk payloads of those arrays in every discovered
    group that contains them. Used by the runtime hook to seed the
    workspace timestamp coordinate so that plugin value-based
    timestamp resolvers see real values instead of NaT on staged
    re-ingest. Coordinate arrays missing in a group are tolerated (no-op).
    Never copies data-array chunks.

    All metadata reads from the final target use the driver-correct
    ``session.fs()`` filesystem; ``session`` is REQUIRED.

    When ``strict=True`` (default), unexpected exceptions are wrapped as
    ``StagedMetadataError`` so callers that catch ``StagedMetadataError``
    propagate hard failures while ``except Exception`` debug-suppressors do
    not silently swallow them.

    ``coordinate_arrays=None`` and ``coordinate_arrays=[]`` are
    semantically identical and both preserve exact legacy metadata-only
    behavior.

    Returns {group: {"seeded": bool, "files": int}} for logging.
    """
    if groups is None:
        groups = _discover_zarr_groups(session, final_target_uri)

    results: dict[str, Any] = {}

    for group in groups:
        results[group] = {"seeded": False, "files": 0}
        try:
            _seed_group_via_session(
                session=session,
                temp_store_uri=temp_store_uri,
                final_target_uri=final_target_uri,
                group=group,
                strict=strict,
                results=results,
                coordinate_arrays=coordinate_arrays,
            )

            if results[group]["files"] > 0:
                results[group]["seeded"] = True
                log.debug(
                    "Seeded %d %s for group %s",
                    results[group]["files"],
                    "metadata + coord files" if coordinate_arrays else "zarr.json files",
                    group,
                )
        except StagedMetadataError:
            raise
        except Exception as e:
            if strict:
                raise StagedMetadataError(
                    f"Staged metadata seeding failed unexpectedly for group {group}: {e}"
                ) from e
            log.warning("Staged metadata seeding skipped for group %s: %s", group, e)

    return results


def _discover_zarr_groups(session: StorageSession, uri: str) -> list[str]:
    """Discover top-level Zarr groups under ``uri`` from existing metadata."""
    from firecube.core.uris import is_remote_target, local_path_from_target

    fs = session.fs()
    if is_remote_target(uri):
        final_uri = StorageUri.parse(uri)
    else:
        final_uri = StorageUri.from_local_path(local_path_from_target(uri))

    if not fs.exists(final_uri):
        return []

    final_root = final_uri.path.rstrip("/")
    groups: set[str] = set()
    for entry in fs.find(final_uri):
        entry_path = entry.path.rstrip("/")
        if not (entry_path.endswith("/zarr.json") or entry_path.endswith("/.zgroup")):
            continue
        rel = (
            entry_path[len(final_root) + 1 :]
            if entry_path.startswith(f"{final_root}/")
            else entry_path.lstrip("/")
        )
        parent = rel.rsplit("/", 1)[0] if "/" in rel else ""
        if not parent:
            continue
        groups.add(parent.split("/", 1)[0])

    return sorted(groups)


def _is_coordinate_chunk(path: str, coords: list[str] | None) -> bool:
    """Return True if ``path`` is a chunk file under a named coord array.

    Uses path-segment matching: splits on "/" and looks for the exact
    sequence ``<coord>/c`` where ``<coord>`` is in ``coords``. Returns
    False for the coord's ``zarr.json`` (already covered by the
    metadata filter) and for arrays whose names contain a coord name
    as a substring (e.g. ``timestamp_bounds`` for ``["timestamp"]``).

    Args:
        path: The entry path relative to or under the group root.
              Trailing slashes are stripped.
        coords: Coordinate array names to match (exact path segments).
                ``None`` and ``[]`` always return False (legacy mode).

    Examples:
        _is_coordinate_chunk("data/timestamp/c/0", ["timestamp"]) -> True
        _is_coordinate_chunk("data/timestamp/c/0/0", ["timestamp"]) -> True
        _is_coordinate_chunk("data/timestamp/zarr.json", ["timestamp"]) -> False
        _is_coordinate_chunk("data/timestamp_bounds/c/0", ["timestamp"]) -> False
        _is_coordinate_chunk("data/val/c/0", ["timestamp"]) -> False
        _is_coordinate_chunk("data/timestamp/c/0", []) -> False
        _is_coordinate_chunk("data/timestamp/c/0", None) -> False
    """
    if not coords:
        return False
    parts = path.rstrip("/").split("/")
    return any(parts[i] in coords and parts[i + 1] == "c" for i in range(len(parts) - 2))


def _seed_group_via_session(
    *,
    session: StorageSession,
    temp_store_uri: str,
    final_target_uri: str,
    group: str,
    strict: bool,
    results: dict[str, Any],
    coordinate_arrays: list[str] | None = None,
) -> None:
    from firecube.core.uris import is_remote_target, local_path_from_target

    fs = session.fs()
    if is_remote_target(final_target_uri):
        final_uri = StorageUri.parse(final_target_uri)
    else:
        final_uri = StorageUri.from_local_path(local_path_from_target(final_target_uri))
    group_uri = final_uri.join(group.strip("/"))

    if not fs.exists(group_uri):
        return  # Fresh ingest — no final target yet

    all_entries = fs.find(group_uri)
    final_root = final_uri.path.rstrip("/")
    files_to_copy = [
        entry
        for entry in all_entries
        if entry.path.endswith("zarr.json")
        or _is_coordinate_chunk(
            entry.path[len(final_root) + 1 :]
            if entry.path.startswith(f"{final_root}/")
            else entry.path.lstrip("/"),
            coordinate_arrays,
        )
    ]
    if not files_to_copy:
        return

    temp_root = local_path_from_target(temp_store_uri)
    for src_uri in files_to_copy:
        rel = (
            src_uri.path[len(final_root) + 1 :]
            if src_uri.path.startswith(f"{final_root}/")
            else src_uri.path.lstrip("/")
        )
        dst_path = temp_root / rel
        if dst_path.exists():
            results[group]["files"] += 1
            continue
        try:
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            with fs.open(src_uri, "rb") as fh:
                raw = fh.read()
            if rel.endswith("zarr.json"):
                payload = json.loads(raw)
                if payload.get("node_type") == "array":
                    attrs = payload.get("attributes")
                    if isinstance(attrs, dict):
                        # RUN-STATE attrs (stripped on seed): only ``firecube_static_written``
                        # today. If a second run-state reserved attr is added later, refactor
                        # _reserved_attrs.py into RESERVED_SHAPE_ATTRS (kept on seed) vs
                        # RESERVED_RUN_STATE_ATTRS (stripped on seed) and switch this strip
                        # from a literal key to a set membership check.
                        # SHAPE attrs (preserved on seed) include ``firecube_preallocated``,
                        # ``firecube_coord_managed``, ``firecube_consolidated_at``, and
                        # ``firecube_group_identity_hash``: they encode contract shape
                        # negotiated at preallocate time and MUST survive staged seeding so
                        # ingest startup can verify per-group identity on the temp store.
                        attrs.pop("firecube_static_written", None)
                dst_path.write_bytes(json.dumps(payload).encode())
            else:
                dst_path.write_bytes(raw)
            results[group]["files"] += 1
        except Exception as e:
            if strict:
                raise StagedMetadataError(
                    f"Failed to seed staged metadata {src_uri.to_str()} -> {dst_path}: {e}"
                ) from e
            log.warning("Failed to seed staged metadata %s: %s", src_uri.to_str(), e)
