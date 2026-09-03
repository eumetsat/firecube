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

"""``firecube zarr consolidate-time-coord``: densify legacy per-slot time coordinates."""

from __future__ import annotations

import os
import shutil
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
import numpy as np

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import wrap_user_facing_errors
from firecube.cli._product import require_full_uri, resolve_product_identity
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_product_uri,
    validate_uri_storage_coherence,
)
from firecube.core.api import (
    ATTR_CONSOLIDATED_AT,
    ATTR_COORD_MANAGED,
    ATTR_PREALLOCATED,
    assert_coord_markers_consistent,
)
from firecube.core.controlplane import ChunkManager
from firecube.core.errors import SchemaDriftError
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.uris import local_path_from_target
from firecube.core.zarr._coord_chunks import resolve_coord_chunks
from firecube.core.zarr.coord_materialization import (
    coord_to_datetime64,
)


@click.command(
    "consolidate-time-coord",
    epilog="""\b
Examples:
  # Preview consolidation on an existing cube with chunked time coordinates
  firecube zarr consolidate-time-coord --target file:///tmp/x.zarr \
      --product-name x.zarr --storage-type local --storage-driver fsspec --dry-run

See also: firecube zarr preallocate, firecube zarr validate
""",
)
@click.option(
    "--target",
    required=True,
    help="URI of the Zarr cube to consolidate.",
)
@click.option("--product-name", "product_name", required=True, help="Logical product name.")
@click.option(
    "--storage-type",
    "storage_type",
    required=True,
    type=click.Choice(["local", "s3"], case_sensitive=False),
    help="Storage backend type.",
)
@click.option(
    "--storage-driver",
    "storage_driver",
    required=True,
    type=click.Choice(["fsspec", "obstore"], case_sensitive=False),
    help="Storage driver.",
)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Report proposed changes without mutating the store.",
)
@click.option(
    "--chunk-size",
    "chunk_size",
    default=None,
    type=int,
    help="Override the consolidated coordinate chunk size (default: computed from the axis length).",
)
@click.option(
    "--time-dim",
    "time_dim",
    default=None,
    help=(
        "Explicit time-dimension name for cubes written with a custom "
        "``time_dim_name`` (plugin ``BaseIngestor.time_dim_name`` ClassVar). "
        "When omitted, the effective name is discovered per group from the "
        "resolved-index record (``params.coordinate``) and then from the "
        "group's 1-D datetime coord arrays. Pass this when discovery cannot "
        "settle on a single candidate or must be overridden."
    ),
)
@click.pass_context
@wrap_user_facing_errors
def consolidate_time_coord(
    ctx: click.Context,
    target: str,
    product_name: str,
    storage_type: str,
    storage_driver: str,
    dry_run: bool,
    chunk_size: int | None,
    time_dim: str | None,
) -> None:
    """Consolidate time coordinate chunks from per-slot storage to dense storage.

    Use this command on existing cubes produced before dense time-coordinate
    materialization. After consolidation, the time coordinate is stored in a
    denser chunk layout so object storage opens the cube with less metadata work.

    Consolidation seals the cube. After it completes, the cube becomes
    read-only for further ingest. The operation is irreversible without a
    restore from backup.

    The rewrite stages the new coordinate as a time.consolidating sibling
    before replacing the original array. If a local consolidation is
    interrupted, re-run the command: it detects the partial state, repairs
    it, and preserves the coordinate values.

    Use it for existing cubes with chunks=(1,) on the time coordinate.
    Always run with --dry-run first.
    """
    import zarr
    from firecube.core.filesystem.store_factory import create_zarr_store

    require_full_uri(target, option_name="--target")
    parsed = parse_product_uri(target)
    storage_type = apply_smart_default(parsed, storage_type)
    validate_uri_storage_coherence(parse_product_uri(target), storage_type)
    if chunk_size is not None and chunk_size <= 0:
        raise click.ClickException("--chunk-size must be a positive integer.")

    remote = storage_type.lower() == "s3"
    if remote:
        raise click.ClickException(
            "remote (s3) consolidate-time-coord is not currently supported due to non-atomic crash-recovery; see follow-up plan"
        )

    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    handle = create_zarr_store(uri=target, storage_config=storage_config, mode="a")
    root = zarr.open_group(**handle.zarr_kwargs(), mode="a", zarr_format=3)
    reference = _read_consolidate_reference_index(
        target,
        storage_config,
        product_name=product_name,
    )
    summary = _ConsolidateSummary()
    consolidated_groups: list[str] = []
    consolidated_at = datetime.now(tz=UTC).isoformat()

    # Manager and existing-seal snapshot must live outside the loop so the
    # already-sealed branch can backfill a WAL event when a prior rerun crashed
    # between stamping firecube_consolidated_at and recording the event.
    identity = resolve_product_identity(
        target,
        format="zarr",
        product_name=product_name,
        option_name="--target",
    )
    manager = ChunkManager(
        binding=StorageBinding(
            identity=identity,
            driver=StorageDriverConfig.from_storage_config(storage_config),
        )
    )
    try:
        existing_seal_groups: set[str] = {
            group_name
            for event in manager.list_time_coord_consolidations(product=identity.product_name)
            for group_name in event.groups
        }

        for group in _walk_zarr_groups(root):
            summary.scanned += 1
            resolved_time_dim, dim_source = _resolve_time_dim_for_group(
                group, explicit=time_dim, reference=reference
            )
            array_keys = set(group.array_keys())
            if resolved_time_dim is None or not array_keys:
                click.echo(f"Group {_display_group_name(group)}: no time coord, skipping")
                continue
            consolidating_key = f"{resolved_time_dim}.consolidating"
            if resolved_time_dim not in array_keys and consolidating_key not in array_keys:
                raise click.ClickException(
                    f"Group {_display_group_name(group)}: expected time coord "
                    f"{resolved_time_dim!r} (source: {dim_source}) not present "
                    f"in group (arrays: {sorted(array_keys)!r}). Refusing to "
                    "no-op. Pass a matching --time-dim or ensure the store was "
                    "written by a plugin whose time_dim_name matches this cube."
                )
            consolidated = _consolidate_group_time_coord(
                group,
                target=target,
                chunk_size=chunk_size,
                dry_run=dry_run,
                remote=remote,
                reference=reference,
                consolidated_at=consolidated_at,
                summary=summary,
                time_dim_name=resolved_time_dim,
                manager=manager,
                existing_seal_groups=existing_seal_groups,
            )
            if consolidated:
                consolidated_groups.append(str(getattr(group, "path", "") or "/").strip("/"))

        if summary.eligible == 0 and summary.already_sealed == 0 and summary.blocked_by_drift == 0:
            click.echo("consolidate-time-coord: no group with a time coord found.")
        if consolidated_groups:
            manager.record_time_coord_consolidation(tuple(consolidated_groups), consolidated_at)
    finally:
        manager.close()

    if dry_run:
        click.echo("Summary:")
        click.echo(f"  Total groups scanned: {summary.scanned}")
        click.echo(f"  Groups eligible for consolidation: {summary.eligible}")
        click.echo(f"  Groups already sealed: {summary.already_sealed}")
        click.echo(f"  Groups blocked by drift: {summary.blocked_by_drift}")
        if summary.blocked_by_drift > 0:
            raise click.exceptions.Exit(1)


def _walk_zarr_groups(root: Any) -> Generator[Any]:
    """Yield root and descendant Zarr groups depth-first."""
    yield root
    for name in sorted(root.group_keys()):
        yield from _walk_zarr_groups(root[name])


def _display_group_name(group: Any) -> str:
    path = str(getattr(group, "path", "") or "/")
    return path if path.startswith("/") else f"/{path}"


def _resolve_time_dim_for_group(
    group: Any,
    *,
    explicit: str | None,
    reference: Any | None,
) -> tuple[str | None, str | None]:
    """Return ``(time_dim_name, source)`` for a consolidate target group.

    Resolution order:

    1. ``explicit`` — operator-supplied ``--time-dim`` override.
    2. Resolved-index reference — reads
       ``reference.index.groups[<group>].params.coordinate`` when the
       persisted record stores it (irregular-time axes).
    3. Discovery — a single 1-D ``datetime64`` array under the group whose
       dimension names match ``(<array_name>,)`` (also considers a
       ``<array_name>.consolidating`` sibling for partial-recovery states).

    Returns ``(None, None)`` when no candidate exists so the caller can skip
    the group with the ``no time coord, skipping`` message.
    """
    if explicit:
        return explicit, "explicit"

    if reference is not None:
        group_key = str(getattr(group, "path", "") or "").strip("/")
        groups_meta = getattr(reference, "index", {}).get("groups", {})
        payload = groups_meta.get(group_key)
        if isinstance(payload, dict):
            params = payload.get("params") or {}
            coord = params.get("coordinate")
            if isinstance(coord, str) and coord:
                return coord, "reference"

    candidates: set[str] = set()
    for name in group.array_keys():
        base = name[: -len(".consolidating")] if name.endswith(".consolidating") else name
        try:
            arr = group[name]
        except KeyError:
            continue
        if arr.dtype.kind != "M":
            continue
        if getattr(arr, "ndim", None) != 1:
            continue
        dim_names = getattr(getattr(arr, "metadata", None), "dimension_names", None)
        if dim_names is None or (len(dim_names) == 1 and dim_names[0] == base):
            candidates.add(base)
    if len(candidates) == 1:
        return candidates.pop(), "discovered"
    if len(candidates) > 1:
        raise click.ClickException(
            f"Group {_display_group_name(group)}: multiple time-coord candidates "
            f"{sorted(candidates)!r}. Pass --time-dim explicitly to disambiguate."
        )
    return None, None


@dataclass
class _ConsolidateSummary:
    """Mutable tally of per-group outcomes reported at the end of a dry-run."""

    scanned: int = 0
    eligible: int = 0
    already_sealed: int = 0
    blocked_by_drift: int = 0


def _consolidate_group_time_coord(
    group: Any,
    *,
    target: str,
    chunk_size: int | None,
    dry_run: bool,
    remote: bool,
    reference: Any | None,
    consolidated_at: str,
    summary: _ConsolidateSummary,
    time_dim_name: str,
    manager: ChunkManager | None = None,
    existing_seal_groups: set[str] | None = None,
) -> bool:
    group_name = _display_group_name(group)
    consolidating_name = f"{time_dim_name}.consolidating"
    coord_path = f"/{time_dim_name}" if group_name == "/" else f"{group_name}/{time_dim_name}"
    if not remote:
        state = _detect_local_consolidation_state(group, target=target, coord_name=time_dim_name)
        if state == "already_sealed":
            _maybe_backfill_consolidated_wal(
                group,
                group_name=group_name,
                time_dim_name=time_dim_name,
                dry_run=dry_run,
                manager=manager,
                existing_seal_groups=existing_seal_groups,
            )
            click.echo(f"Group {group_name}: already sealed, skipping")
            summary.already_sealed += 1
            return False
        if state == "partial_pre_swap":
            if dry_run:
                click.echo(f"Group {group_name}:")
                click.echo("  Recovery state: partial pre-swap")
                click.echo(
                    f"  Proposed recovery: delete {consolidating_name} and rerun consolidation"
                )
                summary.eligible += 1
                return False
            _discard_local_consolidating_array(group, target=target, coord_name=time_dim_name)
            click.echo(f"Group {group_name}: removed stale {consolidating_name}; retrying")
        elif state == "partial_post_delete":
            if dry_run:
                click.echo(f"Group {group_name}:")
                click.echo("  Recovery state: partial post-delete")
                click.echo(
                    f"  Proposed recovery: rename {consolidating_name} to {time_dim_name} "
                    "and stamp markers"
                )
                summary.eligible += 1
                return False
            _promote_local_consolidating_array(group, target=target, coord_name=time_dim_name)
            arr = group[time_dim_name]
            _seal_recovered_time_coord(
                arr, group_name, consolidated_at=consolidated_at, coord_name=time_dim_name
            )
            summary.eligible += 1
            click.echo(f"Group {group_name}: recovered partial consolidation for {coord_path}")
            return True
        elif state == "unexpected":
            raise click.ClickException(
                f"Group {group_name}: unexpected consolidation state for "
                f"{time_dim_name}/{consolidating_name}. "
                "Refusing to continue without overwriting ambiguous state."
            )

    arr = group[time_dim_name]

    assert_coord_markers_consistent(dict(arr.attrs), coord_path)
    if bool(arr.attrs.get(ATTR_COORD_MANAGED, False)):
        raise click.ClickException(
            f"Group {coord_path} carries {ATTR_COORD_MANAGED}: the engine owns its "
            "observed coordinate values and further windows may still be materialized. "
            "Refusing to consolidate a coord-managed cube."
        )
    if bool(arr.attrs.get(ATTR_PREALLOCATED, False)):
        click.echo(f"Group {group_name}: already sealed, skipping")
        summary.already_sealed += 1
        return False
    if arr.dtype.kind != "M":
        raise click.ClickException(
            f"Group {coord_path} has non-datetime dtype {arr.dtype}. Cannot consolidate."
        )

    values = np.asarray(arr[:])
    values = values.copy()
    drift = _analyze_time_coord_against_reference(
        group_name, values, reference, time_dim_name=time_dim_name
    )
    new_chunks = (
        (int(chunk_size),) if chunk_size is not None else resolve_coord_chunks(None, len(values))
    )
    strategy = "s3 staged delete/recreate" if remote else "local sibling rename"

    if dry_run:
        current_attrs = dict(arr.attrs)
        proposed_markers = f"{ATTR_PREALLOCATED}=True, {ATTR_CONSOLIDATED_AT}={consolidated_at}"
        click.echo(f"Group {coord_path}:")
        click.echo(f"  Current chunks: {arr.chunks}")
        click.echo(f"  Current attrs: {current_attrs}")
        click.echo(f"  Proposed chunks: {new_chunks}")
        click.echo(f"  Markers to stamp: {proposed_markers}")
        click.echo(f"  Atomic strategy: {strategy}")
        click.echo(f"  Drift status: {drift.status_message}")
        if drift.is_drift:
            summary.blocked_by_drift += 1
        else:
            summary.eligible += 1
        return False

    if drift.is_drift:
        raise SchemaDriftError(
            f"Group {coord_path} differs from the WAL resolved-index record. "
            f"{drift.status_message}. Refusing to consolidate drifted coordinates."
        )

    summary.eligible += 1
    attrs = dict(arr.attrs)
    attrs.pop(ATTR_PREALLOCATED, None)
    attrs.pop(ATTR_COORD_MANAGED, None)
    attrs[ATTR_PREALLOCATED] = True
    attrs[ATTR_CONSOLIDATED_AT] = consolidated_at
    dimension_names = getattr(getattr(arr, "metadata", None), "dimension_names", None)
    fill_value = getattr(arr, "fill_value", None)

    if remote:
        _rewrite_time_coord_remote(
            group,
            values=values,
            chunks=new_chunks,
            attrs=attrs,
            fill_value=fill_value,
            dimension_names=dimension_names,
            coord_name=time_dim_name,
        )
    else:
        _rewrite_time_coord_local(
            group,
            target=target,
            values=values,
            chunks=new_chunks,
            attrs=attrs,
            fill_value=fill_value,
            dimension_names=dimension_names,
            coord_name=time_dim_name,
        )
    click.echo(f"Group {group_name}: consolidated {coord_path} chunks {arr.chunks} -> {new_chunks}")
    return True


def _maybe_backfill_consolidated_wal(
    group: Any,
    *,
    group_name: str,
    time_dim_name: str,
    dry_run: bool,
    manager: ChunkManager | None,
    existing_seal_groups: set[str] | None,
) -> None:
    """Backfill a missing ``ConsolidatedTimeCoord`` WAL event for a sealed group.

    Runs in the already-sealed branch of ``_consolidate_group_time_coord`` when
    a prior consolidation crashed between marker-stamp (``firecube_consolidated_at``
    on-array) and WAL-record. Applies a three-step guard:

    1. ``firecube_consolidated_at`` MUST be present on the coord array. Absence
       signals an ordinary preallocated-but-never-consolidated cube; backfilling
       there would spuriously seal a live cube (Blocker B).
    2. No ``ConsolidatedTimeCoord`` event for this product+group may already
       exist (idempotent no-op on repeat reruns).
    3. Record the event using the stored marker timestamp so provenance
       matches the original consolidation time — not ``datetime.now()``.
    """
    if dry_run or manager is None:
        return
    try:
        arr = group[time_dim_name]
    except (KeyError, IndexError):
        return
    stored_consolidated_at = arr.attrs.get(ATTR_CONSOLIDATED_AT)
    if not isinstance(stored_consolidated_at, str) or not stored_consolidated_at:
        return
    normalized_group = str(getattr(group, "path", "") or "").strip("/")
    if existing_seal_groups is not None and normalized_group in existing_seal_groups:
        return
    manager.record_time_coord_consolidation((normalized_group,), stored_consolidated_at)
    if existing_seal_groups is not None:
        existing_seal_groups.add(normalized_group)
    click.echo(
        f"Group {group_name}: backfilled consolidated WAL event "
        f"({ATTR_CONSOLIDATED_AT}={stored_consolidated_at})"
    )


def _detect_local_consolidation_state(group: Any, *, target: str, coord_name: str) -> str:
    """Detect the local filesystem state for a group's consolidating coord."""
    group_path = _local_group_path(group, target=target)
    coord = group_path / coord_name
    consolidating = group_path / f"{coord_name}.consolidating"

    if coord.exists():
        arr = group[coord_name]
        assert_coord_markers_consistent(dict(arr.attrs), f"{target}/{coord_name}")
        if bool(arr.attrs.get(ATTR_PREALLOCATED, False)):
            if consolidating.exists():
                return "unexpected"
            return "already_sealed"
        if consolidating.exists():
            return "partial_pre_swap"
        return "unsealed_legacy"
    if consolidating.exists():
        return "partial_post_delete"
    return "unexpected"


def _local_group_path(group: Any, *, target: str) -> Path:
    target_root = local_path_from_target(target)
    group_rel = str(getattr(group, "path", "") or "").strip("/")
    return target_root / group_rel if group_rel else target_root


def _discard_local_consolidating_array(group: Any, *, target: str, coord_name: str) -> None:
    temp_path = _local_group_path(group, target=target) / f"{coord_name}.consolidating"
    shutil.rmtree(temp_path)


def _promote_local_consolidating_array(group: Any, *, target: str, coord_name: str) -> None:
    group_path = _local_group_path(group, target=target)
    os.rename(group_path / f"{coord_name}.consolidating", group_path / coord_name)


def _seal_recovered_time_coord(
    arr: Any, group_name: str, *, consolidated_at: str, coord_name: str
) -> None:
    consolidating_path = f"{group_name}/{coord_name}.consolidating"
    if arr.dtype.kind != "M":
        raise click.ClickException(
            f"Group {consolidating_path} has non-datetime dtype {arr.dtype}. Cannot recover."
        )
    assert_coord_markers_consistent(dict(arr.attrs), consolidating_path)
    if bool(arr.attrs.get(ATTR_COORD_MANAGED, False)):
        raise click.ClickException(
            f"Group {consolidating_path} carries {ATTR_COORD_MANAGED}; "
            "refusing to seal engine-managed observed coordinates during recovery."
        )
    arr.attrs[ATTR_PREALLOCATED] = True
    arr.attrs[ATTR_CONSOLIDATED_AT] = consolidated_at


def _rewrite_time_coord_local(
    group: Any,
    *,
    target: str,
    values: np.ndarray,
    chunks: tuple[int, ...],
    attrs: dict[str, Any],
    fill_value: Any,
    dimension_names: Any,
    coord_name: str,
) -> None:
    consolidating_name = f"{coord_name}.consolidating"
    target_root = local_path_from_target(target)
    group_rel = str(getattr(group, "path", "") or "").strip("/")
    group_path = target_root / group_rel if group_rel else target_root
    final_path = group_path / coord_name
    temp_path = group_path / consolidating_name
    shutil.rmtree(temp_path, ignore_errors=True)
    temp = group.create_array(
        consolidating_name,
        shape=values.shape,
        dtype=values.dtype,
        chunks=chunks,
        fill_value=fill_value,
        attributes=attrs,
        dimension_names=dimension_names,
        overwrite=True,
    )
    temp[...] = values
    shutil.rmtree(final_path)
    os.rename(temp_path, final_path)


def _rewrite_time_coord_remote(
    group: Any,
    *,
    values: np.ndarray,
    chunks: tuple[int, ...],
    attrs: dict[str, Any],
    fill_value: Any,
    dimension_names: Any,
    coord_name: str,
) -> None:
    # Object stores have no atomic directory rename. Stage first so a failed
    # write leaves the original array intact; after deletion, rerun recreates
    # the value-preserving sealed coord from the same legacy source contract.
    consolidating_name = f"{coord_name}.consolidating"
    if consolidating_name in group.array_keys():
        del group[consolidating_name]
    temp = group.create_array(
        consolidating_name,
        shape=values.shape,
        dtype=values.dtype,
        chunks=chunks,
        fill_value=fill_value,
        attributes=attrs,
        dimension_names=dimension_names,
        overwrite=True,
    )
    temp[...] = values
    del group[coord_name]
    final = group.create_array(
        coord_name,
        shape=values.shape,
        dtype=values.dtype,
        chunks=chunks,
        fill_value=fill_value,
        attributes=attrs,
        dimension_names=dimension_names,
        overwrite=True,
    )
    final[...] = values
    del group[consolidating_name]


def _read_consolidate_reference_index(
    target: str,
    storage_config: Any,
    *,
    product_name: str,
) -> Any | None:
    """Best-effort read of the control-plane resolved-index record."""
    identity = resolve_product_identity(
        target,
        format="zarr",
        product_name=product_name,
        option_name="--target",
    )
    manager = ChunkManager(
        binding=StorageBinding(
            identity=identity,
            driver=StorageDriverConfig.from_storage_config(storage_config),
        )
    )
    try:
        return manager.get_resolved_index(product=identity.product_name)
    except FileNotFoundError:
        return None
    finally:
        manager.close()


@dataclass(frozen=True)
class _TimeCoordDriftAnalysis:
    """Result of comparing a group's time coord array against the reference index.

    ``is_drift`` is True when the coord array cannot be safely resealed because
    the persisted reference index proves the values diverge. ``status_message``
    is a single line suitable for dry-run reporting and for the live-run
    ``SchemaDriftError`` payload.
    """

    is_drift: bool
    status_message: str


def _analyze_time_coord_against_reference(
    group_name: str,
    values: np.ndarray,
    reference: Any | None,
    *,
    time_dim_name: str,
) -> _TimeCoordDriftAnalysis:
    """Return a structured drift analysis; no I/O.

    Reports a single ``DRIFT DETECTED AT SLOT N: expected X, got Y`` message
    when the first divergent slot is found. Falls back to ``CLEAN`` variants
    when no reference exists or the reference is not directly comparable.
    """
    if reference is None:
        return _TimeCoordDriftAnalysis(
            is_drift=False,
            status_message="CLEAN (no reference index — value-preserving mode)",
        )

    group_key = group_name.strip("/")
    groups = getattr(reference, "index", {}).get("groups", {})
    payload = groups.get(group_key)
    if not isinstance(payload, dict):
        return _TimeCoordDriftAnalysis(
            is_drift=False,
            status_message="CLEAN (no group reference index — value-preserving mode)",
        )

    kind = payload.get("kind")
    params = payload.get("params") or {}
    size = int(payload.get("size") or 0)
    if kind == "regular_time":
        expected = _regular_time_values_from_reference(params, size, values.dtype)
    elif kind == "irregular_time":
        reference_coord = params.get("coordinate", time_dim_name)
        if reference_coord != time_dim_name:
            return _TimeCoordDriftAnalysis(
                is_drift=False,
                status_message=(
                    f"CLEAN (reference coordinate {reference_coord!r} does not match "
                    f"time_dim_name {time_dim_name!r}; skipped)"
                ),
            )
        raw_values = params.get("values")
        if not isinstance(raw_values, list):
            return _TimeCoordDriftAnalysis(
                is_drift=False,
                status_message="CLEAN (no concrete reference values)",
            )
        expected = np.asarray(
            [coord_to_datetime64(value) for value in raw_values], dtype=values.dtype
        )
    else:
        return _TimeCoordDriftAnalysis(
            is_drift=False,
            status_message="CLEAN (non-datetime reference skipped)",
        )

    if values.shape != expected.shape:
        return _TimeCoordDriftAnalysis(
            is_drift=True,
            status_message=(
                f"DRIFT DETECTED: shape mismatch, expected {expected.shape}, got {values.shape}"
            ),
        )

    equal_slots = np.equal(values, expected)
    if values.dtype.kind == "M" and expected.dtype.kind == "M":
        equal_slots = equal_slots | (np.isnat(values) & np.isnat(expected))
    diff_mask = ~equal_slots
    if not bool(diff_mask.any()):
        return _TimeCoordDriftAnalysis(
            is_drift=False,
            status_message="CLEAN (reference index matched)",
        )
    slot = int(np.argmax(diff_mask))
    return _TimeCoordDriftAnalysis(
        is_drift=True,
        status_message=(
            f"DRIFT DETECTED AT SLOT {slot}: expected {expected[slot]!s}, got {values[slot]!s}"
        ),
    )


def _regular_time_values_from_reference(
    params: dict[str, Any], size: int, dtype: np.dtype[Any]
) -> np.ndarray:
    epoch = np.datetime64(str(params["epoch"]).removesuffix("Z"), "ns")
    cadence = np.timedelta64(int(float(params["cadence_s"]) * 1_000_000_000), "ns")
    return (epoch + np.arange(size, dtype=np.int64) * cadence).astype(dtype)
