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

"""IndexedRegionStrategy — direct-Zarr region writes via ``RegionZarrWriter``.

Implements the write-execution loop for ``DirectZarrIngestor``:
open store → ensure groups/arrays → execute ``WriteIntent`` list →
build coverage entries.  Uses ``RegionZarrWriter`` for all store I/O
and ``CoverageTracker`` for coverage bookkeeping.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import logging
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import replace
from operator import index as operator_index
from typing import TYPE_CHECKING, Any, cast

import numpy as np

from firecube.core.errors import SchemaDriftError
from firecube.core.uris import storage_uri_from_target
from firecube.core.zarr._drift import (
    arrays_equal_missing_aware as _arrays_equal_missing_aware,
)
from firecube.core.zarr.guard._layout import resolve_effective_layout, warn_default_layout
from firecube.core.zarr.guard.chunk_geometry import physical_chunk_keys_for_region
from firecube.core.zarr.guard.engine_owned import is_engine_owned_spec, is_pod_written_slot_array
from firecube.core.zarr.guard.guarded_write import WriteResult
from firecube.core.zarr.guard.metadata_authority import (
    HeldClaim,
    MetadataAuthority,
    adopt_held_claim,
    ensure_group,
)
from firecube.core.zarr.guard.unit_ownership import require_window_owns_stored_units
from firecube.core.zarr.region_writer import RegionZarrWriter, open_root_read_only
from firecube.ingestor.runtime.coverage import CoverageTracker
from firecube.ingestor.runtime.parallel_evidence import log_filter_evidence

if TYPE_CHECKING:
    from firecube.core.config import StorageConfig
    from firecube.core.storage.session import StorageSession


def _session_for_store(store_uri: str, storage_config: StorageConfig) -> StorageSession:
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding
    from firecube.core.storage.driver_config import StorageDriverConfig
    from firecube.core.storage.session import StorageSession

    uri = storage_uri_from_target(store_uri)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(uri, format="zarr", product_name=store_uri),
            driver=StorageDriverConfig.from_storage_config(storage_config),
        )
    )


log = logging.getLogger("firecube.runtime.zarr.strategies.indexed_region")


def ensure_group_ancestors(writer: RegionZarrWriter, group: str) -> None:
    """Create the root and the missing parents of *group* under the writer's own authority.

    An authority built from a claim over one group covers that group's
    subtree only, so it may not create the nodes above it. Those are created
    empty, when absent, under the writer's ``PROCESS_LOCAL`` authority over
    the whole store (the threads of this process are excluded, other
    processes are not), before the claim's authority is used.
    """
    root = writer._open_root()
    parent = "/".join(p for p in group.split("/") if p).rpartition("/")[0]
    if parent:
        ensure_group(writer.authority, root, parent)


def require_intents_in_slot_window(
    group_to_intents: Mapping[str, Sequence[Any]],
    slot_range: tuple[int, int],
    slot_group: str | None,
) -> None:
    """Refuse an intent addressing a slot outside the assigned half-open window.

    With ``slot_group`` set only that group's intents are checked; the others
    are skipped with a warning (this pod does not own them). Static intents
    carry no time semantics and are exempt.

    Raises:
        WriteIntentRangeError: An intent's ``ts_index`` is outside ``slot_range``.
    """
    slot_start, slot_end = slot_range
    for group_name, intents in group_to_intents.items():
        if slot_group is not None and group_name != slot_group:
            if intents:
                log.warning(
                    "slot_group=%r: skipping %d intent(s) for group %r "
                    "(this pod does not own that group)",
                    slot_group,
                    len(intents),
                    group_name,
                )
            continue
        for intent in intents:
            if getattr(intent, "kind", None) == "static":
                continue
            ts = getattr(intent, "ts_index", None)
            if ts is not None and not (slot_start <= ts < slot_end):
                from firecube.ingestor.errors import WriteIntentRangeError

                raise WriteIntentRangeError(
                    f"Intent ts_index={ts} for group={group_name!r} is outside "
                    f"assigned slot_range=[{slot_start}, {slot_end}). "
                    "Plugin filter is advisory; this is a correctness violation."
                )


# Reserved array attr stamped once a static (write-once) array's data has been
# committed. Its presence — not the array contents — is the authoritative
# "already written in a prior run" signal on resume. Registered in
# ``firecube.core.zarr._reserved_attrs.RESERVED_ARRAY_ATTRS``.
_STATIC_WRITTEN_ATTR = "firecube_static_written"


@dataclasses.dataclass(frozen=True)
class _RegionFutureMeta:
    group: str
    array: str
    ts_index: int
    aligned: bool


@dataclasses.dataclass(frozen=True)
class _RegionSelection:
    ts_index: int
    y_start: int
    y_stop: int
    channel_index: int | None


def _schema_dimension_names(arr_spec: Any, time_coord_name: str) -> tuple[str, ...] | None:
    explicit = getattr(arr_spec, "dimension_names", None)
    if explicit is not None:
        return explicit
    shape = tuple(getattr(arr_spec, "shape", ()))
    if getattr(arr_spec, "name", None) == time_coord_name and len(shape) == 1:
        return (time_coord_name,)
    return None


class IndexedRegionStrategy:
    """Write strategy that dispatches ``WriteIntent`` objects to ``RegionZarrWriter``.

    Implements the ``RegionWriteStrategy`` Protocol.

    Constructed with a store URI and schema; ``write_groups()`` receives
    the per-batch intents and returns coverage metrics.
    """

    def __init__(
        self,
        *,
        store_uri: str,
        schema: Sequence[Any] | None = None,
        coord_names_by_group: dict[str, frozenset[str]] | None = None,
        time_coord_name: str = "timestamp",
        storage_config: StorageConfig | None = None,
        session: StorageSession | None = None,
    ) -> None:
        self._store_uri = store_uri
        self._schema = schema or []
        self._coord_names_by_group = coord_names_by_group or {}
        self._time_coord_name = time_coord_name
        self._storage_config = storage_config
        self._session = session

    def write_groups(
        self,
        *,
        group_to_intents: dict[str, list[Any]],
        schema: Sequence[Any] | None = None,
        claim_for_group: Callable[[str], Any] | None = None,
        claim_for_slot: Callable[[str, int], Any] | None = None,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
        codec_pipelines_by_array: Mapping[tuple[str, str], tuple[Any, Any, Any]] | None = None,
        region_write_concurrency: int = 1,
        suppress_static_emission_for_non_owner: bool = False,
        static_owner_slot_start: int | None = None,
        zarr_write_empty_chunks: bool = False,
        on_write: Callable[[WriteResult], None] | None = None,
        track_units: bool = False,
    ) -> dict[str, Any]:
        """Dispatch write intents into the store.

        Implements `RegionWriteStrategy.write_groups` and adds the DirectZarr
        knobs: static-write ownership and the ``write_empty_chunks`` setting,
        which is applied to each array this call writes and never to
        ``zarr.config``, so it cannot leak into other threads or Zarr
        operations in the same process.

        Args:
            group_to_intents: Write intents to dispatch, keyed by group path.
            schema: Array specs used for schema setup and drift checks.
            claim_for_group: Called as ``(group)`` to obtain a write claim
                guarding schema setup. When its scope yields a `HeldClaim`,
                the schema step and the static write-once markers change
                ``zarr.json`` under an authority adopted from that claim,
                over the group's subtree; otherwise under the writer's own
                ``PROCESS_LOCAL`` authority.
            claim_for_slot: Called as ``(group, ts_index)`` to obtain a
                per-slot write claim guarding intent dispatch.
            slot_range: Half-open ``(start, stop)`` slot window this caller
                owns. ``None`` accepts every slot the intents address.
            slot_group: Group whose slots *slot_range* applies to.
            codec_pipelines_by_array: Per-array ``(filters, serializer,
                compressors)`` overrides, keyed by ``(group, array_name)``.
            region_write_concurrency: Maximum region writes in flight at once.
            suppress_static_emission_for_non_owner: Skip static write intents
                unless this caller owns them, so parallel pods do not all
                rewrite the same static arrays.
            static_owner_slot_start: Slot start of the pod that owns static
                writes, compared against this run's own slot start.
            zarr_write_empty_chunks: Whether to persist chunks holding only
                fill values. ``False`` keeps sparse arrays compact.
            on_write: Called on the writing thread with the `WriteResult` of
                every payload write (see `RegionZarrWriter`).
            track_units: Report in each `WriteResult` which touched units
                hold no object after the write. Requires ``on_write``.

        Returns:
            Write metrics for the call, including
            ``zarr_write_empty_chunks_effective``.

        Raises:
            ValueError: ``track_units`` is set without ``on_write``.
        """
        if track_units and on_write is None:
            raise ValueError("track_units=True reports to on_write; pass on_write as well")
        result = self._write_groups(
            group_to_intents=group_to_intents,
            schema=schema,
            claim_for_group=claim_for_group,
            claim_for_slot=claim_for_slot,
            slot_range=slot_range,
            slot_group=slot_group,
            codec_pipelines_by_array=codec_pipelines_by_array,
            region_write_concurrency=region_write_concurrency,
            suppress_static_emission_for_non_owner=suppress_static_emission_for_non_owner,
            static_owner_slot_start=static_owner_slot_start,
            zarr_write_empty_chunks=zarr_write_empty_chunks,
            on_write=on_write,
            track_units=track_units,
        )
        result["zarr_write_empty_chunks_effective"] = zarr_write_empty_chunks
        return result

    def _write_groups(
        self,
        *,
        group_to_intents: dict[str, list[Any]],
        schema: Sequence[Any] | None = None,
        claim_for_group: Callable[[str], Any] | None = None,
        claim_for_slot: Callable[[str, int], Any] | None = None,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
        codec_pipelines_by_array: Mapping[tuple[str, str], tuple[Any, Any, Any]] | None = None,
        region_write_concurrency: int = 1,
        suppress_static_emission_for_non_owner: bool = False,
        static_owner_slot_start: int | None = None,
        zarr_write_empty_chunks: bool = False,
        on_write: Callable[[WriteResult], None] | None = None,
        track_units: bool = False,
    ) -> dict[str, Any]:
        """Execute write intents grouped by Zarr group and return metrics.

        Args:
            group_to_intents: Mapping of group name to list of ``WriteIntent``
                objects.
            schema: Group specifications for array creation.  Falls back to
                the constructor-provided schema if ``None``.
            claim_for_group: Optional callback returning a context manager for
                group/schema coordination.  When ``claim_for_slot`` is
                provided, this protects schema setup and the static
                write-once markers only.
            claim_for_slot: Optional callback returning a context manager for
                per-``ts_index`` intent dispatch.  Falls back to
                ``claim_for_group`` and then a null context when omitted.
            slot_range: Optional half-open ``[start, end)`` slot range.  When
                provided, all intents are validated before any schema setup or
                writes occur.
            slot_group: Optional group name that this pod owns.  When set
            together with ``slot_range``, the post-intent assertion only
            validates intents for ``slot_group``; intents for other groups
            are skipped with a warning (not silently dropped) and no
            writes happen for them.
            region_write_concurrency: Maximum concurrent region writes per
                claimed timestamp slot. ``1`` uses the historical serial
                dispatch path.
            suppress_static_emission_for_non_owner: When true, static intents
                are emitted only by the worker whose slot start matches
                ``static_owner_slot_start``.
            static_owner_slot_start: V1 scalar owner ``slot_start`` for one
                group per run.
            zarr_write_empty_chunks: Passed to every writer this call builds.
            on_write: Passed to every writer this call builds.
            track_units: Passed to every writer this call builds.

        Returns:
            dict with ``"coverage"`` list and ``"duration_s"`` float.
        """
        effective_schema = schema if schema is not None else self._schema
        t0 = time.monotonic()
        tracker = CoverageTracker(time_dim_name=self._time_coord_name)

        schema_by_group: dict[str, Any] = {}
        for spec in effective_schema:
            schema_by_group[spec.group] = spec

        array_specs_by_path = {
            (group_spec.group, arr_spec.name): arr_spec
            for group_spec in effective_schema
            for arr_spec in getattr(group_spec, "arrays", ())
        }

        if region_write_concurrency < 1:
            raise ValueError(
                f"region_write_concurrency must be >= 1, got {region_write_concurrency}"
            )

        if region_write_concurrency > 1:
            self._validate_declared_concurrent_region_intents(
                group_to_intents=group_to_intents,
                array_specs_by_path=array_specs_by_path,
                slot_group=slot_group,
                slot_range=slot_range,
            )

        region_writes_total_count = 0
        region_writes_aligned_count = 0

        if slot_range is not None:
            all_intents = [
                intent
                for intents in group_to_intents.values()
                for intent in intents
                if hasattr(intent, "ts_index")
            ]
            require_intents_in_slot_window(group_to_intents, slot_range, slot_group)
            log_filter_evidence(
                log,
                stage="post_intent_assertion",
                planned_range=slot_range,
                original_count=len(all_intents),
                filtered_count=len(all_intents),
                dropped_count=0,
            )
            # Before any write of this batch, every group: the window must own
            # every stored unit its writes touch.
            written_paths = self._pod_written_paths(group_to_intents, schema_by_group, slot_group)
            if written_paths:
                require_window_owns_stored_units(
                    self._stored_root_read_only(), written_paths, slot_range
                )

        for group_name, intents in group_to_intents.items():
            # Phase 3.1 T7: When slot_group is set, skip writes for other groups
            # (warning already logged above during the assertion pass).
            if slot_group is not None and slot_range is not None and group_name != slot_group:
                continue
            coord_names = self._coord_names_by_group.get(
                group_name, frozenset({"y", "x", "channel"})
            )
            group_spec = schema_by_group.get(group_name)
            if group_spec is not None:
                static_names = frozenset(
                    arr.name for arr in group_spec.arrays if not getattr(arr, "time_indexed", True)
                )
                coord_names = coord_names | static_names
            writer = self._new_writer(
                coord_names,
                zarr_write_empty_chunks,
                on_write=on_write,
                track_units=track_units,
                fixed_extent=slot_range is not None,
            )

            intents_with_ts = [
                intent for intent in intents if getattr(intent, "ts_index", None) is not None
            ]
            if intents_with_ts and group_spec is not None:
                max_ts = max(intent.ts_index for intent in intents_with_ts) + 1
                augmented_arrays = type(group_spec.arrays)(
                    replace(arr, expected_time_count=max_ts)
                    if hasattr(arr, "expected_time_count")
                    and arr.expected_time_count is None
                    and getattr(arr, "time_indexed", True)
                    else arr
                    for arr in group_spec.arrays
                )
                group_spec = replace(group_spec, arrays=augmented_arrays)
            # In slot-range (parallel) mode the global schema is created/verified
            # exactly once per pod at startup (_verify_schema_at_pod_startup), so
            # repeating it here under an exclusive per-batch claim is redundant and
            # would make concurrent pods on the same group race on idempotent setup.
            # Skip the claim and the ensure in that mode only; single-pod runs keep
            # the claim-then-ensure ordering byte-for-byte unchanged.
            parallel_mode = slot_range is not None
            with self._schema_authority(
                writer, group_name, None if parallel_mode else claim_for_group
            ) as authority:
                if group_spec is not None and not parallel_mode:
                    # Under a held claim its authority; otherwise the writer's own.
                    auth_kw: dict[str, Any] = (
                        {"authority": authority} if authority is not None else {}
                    )
                    for arr_spec in group_spec.arrays:
                        if arr_spec.time_indexed:
                            expected_time_count = getattr(arr_spec, "expected_time_count", None)
                            effective_shape = (
                                (expected_time_count, *arr_spec.shape[1:])
                                if expected_time_count is not None
                                else arr_spec.shape
                            )
                        else:
                            effective_shape = arr_spec.shape
                        allow_grow = bool(intents_with_ts)
                        filters, serializer, compressors = (
                            codec_pipelines_by_array.get(
                                (group_name, arr_spec.name),
                                (None, None, None),
                            )
                            if codec_pipelines_by_array is not None
                            else (None, None, None)
                        )
                        array_path = f"{group_name}/{arr_spec.name}"
                        layout = resolve_effective_layout(
                            arr_spec,
                            coord_name=self._time_coord_name,
                            # Static arrays (possibly rank 0) have no time extent.
                            time_count=effective_shape[0] if arr_spec.time_indexed else 0,
                            engine_owned=is_engine_owned_spec(arr_spec),
                        )
                        writer.ensure_group(
                            array_path,
                            shape=effective_shape,
                            dtype=arr_spec.dtype,
                            fill_value=arr_spec.fill_value,
                            chunks=layout.chunks,
                            allow_grow=allow_grow,
                            shards=layout.shards,
                            attrs=arr_spec.attrs,
                            dimension_names=_schema_dimension_names(
                                arr_spec,
                                self._time_coord_name,
                            ),
                            filters=filters,
                            serializer=serializer,
                            compressors=compressors,
                            # Warns once: only the batch that creates the array
                            # applies the default; later batches open it as stored.
                            on_create=(
                                (lambda _arr, p=array_path, lay=layout: warn_default_layout(p, lay))
                                if layout.is_default
                                else None
                            ),
                            **auth_kw,
                        )
                    writer.set_group_attrs(
                        group_name, getattr(group_spec, "attrs", None), **auth_kw
                    )

            static_intents = [
                intent for intent in intents if getattr(intent, "kind", None) == "static"
            ]
            timed_intents = [
                intent for intent in intents if getattr(intent, "kind", None) != "static"
            ]

            region_writes_total_count += sum(
                1 for intent in timed_intents if getattr(intent, "kind", None) == "region"
            )

            if region_write_concurrency > 1:
                region_writes_aligned_count += self._count_declared_aligned_region_writes(
                    timed_intents,
                    array_specs_by_path,
                )

            # Outside a slot window each write-once marker is stamped under the
            # authority of the group's schema claim, taken around the stamp alone.
            stamp_scope = functools.partial(
                self._schema_authority,
                writer,
                group_name,
                None if parallel_mode else claim_for_group,
            )
            if region_write_concurrency == 1:
                for intent in static_intents:
                    if self._should_suppress_static_intent(
                        intent,
                        slot_range=slot_range,
                        suppress_static_emission_for_non_owner=suppress_static_emission_for_non_owner,
                        static_owner_slot_start=static_owner_slot_start,
                    ):
                        continue
                    self._dispatch_static_intent(writer, intent, stamp_scope=stamp_scope)

                intents_by_slot: dict[int, list[Any]] = {}
                for intent in timed_intents:
                    intents_by_slot.setdefault(intent.ts_index, []).append(intent)

                for ts_index, slot_intents in intents_by_slot.items():
                    if claim_for_slot is not None:
                        slot_ctx = claim_for_slot(group_name, ts_index)
                    elif claim_for_group is not None:
                        slot_ctx = claim_for_group(group_name)
                    else:
                        slot_ctx = contextlib.nullcontext()
                    with slot_ctx:
                        for intent in slot_intents:
                            self._dispatch_intent(writer, intent)
                            if intent.kind == "timestamp" and intent.timestamp_val is not None:
                                tracker.record_write(
                                    group=group_name,
                                    arrays=[self._time_coord_name],
                                    ts_index=intent.ts_index,
                                    time_val=intent.timestamp_val,
                                    aligned=True,
                                )
                            elif intent.kind == "region":
                                tracker.record_write(
                                    group=group_name,
                                    arrays=[intent.array],
                                    ts_index=intent.ts_index,
                                    time_val=None,
                                    aligned=True,
                                )
                            elif intent.kind == "1d":
                                # Only advance coverage time bounds when this 1-D write
                                # targets the declared time-coord array AND carries a real
                                # timestamp value. Non-time-coord 1-D writes still register
                                # their index range (for span coverage) but pass
                                # ``time_val=None`` so they cannot poison ``time_min``/
                                # ``time_max`` with arbitrary data.
                                is_time_coord = intent.array == self._time_coord_name
                                time_val = (
                                    intent.timestamp_val
                                    if is_time_coord and intent.timestamp_val is not None
                                    else None
                                )
                                tracker.record_write(
                                    group=group_name,
                                    arrays=[intent.array],
                                    ts_index=intent.ts_index,
                                    time_val=time_val,
                                    aligned=True,
                                )
            else:
                root = writer._open_root()
                aligned_by_intent = self._validate_opened_concurrent_region_targets(
                    root=root,
                    group_name=group_name,
                    timed_intents=timed_intents,
                    array_specs_by_path=array_specs_by_path,
                )

                for intent in static_intents:
                    if self._should_suppress_static_intent(
                        intent,
                        slot_range=slot_range,
                        suppress_static_emission_for_non_owner=suppress_static_emission_for_non_owner,
                        static_owner_slot_start=static_owner_slot_start,
                    ):
                        continue
                    self._dispatch_static_intent(writer, intent, stamp_scope=stamp_scope)

                self._dispatch_timed_intents_concurrently(
                    writer=writer,
                    group_name=group_name,
                    timed_intents=timed_intents,
                    claim_for_group=claim_for_group,
                    claim_for_slot=claim_for_slot,
                    tracker=tracker,
                    region_write_concurrency=region_write_concurrency,
                    aligned_by_intent=aligned_by_intent,
                    time_coord_name=self._time_coord_name,
                )

        coverage = [dataclasses.asdict(c) for c in tracker.build_coverage()]
        return {
            "coverage": coverage,
            "duration_s": time.monotonic() - t0,
            "region_write_concurrency_effective": region_write_concurrency,
            "region_writes_aligned_count": region_writes_aligned_count,
            "region_writes_total_count": region_writes_total_count,
        }

    def _new_writer(
        self,
        coord_names: frozenset[str],
        write_empty_chunks: bool,
        *,
        on_write: Callable[[WriteResult], None] | None = None,
        track_units: bool = False,
        fixed_extent: bool = False,
    ) -> RegionZarrWriter:
        """Build a batch writer; pass ``fixed_extent`` only for slot-window batches.

        Pod startup creates a slot-window batch's arrays at the full slot count
        and its writer never grows them. Growth by another process is not detected.
        """
        return RegionZarrWriter(
            self._store_uri,
            store=self._session_store(mode="a"),
            coord_names=coord_names,
            time_coord_name=self._time_coord_name,
            write_empty_chunks=write_empty_chunks,
            on_write=on_write,
            track_units=track_units,
            fixed_extent=fixed_extent,
        )

    @staticmethod
    @contextlib.contextmanager
    def _schema_authority(
        writer: RegionZarrWriter,
        group_name: str,
        claim_for_group: Callable[[str], Any] | None,
    ) -> Iterator[MetadataAuthority | None]:
        """Hold the group's schema claim, if any, and yield the authority it names.

        When the claim's scope yields a `HeldClaim`, the root and the group's
        parents are first created under the writer's own authority
        (`ensure_group_ancestors`), then an authority over the group's
        subtree is adopted from the claim; it is released before the claim.
        Without a claim callable, or with a claim that names none, yields
        ``None``: the writer's own ``PROCESS_LOCAL`` authority applies.
        """
        if claim_for_group is None:
            yield None
            return
        with claim_for_group(group_name) as held:
            if not isinstance(held, HeldClaim):
                yield None
                return
            ensure_group_ancestors(writer, group_name)
            with adopt_held_claim(writer.metadata_scope(group_name), held) as authority:
                yield authority

    def _session_store(self, *, mode: str) -> object | None:
        if not self._storage_config:
            return None
        session = self._session or _session_for_store(self._store_uri, self._storage_config)
        return session.zarr.create_store(
            uri=storage_uri_from_target(self._store_uri),
            mode=mode,
        ).store

    def _stored_root_read_only(self) -> Any | None:
        """The store as written so far, opened read-only (``None`` if it does not exist)."""
        try:
            store = self._session_store(mode="r")
        except FileNotFoundError:
            return None
        return open_root_read_only(self._store_uri, store=store)

    def _pod_written_paths(
        self,
        group_to_intents: Mapping[str, Sequence[Any]],
        schema_by_group: Mapping[str, Any],
        slot_group: str | None,
    ) -> list[str]:
        """Paths of the time-indexed arrays this batch writes slots of.

        Engine-owned arrays and the time coordinate are left out: pods only
        verify engine-owned arrays, and a time coordinate that carries a
        preallocate marker. An unmarked coordinate, which pods do write, is
        left out as well. Static arrays have no slots.
        """
        paths: set[str] = set()
        for group_name, intents in group_to_intents.items():
            if slot_group is not None and group_name != slot_group:
                continue
            group_spec = schema_by_group.get(group_name)
            specs = {arr.name: arr for arr in getattr(group_spec, "arrays", ())}
            for intent in intents:
                if getattr(intent, "kind", None) not in ("region", "1d"):
                    continue
                if intent.array == self._time_coord_name:
                    continue
                spec = specs.get(intent.array)
                if spec is not None and not is_pod_written_slot_array(
                    spec, time_coord_name=self._time_coord_name
                ):
                    continue
                paths.add(f"{group_name}/{intent.array}")
        return sorted(paths)

    @staticmethod
    def _should_suppress_static_intent(
        intent: Any,
        *,
        slot_range: tuple[int, int] | None,
        suppress_static_emission_for_non_owner: bool,
        static_owner_slot_start: int | None,
    ) -> bool:
        if slot_range is None:
            # Serial mode: no non-owner concept, always write statics locally.
            return False
        if not suppress_static_emission_for_non_owner:
            return False
        slot_start = slot_range[0]
        if slot_start == static_owner_slot_start:
            return False
        log.warning(
            "static write suppressed as non-owner: group=%s array=%s slot_start=%s owner=%s",
            intent.group,
            intent.array,
            slot_start,
            static_owner_slot_start,
        )
        return True

    @classmethod
    def _validate_declared_concurrent_region_intents(
        cls,
        *,
        group_to_intents: dict[str, list[Any]],
        array_specs_by_path: Mapping[tuple[str, str], Any],
        slot_group: str | None,
        slot_range: tuple[int, int] | None,
    ) -> None:
        for group_name, intents in group_to_intents.items():
            if slot_group is not None and slot_range is not None and group_name != slot_group:
                continue
            for intent in intents:
                if getattr(intent, "kind", None) != "region":
                    continue
                spec = cls._array_spec_for_intent(intent, array_specs_by_path)
                if getattr(spec, "shards", None) is not None:
                    raise ValueError(
                        "Concurrent region writes do not support sharded targets: "
                        f"group={intent.group!r} array={intent.array!r} declares "
                        f"shards={tuple(spec.shards)!r}. Set region_write_concurrency=1 "
                        "or recreate the target unsharded."
                    )
                cls._validate_region_selection(
                    intent,
                    tuple(getattr(spec, "shape", ())),
                    source="declared schema",
                )

    @classmethod
    def _validate_opened_concurrent_region_targets(
        cls,
        *,
        root: Any,
        group_name: str,
        timed_intents: Sequence[Any],
        array_specs_by_path: Mapping[tuple[str, str], Any],
    ) -> dict[int, bool]:
        aligned_by_intent: dict[int, bool] = {}
        by_slot: dict[int, list[Any]] = {}

        for intent in timed_intents:
            if getattr(intent, "kind", None) != "region":
                continue
            by_slot.setdefault(intent.ts_index, []).append(intent)

        for slot_intents in by_slot.values():
            chunk_owner: dict[tuple[str, str, tuple[int, ...]], Any] = {}
            for intent in slot_intents:
                spec = cls._array_spec_for_intent(intent, array_specs_by_path)
                arr_path = f"{intent.group}/{intent.array}"
                try:
                    arr = root[arr_path]
                except KeyError as exc:
                    raise ValueError(
                        "Concurrent region write target is missing after schema setup: "
                        f"group={intent.group!r} array={intent.array!r}"
                    ) from exc

                if getattr(spec, "shards", None) is not None:
                    raise ValueError(
                        "Concurrent region writes do not support sharded targets: "
                        f"group={intent.group!r} array={intent.array!r} declares "
                        f"shards={tuple(spec.shards)!r}."
                    )

                existing_shards = getattr(arr, "shards", None)
                if existing_shards is not None:
                    raise ValueError(
                        "Concurrent region writes do not support sharded targets: "
                        f"group={intent.group!r} array={intent.array!r} is sharded on disk "
                        f"with shards={tuple(existing_shards)!r}. Set "
                        "region_write_concurrency=1 or recreate the target unsharded."
                    )

                shape = tuple(int(axis) for axis in getattr(arr, "shape", ()))
                chunks = cls._chunks_for_opened_array(arr, arr_path)
                selection = cls._validate_region_selection(
                    intent,
                    shape,
                    source="opened target array",
                )
                keys, aligned = physical_chunk_keys_for_region(
                    group=group_name,
                    intent=intent,
                    shape=shape,
                    chunks=chunks,
                    selection=selection,
                )
                if not keys:
                    raise ValueError(
                        "Concurrent region write selection touches no physical chunks: "
                        f"group={intent.group!r} array={intent.array!r} "
                        f"ts_index={intent.ts_index!r} y_slice={intent.y_slice!r}"
                    )
                for key in keys:
                    previous = chunk_owner.get(key)
                    if previous is not None:
                        raise ValueError(
                            "Concurrent region writes overlap one physical chunk: "
                            f"group={intent.group!r} array={intent.array!r} "
                            f"chunk={key[2]!r} previous_ts_index={previous.ts_index!r} "
                            f"current_ts_index={intent.ts_index!r}."
                        )
                    chunk_owner[key] = intent
                aligned_by_intent[id(intent)] = aligned

        return aligned_by_intent

    @classmethod
    def _dispatch_timed_intents_concurrently(
        cls,
        *,
        writer: RegionZarrWriter,
        group_name: str,
        timed_intents: Sequence[Any],
        claim_for_group: Callable[[str], Any] | None,
        claim_for_slot: Callable[[str, int], Any] | None,
        tracker: CoverageTracker,
        region_write_concurrency: int,
        aligned_by_intent: Mapping[int, bool],
        time_coord_name: str,
    ) -> None:
        intents_by_slot: dict[int, list[Any]] = {}
        for intent in timed_intents:
            intents_by_slot.setdefault(intent.ts_index, []).append(intent)

        has_region_intents = any(
            getattr(intent, "kind", None) == "region" for intent in timed_intents
        )
        executor_ctx: contextlib.AbstractContextManager[ThreadPoolExecutor | None]
        executor_ctx = (
            ThreadPoolExecutor(max_workers=region_write_concurrency)
            if has_region_intents
            else contextlib.nullcontext(None)
        )

        with executor_ctx as executor:
            for ts_index, slot_intents in intents_by_slot.items():
                if claim_for_slot is not None:
                    slot_ctx = claim_for_slot(group_name, ts_index)
                elif claim_for_group is not None:
                    slot_ctx = claim_for_group(group_name)
                else:
                    slot_ctx = contextlib.nullcontext()
                with slot_ctx:
                    pending: set[Future[None]] = set()
                    future_meta: dict[Future[None], _RegionFutureMeta] = {}
                    try:
                        for intent in slot_intents:
                            if intent.kind == "region":
                                if executor is None:
                                    raise RuntimeError(
                                        "internal error: missing executor for region write"
                                    )
                                cls._wait_for_region_capacity(
                                    pending=pending,
                                    future_meta=future_meta,
                                    tracker=tracker,
                                    limit=region_write_concurrency,
                                )
                                data = cls._resolved_region_data(intent)
                                future = cast(
                                    Future[None],
                                    executor.submit(
                                        writer.write_region,
                                        group=intent.group,
                                        array_name=intent.array,
                                        ts_index=intent.ts_index,
                                        y_slice=intent.y_slice,
                                        data=data,
                                        channel_index=intent.channel_index,
                                    ),
                                )
                                pending.add(future)
                                future_meta[future] = _RegionFutureMeta(
                                    group=group_name,
                                    array=intent.array,
                                    ts_index=intent.ts_index,
                                    aligned=bool(aligned_by_intent.get(id(intent), False)),
                                )
                                continue

                            cls._drain_pending_region_writes(
                                pending=pending,
                                future_meta=future_meta,
                                tracker=tracker,
                            )
                            cls._dispatch_intent(writer, intent)
                            cls._record_timed_non_region_coverage(
                                tracker=tracker,
                                group_name=group_name,
                                intent=intent,
                                time_coord_name=time_coord_name,
                            )

                        cls._drain_pending_region_writes(
                            pending=pending,
                            future_meta=future_meta,
                            tracker=tracker,
                        )
                    except BaseException:
                        cls._cancel_and_drain_pending(pending)
                        raise

    @classmethod
    def _wait_for_region_capacity(
        cls,
        *,
        pending: set[Future[None]],
        future_meta: dict[Future[None], _RegionFutureMeta],
        tracker: CoverageTracker,
        limit: int,
    ) -> None:
        if len(pending) < limit:
            return
        done, not_done = wait(pending, return_when=FIRST_COMPLETED)
        pending.clear()
        pending.update(cast(set[Future[None]], not_done))
        cls._record_completed_region_futures(done, future_meta, tracker)

    @classmethod
    def _drain_pending_region_writes(
        cls,
        *,
        pending: set[Future[None]],
        future_meta: dict[Future[None], _RegionFutureMeta],
        tracker: CoverageTracker,
    ) -> None:
        if not pending:
            return
        done, not_done = wait(pending, return_when=ALL_COMPLETED)
        pending.clear()
        pending.update(cast(set[Future[None]], not_done))
        cls._record_completed_region_futures(done, future_meta, tracker)

    @staticmethod
    def _record_completed_region_futures(
        done: set[Future[None]],
        future_meta: dict[Future[None], _RegionFutureMeta],
        tracker: CoverageTracker,
    ) -> None:
        for future in done:
            future.result()
            meta = future_meta.pop(future)
            tracker.record_write(
                group=meta.group,
                arrays=[meta.array],
                ts_index=meta.ts_index,
                time_val=None,
                aligned=meta.aligned,
            )

    @staticmethod
    def _cancel_and_drain_pending(pending: set[Future[None]]) -> None:
        if not pending:
            return
        for future in pending:
            future.cancel()
        wait(pending, return_when=ALL_COMPLETED)
        for future in pending:
            try:
                future.result()
            except BaseException as exc:
                log.warning(
                    "Region write worker failed during cleanup: %s",
                    exc,
                    exc_info=True,
                )
        pending.clear()

    @staticmethod
    def _record_timed_non_region_coverage(
        *,
        tracker: CoverageTracker,
        group_name: str,
        intent: Any,
        time_coord_name: str,
    ) -> None:
        if intent.kind == "timestamp" and intent.timestamp_val is not None:
            tracker.record_write(
                group=group_name,
                arrays=[time_coord_name],
                ts_index=intent.ts_index,
                time_val=intent.timestamp_val,
                aligned=True,
            )
        elif intent.kind == "1d":
            is_time_coord = intent.array == time_coord_name
            time_val = (
                intent.timestamp_val if is_time_coord and intent.timestamp_val is not None else None
            )
            tracker.record_write(
                group=group_name,
                arrays=[intent.array],
                ts_index=intent.ts_index,
                time_val=time_val,
                aligned=True,
            )

    @staticmethod
    def _resolved_region_data(intent: Any) -> np.ndarray:
        return cast(np.ndarray, intent.data() if callable(intent.data) else intent.data)

    @classmethod
    def _count_declared_aligned_region_writes(
        cls,
        timed_intents: Sequence[Any],
        array_specs_by_path: Mapping[tuple[str, str], Any],
    ) -> int:
        count = 0
        for intent in timed_intents:
            if getattr(intent, "kind", None) != "region":
                continue
            try:
                spec = cls._array_spec_for_intent(intent, array_specs_by_path)
                if (
                    getattr(spec, "shards", None) is not None
                    or getattr(spec, "chunks", None) is None
                ):
                    continue
                selection = cls._validate_region_selection(
                    intent,
                    tuple(getattr(spec, "shape", ())),
                    source="declared schema",
                )
                _, aligned = physical_chunk_keys_for_region(
                    group=intent.group,
                    intent=intent,
                    shape=tuple(int(axis) for axis in spec.shape),
                    chunks=tuple(int(axis) for axis in spec.chunks),
                    selection=selection,
                )
            except ValueError:
                continue
            if aligned:
                count += 1
        return count

    @staticmethod
    def _array_spec_for_intent(
        intent: Any,
        array_specs_by_path: Mapping[tuple[str, str], Any],
    ) -> Any:
        key = (intent.group, intent.array)
        try:
            return array_specs_by_path[key]
        except KeyError as exc:
            raise ValueError(
                "Concurrent region write target is not declared in zarr_schema(): "
                f"group={intent.group!r} array={intent.array!r}"
            ) from exc

    @staticmethod
    def _chunks_for_opened_array(arr: Any, arr_path: str) -> tuple[int, ...]:
        chunks = getattr(arr, "chunks", None)
        if chunks is None:
            raise ValueError(
                "Concurrent region writes require known chunk metadata for "
                f"{arr_path!r}; opened array reports chunks=None."
            )
        chunk_tuple = tuple(int(axis) for axis in chunks)
        if len(chunk_tuple) != int(getattr(arr, "ndim", len(getattr(arr, "shape", ())))):
            raise ValueError(
                "Concurrent region writes require chunk rank to match array rank for "
                f"{arr_path!r}; chunks={chunk_tuple!r} shape={tuple(arr.shape)!r}."
            )
        if any(axis <= 0 for axis in chunk_tuple):
            raise ValueError(
                "Concurrent region writes require positive chunk sizes for "
                f"{arr_path!r}; chunks={chunk_tuple!r}."
            )
        return chunk_tuple

    @staticmethod
    def _validate_region_selection(
        intent: Any,
        shape: tuple[int, ...],
        *,
        source: str,
    ) -> _RegionSelection:
        rank = len(shape)
        if rank < 3:
            raise ValueError(
                "Concurrent region writes support arrays of rank 3 or more only: "
                f"group={intent.group!r} array={intent.array!r} {source} rank={rank}."
            )
        if any(axis <= 0 for axis in shape[1:]):
            raise ValueError(
                "Concurrent region writes require positive non-time dimensions: "
                f"group={intent.group!r} array={intent.array!r} {source} shape={shape!r}."
            )

        ts_index = IndexedRegionStrategy._as_non_negative_int(
            intent.ts_index,
            field="ts_index",
            intent=intent,
        )
        y_slice = intent.y_slice
        if not isinstance(y_slice, slice):
            raise ValueError(
                "Concurrent region writes require y_slice to be a slice: "
                f"group={intent.group!r} array={intent.array!r} y_slice={y_slice!r}."
            )
        if y_slice.step not in (None, 1):
            raise ValueError(
                "Concurrent region writes require contiguous y slices with step 1: "
                f"group={intent.group!r} array={intent.array!r} y_slice={y_slice!r}."
            )
        y_len = shape[1]
        y_start = IndexedRegionStrategy._slice_endpoint(
            y_slice.start,
            default=0,
            field="y_slice.start",
            intent=intent,
        )
        y_stop = IndexedRegionStrategy._slice_endpoint(
            y_slice.stop,
            default=y_len,
            field="y_slice.stop",
            intent=intent,
        )
        if y_start < 0 or y_stop < 0 or y_start >= y_stop or y_stop > y_len:
            raise ValueError(
                "Concurrent region write y_slice is outside the target array: "
                f"group={intent.group!r} array={intent.array!r} {source} "
                f"shape={shape!r} y_slice={y_slice!r}."
            )

        channel_index = getattr(intent, "channel_index", None)
        if rank == 3:
            if channel_index is not None:
                raise ValueError(
                    "Concurrent rank-3 region writes must not set channel_index: "
                    f"group={intent.group!r} array={intent.array!r} "
                    f"channel_index={channel_index!r}."
                )
            return _RegionSelection(ts_index, y_start, y_stop, None)

        if channel_index is None:
            return _RegionSelection(ts_index, y_start, y_stop, None)
        if rank > 4:
            raise ValueError(
                "Concurrent region writes support channel_index for rank-4 arrays "
                f"only: group={intent.group!r} array={intent.array!r} rank={rank} "
                f"channel_index={channel_index!r}."
            )
        channel = IndexedRegionStrategy._as_non_negative_int(
            channel_index,
            field="channel_index",
            intent=intent,
        )
        if channel >= shape[3]:
            raise ValueError(
                "Concurrent region write channel_index is outside the target array: "
                f"group={intent.group!r} array={intent.array!r} {source} "
                f"shape={shape!r} channel_index={channel_index!r}."
            )
        return _RegionSelection(ts_index, y_start, y_stop, channel)

    @staticmethod
    def _as_non_negative_int(value: Any, *, field: str, intent: Any) -> int:
        try:
            resolved = operator_index(value)
        except TypeError as exc:
            raise ValueError(
                "Concurrent region writes require integer selection values: "
                f"group={intent.group!r} array={intent.array!r} {field}={value!r}."
            ) from exc
        if resolved < 0:
            raise ValueError(
                "Concurrent region writes require non-negative selection values: "
                f"group={intent.group!r} array={intent.array!r} {field}={value!r}."
            )
        return int(resolved)

    @staticmethod
    def _slice_endpoint(value: Any, *, default: int, field: str, intent: Any) -> int:
        if value is None:
            return default
        return IndexedRegionStrategy._as_non_negative_int(value, field=field, intent=intent)

    @classmethod
    def _dispatch_static_intent(
        cls,
        writer: RegionZarrWriter,
        intent: Any,
        *,
        stamp_scope: Callable[[], contextlib.AbstractContextManager[MetadataAuthority | None]]
        | None = None,
    ) -> None:
        """Dispatch a ``kind="static"`` intent with idempotent resume semantics.

        Static arrays are write-once: a re-run on the same store must either
        replay the identical bytes (no-op) or fail loudly.

        ``ensure_group`` pre-creates the static array at its declared shape
        (filled with ``fill_value``) during schema setup, so the array always
        exists by the time this runs. "Was this committed in a prior run?"
        therefore cannot be answered from array contents — an all-fill array is
        indistinguishable from legitimate all-fill data (e.g. an all-NaN/NaT
        placeholder mask), and inferring freshness from contents would silently
        overwrite such data on resume. Instead firecube stamps the reserved
        ``firecube_static_written`` marker attr once the write commits:

        - marker absent  → never committed (a fresh shell, or a crash before
          commit): write the data, then stamp the marker. The full array is not
          read, so first writes stay O(1) in store reads.
        - marker present → committed previously: the incoming data must replay
          the identical bytes (NaN/NaT-aware via ``equal_nan=True``), else
          `SchemaDriftError`. Re-writing is skipped on an exact match.

        Marker check, comparison, write and marker stamp run as one
        transaction under the writer's lock on the whole array, so concurrent
        batches of one run (``pipeline_workers``) take it in turn within the
        process. The array is opened inside the lock. The batch that takes the
        lock second finds the marker and compares as a resume does: an
        identical payload is a no-op, a different one raises
        `SchemaDriftError`. Which payload stays depends on scheduling.

        The marker is stamped through `RegionZarrWriter.update_array_attrs`,
        which reads the node from the store under its node lock: a concurrent
        schema step's metadata write of the same array (a ``_FillValue``
        repair) cannot erase the marker, nor the marker that write. It is
        stamped under the authority *stamp_scope* yields, entered around the
        stamp alone and inside the whole-array lock (the writer's own
        ``PROCESS_LOCAL`` authority when it yields ``None`` or is omitted).
        The run's schema claim, which serialises schema steps, is therefore
        never held while the payload is written.
        """
        arr_path = f"{intent.group}/{intent.array}"
        # callable() is the right duck-type check; typing.Callable would treat
        # some numpy objects with __call__ descriptors as lazy payloads.
        data = cast(np.ndarray, intent.data() if callable(intent.data) else intent.data)
        if writer.verify_engine_written_coordinate(intent.group, intent.array, data):
            return  # preallocate's coordinate: verified, neither written nor marked
        root = writer._open_root()
        with writer._hold_array(arr_path):
            try:
                arr = root[arr_path]  # read after the lock: another batch may have stamped it
            except KeyError:
                arr = None
            if arr is not None and bool(arr.attrs.get(_STATIC_WRITTEN_ATTR, False)):
                existing = np.asarray(arr[...])  # [...] also reads a rank-0 array
                if not _arrays_equal_missing_aware(existing, data):
                    raise SchemaDriftError(
                        f"Static array {arr_path!r} diverged from existing data on "
                        "resume, or from another batch of this run. Re-ingest from "
                        "scratch; static arrays are write-once."
                    )
                return
            writer.write_static(group=intent.group, array_name=intent.array, data=data)
            scope = stamp_scope() if stamp_scope is not None else contextlib.nullcontext(None)
            with scope as authority:
                writer.update_array_attrs(
                    arr_path, {_STATIC_WRITTEN_ATTR: True}, authority=authority
                )

    @staticmethod
    def _dispatch_intent(writer: RegionZarrWriter, intent: Any) -> None:
        """Route a single ``WriteIntent`` to the appropriate writer method."""
        if intent.kind == "region":
            data = cast(np.ndarray, intent.data() if callable(intent.data) else intent.data)
            writer.write_region(
                group=intent.group,
                array_name=intent.array,
                ts_index=intent.ts_index,
                y_slice=intent.y_slice,
                data=data,
                channel_index=intent.channel_index,
            )
        elif intent.kind == "1d":
            data = cast(np.ndarray, intent.data() if callable(intent.data) else intent.data)
            writer.write_1d(
                group=intent.group,
                array_name=intent.array,
                ts_index=intent.ts_index,
                data=data,
            )
        elif intent.kind == "timestamp":
            writer.write_timestamp(
                group=intent.group,
                ts_index=intent.ts_index,
                timestamp_val=intent.timestamp_val,
            )
        elif intent.kind == "static":
            data = cast(np.ndarray, intent.data() if callable(intent.data) else intent.data)
            writer.write_static(
                group=intent.group,
                array_name=intent.array,
                data=data,
            )
        else:
            raise ValueError(f"Unknown WriteIntent kind: {intent.kind!r}")


__all__ = ["IndexedRegionStrategy"]
