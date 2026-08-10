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
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import numpy as np

from firecube.core.errors import SchemaDriftError
from firecube.core.uris import storage_uri_from_target
from firecube.core.zarr.region_writer import (
    RegionZarrWriter,
    _arrays_equal_missing_aware,
)
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

# Reserved array attr stamped once a static (write-once) array's data has been
# committed. Its presence — not the array contents — is the authoritative
# "already written in a prior run" signal on resume. Registered in
# ``firecube.core.zarr._reserved_attrs.RESERVED_ARRAY_ATTRS``.
_STATIC_WRITTEN_ATTR = "firecube_static_written"


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
    ) -> dict[str, Any]:
        """Execute write intents grouped by Zarr group and return metrics.

        Parameters
        ----------
        group_to_intents:
            Mapping of group name to list of ``WriteIntent`` objects.
        schema:
            Group specifications for array creation.  Falls back to the
            constructor-provided schema if ``None``.
        claim_for_group:
            Optional callback returning a context manager for group/schema
            coordination.  When ``claim_for_slot`` is provided, this protects
            schema setup only.
        claim_for_slot:
            Optional callback returning a context manager for per-``ts_index``
            intent dispatch.  Falls back to ``claim_for_group`` and then a null
            context when omitted.
        slot_range:
            Optional half-open ``[start, end)`` slot range.  When provided, all
            intents are validated before any schema setup or writes occur.
        slot_group:
            Optional group name that this pod owns.  When set together with
            ``slot_range``, the post-intent assertion only validates intents
            for ``slot_group``; intents for other groups are skipped with a
            warning (not silently dropped) and no writes happen for them.

        Returns
        -------
        dict with ``"coverage"`` list and ``"duration_s"`` float.
        """
        effective_schema = schema if schema is not None else self._schema
        t0 = time.monotonic()
        tracker = CoverageTracker(time_dim_name=self._time_coord_name)

        schema_by_group: dict[str, Any] = {}
        for spec in effective_schema:
            schema_by_group[spec.group] = spec

        if slot_range is not None:
            slot_start, slot_end = slot_range
            all_intents = [
                intent
                for intents in group_to_intents.values()
                for intent in intents
                if hasattr(intent, "ts_index")
            ]
            for group_name, intents in group_to_intents.items():
                # Phase 3.1 T7: When slot_group is set, only validate that group's
                # intents; skip + warn for other groups (this pod does not own them).
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
                        # Static arrays carry no time semantics; ts_index is
                        # semantically ignored, so slot-range bounds do not apply.
                        continue
                    ts = getattr(intent, "ts_index", None)
                    if ts is not None and not (slot_start <= ts < slot_end):
                        from firecube.ingestor.errors import WriteIntentRangeError

                        raise WriteIntentRangeError(
                            f"Intent ts_index={ts} for group={group_name!r} is outside "
                            f"assigned slot_range=[{slot_start}, {slot_end}). "
                            "Plugin filter is advisory; this is a correctness violation."
                        )
            log_filter_evidence(
                log,
                stage="post_intent_assertion",
                planned_range=slot_range,
                original_count=len(all_intents),
                filtered_count=len(all_intents),
                dropped_count=0,
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
            zarr_store = None
            if self._storage_config:
                session = self._session or _session_for_store(self._store_uri, self._storage_config)
                zarr_store = session.zarr.create_store(
                    uri=storage_uri_from_target(self._store_uri),
                    mode="a",
                ).store
            writer = RegionZarrWriter(
                self._store_uri,
                store=zarr_store,
                coord_names=coord_names,
                time_coord_name=self._time_coord_name,
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
            # the §24 claim-then-ensure ordering byte-for-byte unchanged.
            parallel_mode = slot_range is not None
            schema_ctx = (
                contextlib.nullcontext()
                if parallel_mode or claim_for_group is None
                else claim_for_group(group_name)
            )
            with schema_ctx:
                if group_spec is not None and not parallel_mode:
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
                        writer.ensure_group(
                            f"{group_name}/{arr_spec.name}",
                            shape=effective_shape,
                            dtype=arr_spec.dtype,
                            fill_value=arr_spec.fill_value,
                            chunks=arr_spec.chunks,
                            allow_grow=allow_grow,
                            shards=arr_spec.shards,
                            attrs=arr_spec.attrs,
                            dimension_names=_schema_dimension_names(
                                arr_spec,
                                self._time_coord_name,
                            ),
                            filters=filters,
                            serializer=serializer,
                            compressors=compressors,
                        )
                    writer.set_group_attrs(group_name, getattr(group_spec, "attrs", None))

            static_intents = [
                intent for intent in intents if getattr(intent, "kind", None) == "static"
            ]
            timed_intents = [
                intent for intent in intents if getattr(intent, "kind", None) != "static"
            ]

            for intent in static_intents:
                self._dispatch_static_intent(writer, intent)

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

        coverage = [dataclasses.asdict(c) for c in tracker.build_coverage()]
        return {
            "coverage": coverage,
            "duration_s": time.monotonic() - t0,
        }

    @classmethod
    def _dispatch_static_intent(cls, writer: RegionZarrWriter, intent: Any) -> None:
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
          :class:`SchemaDriftError`. Re-writing is skipped on an exact match.
        """
        arr_path = f"{intent.group}/{intent.array}"
        root = writer._open_root()
        try:
            arr = root[arr_path]
        except KeyError:
            arr = None
        if arr is not None and bool(arr.attrs.get(_STATIC_WRITTEN_ATTR, False)):
            existing = np.asarray(arr[:])
            if not _arrays_equal_missing_aware(existing, intent.data):
                raise SchemaDriftError(
                    f"Static array {arr_path!r} diverged from existing data on "
                    "resume. Re-ingest from scratch; static arrays are write-once."
                )
            return
        cls._dispatch_intent(writer, intent)
        root[arr_path].attrs[_STATIC_WRITTEN_ATTR] = True

    @staticmethod
    def _dispatch_intent(writer: RegionZarrWriter, intent: Any) -> None:
        """Route a single ``WriteIntent`` to the appropriate writer method."""
        if intent.kind == "region":
            writer.write_region(
                group=intent.group,
                array_name=intent.array,
                ts_index=intent.ts_index,
                y_slice=intent.y_slice,
                data=intent.data,
                channel_index=intent.channel_index,
            )
        elif intent.kind == "1d":
            writer.write_1d(
                group=intent.group,
                array_name=intent.array,
                ts_index=intent.ts_index,
                data=intent.data,
            )
        elif intent.kind == "timestamp":
            writer.write_timestamp(
                group=intent.group,
                ts_index=intent.ts_index,
                timestamp_val=intent.timestamp_val,
            )
        elif intent.kind == "static":
            writer.write_static(
                group=intent.group,
                array_name=intent.array,
                data=intent.data,
            )
        else:
            raise ValueError(f"Unknown WriteIntent kind: {intent.kind!r}")


__all__ = ["IndexedRegionStrategy"]
