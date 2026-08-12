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

"""Direct-Zarr ingestor template for streaming region-based writes.

Provides ``DirectZarrIngestor`` — an abstract base for plugins that bypass
xarray and write directly to Zarr regions using ``RegionZarrWriter``.  The
plugin implements two hooks plus optional Phase 3 slot-range parallelism:

- ``zarr_schema(ctx)`` — declares groups, arrays, and shapes.
- ``build_write_intents(batch, ctx)`` — converts a batch into a list of
  ``WriteIntent`` specs describing region writes to execute.

The template handles store opening, claim coordination, coverage tracking, and metrics.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import random
import time
from abc import abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

import numpy as np

from firecube.core.errors import ClaimConflictError
from firecube.core.slot_index import SlotIndexModel
from firecube.ingestor.api import (
    BaseIngestor,
    ConfigurationError,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
    SchemaDriftError,
    SchemaSizeMismatchError,
    WriteDomain,
    ZarrTemplateConfig,
    merge_batch_metrics,
)
from firecube.ingestor.runtime.zarr.strategies.indexed_region import RegionZarrWriter
from firecube.ingestor.runtime.zarr.write import derive_effective_codecs_for_spec
from firecube.ingestor.types import write_mode_policy

log = logging.getLogger(__name__)


def _ctx_product_name(ctx: PluginContext, default: str) -> str:
    storage = ctx.storage
    if storage is not None and storage.output is not None:
        return str(storage.output.product.product_name)
    return default


def _normalized_fill_value(fill_value: Any) -> Any:
    if fill_value is None:
        return None
    try:
        if bool(np.isnan(fill_value)):
            return "nan"
    except (TypeError, ValueError):
        pass
    return repr(fill_value)


def _compute_schema_hash(schema: Sequence[Any], global_expected: dict[str, int]) -> str:
    """Return a deterministic short hash for the declared Zarr schema shape contract.

    Audit-only fingerprint: emitted into run metadata for post-hoc diffing across
    runs. Never consulted for drift enforcement or write gating — codec drift is
    detected at write time by ``compare_pipelines`` against on-disk codecs.

    Codec fields (``filters``/``serializer``/``compressors``) are folded into the
    per-array record ONLY when at least one is declared, so schemas that omit
    them hash identically to pre-codec-parity baselines (backward-compatible,
    same pattern as group-level ``attrs`` below).
    """
    from firecube.core.zarr.codec_pipeline import normalize_codec_dict

    records: list[dict[str, Any]] = []
    expected_groups = set(global_expected)
    for group_spec in sorted(schema, key=lambda spec: str(spec.group)):
        if expected_groups and group_spec.group not in expected_groups:
            continue
        for arr_spec in sorted(group_spec.arrays, key=lambda arr: str(arr.name)):
            record: dict[str, Any] = {
                "group": group_spec.group,
                "array": arr_spec.name,
                "dtype": str(np.dtype(arr_spec.dtype)),
                "rank": len(arr_spec.shape),
                "shape_non_time": list(arr_spec.shape[1:]),
                "chunks": list(arr_spec.chunks) if arr_spec.chunks is not None else None,
                "fill_value": _normalized_fill_value(arr_spec.fill_value),
                "shards": list(arr_spec.shards) if arr_spec.shards is not None else None,
                "attrs": dict(sorted(arr_spec.attrs.items()))
                if arr_spec.attrs is not None
                else None,
                "dimension_names": list(arr_spec.dimension_names)
                if arr_spec.dimension_names is not None
                else None,
                "time_indexed": bool(arr_spec.time_indexed),
            }
            # Codec fields — only included when at least one is declared.
            # Omitting when all are None preserves byte-identical hashes for
            # legacy schemas (same pattern as group-level attrs below).
            spec_filters = getattr(arr_spec, "filters", None)
            spec_serializer = getattr(arr_spec, "serializer", None)
            spec_compressors = getattr(arr_spec, "compressors", None)
            if (
                spec_filters is not None
                or spec_serializer is not None
                or spec_compressors is not None
            ):
                record["filters"] = (
                    [normalize_codec_dict(f) for f in spec_filters]
                    if spec_filters is not None
                    else None
                )
                record["serializer"] = (
                    normalize_codec_dict(spec_serializer) if spec_serializer is not None else None
                )
                record["compressors"] = (
                    [normalize_codec_dict(c) for c in spec_compressors]
                    if spec_compressors is not None
                    else None
                )
            records.append(record)
        # Fold group-level attrs into the identity ONLY when present, so schemas
        # that declare none hash identically to before (backward-compatible).
        group_attrs = getattr(group_spec, "attrs", None)
        if group_attrs:
            records.append(
                {
                    "group": group_spec.group,
                    "group_attrs": dict(sorted(group_attrs.items())),
                }
            )
    payload = json.dumps(records, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _schema_dimension_names(arr_spec: Any, time_coord_name: str) -> tuple[str, ...] | None:
    explicit = getattr(arr_spec, "dimension_names", None)
    if explicit is not None:
        return explicit
    shape = tuple(getattr(arr_spec, "shape", ()))
    if getattr(arr_spec, "name", None) == time_coord_name and len(shape) == 1:
        return (time_coord_name,)
    return None


def _setup_global_zarr_schema(
    *,
    strategy: Any,
    schema: Sequence[Any],
    global_expected: dict[str, int],
    product: str,
    run_id: str,
    chunk_manager: Any,
    time_coord_name: str = "timestamp",
    template_config: Any = None,
) -> None:
    """Ensure direct-Zarr arrays are sized for the full parallel run.

    When ``template_config`` is provided, per-array codec declarations are
    validated against the template compression setting BEFORE any array is
    created. Invalid combinations raise ``ValueError``, leaving the target
    store untouched.
    """
    # Validate per-array codec declarations against template BEFORE creating any arrays.
    # Lazy import avoids a circular import between direct_zarr and templates.config.
    if template_config is not None:
        from firecube.ingestor.templates.config import validate_zarr_specs_against_template

        all_specs = [arr for group_spec in schema for arr in group_spec.arrays]
        validate_zarr_specs_against_template(all_specs, template_config)

    from firecube.core.uris import storage_uri_from_target
    from firecube.core.zarr.region_writer import group_schema_satisfied
    from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
        _session_for_store,
    )

    schema_has_time_indexed = any(
        arr.time_indexed for group_spec in schema for arr in group_spec.arrays
    )
    if not global_expected and schema_has_time_indexed:
        raise ConfigurationError("Parallel global schema is empty; expected group sizes.")

    for group_name, expected_time_count in global_expected.items():
        if expected_time_count <= 0:
            raise ConfigurationError(
                f"Parallel global schema has non-positive count for group {group_name!r}: "
                f"{expected_time_count}"
            )

    def _make_writer(group_name: str) -> RegionZarrWriter:
        zarr_store = None
        storage_config = getattr(strategy, "_storage_config", None)
        if storage_config:
            session = getattr(strategy, "_session", None) or _session_for_store(
                strategy._store_uri, storage_config
            )
            zarr_store = session.zarr.create_store(
                uri=storage_uri_from_target(strategy._store_uri),
                mode="a",
            ).store
        coord_names = getattr(strategy, "_coord_names_by_group", {}).get(
            group_name, frozenset({"y", "x", "channel"})
        )
        return RegionZarrWriter(
            strategy._store_uri,
            store=zarr_store,
            coord_names=coord_names,
            time_coord_name=time_coord_name,
        )

    schema_by_group = {spec.group: spec for spec in schema}
    for group_name, group_spec in schema_by_group.items():
        expected_time_count = global_expected.get(group_name)
        group_has_time_indexed = any(arr.time_indexed for arr in group_spec.arrays)
        if expected_time_count is None:
            if group_has_time_indexed:
                log.warning(
                    "Group '%s' has time-indexed arrays in zarr_schema() but is missing from "
                    "global_expected_time_count(); skipping global schema pre-allocation. "
                    "If this group receives parallel writes, add it to global_expected_time_count().",
                    group_name,
                )
                continue
            # Static-only group: process without a time count. The sentinel value is
            # never consumed because verify_array_spec skips shape[0] checks for
            # time_indexed=False specs and the per-array effective_shape below
            # uses arr_spec.shape directly for static arrays.
            expected_time_count = 0

        # Read-only fast path: setup is idempotent, so if the arrays already exist
        # and match (the `firecube zarr preallocate` workflow, or a peer pod that
        # already ran setup) there is nothing to mutate. Skip the exclusive claim
        # entirely so simultaneous slot-range pods do not race on it. The claim is
        # only taken below when a real mutation (array creation) is required.
        if group_schema_satisfied(
            _make_writer(group_name), group_name, group_spec.arrays, expected_time_count
        ):
            log.warning(
                "Parallel evidence: stage=schema_verify group=%s expected_shape=%s status=%s",
                group_name,
                global_expected.get(group_name, "static-only"),
                "verified",
            )
            continue

        domain = WriteDomain(
            product=product,
            category="zarr_schema_global",
            name=f"{group_name}:setup",
        )
        # Bounded retry for the exclusive schema-setup claim.
        # Pattern mirrors ChunkManager.ensure_slot_index_model().
        # On ClaimConflictError, re-check convergence before sleeping/retrying:
        # another pod may have finished schema setup while we were waiting.
        _schema_max_retries = 5
        _schema_backoff = 0.1
        for _schema_attempt in range(_schema_max_retries + 1):
            try:
                claim_ctx = (
                    chunk_manager.acquire_claim(
                        product=product,
                        domain=domain,
                        owner_id=f"{run_id}:{group_name}:schema_global",
                    )
                    if chunk_manager is not None
                    else contextlib.nullcontext()
                )
                with claim_ctx:
                    # Re-check INSIDE the claim: another pod may have converged
                    # between the outer satisfied-check and claim acquisition.
                    if group_schema_satisfied(
                        _make_writer(group_name),
                        group_name,
                        group_spec.arrays,
                        expected_time_count,
                    ):
                        log.warning(
                            "Parallel evidence: stage=schema_verify group=%s expected_shape=%s status=%s",
                            group_name,
                            global_expected.get(group_name, "static-only"),
                            "converged_under_claim",
                        )
                        break
                    writer = _make_writer(group_name)
                    existing_shape: tuple[int, ...] | None = None
                    newly_created = False
                    for arr_spec in group_spec.arrays:
                        if arr_spec.time_indexed:
                            effective_shape = (expected_time_count, *arr_spec.shape[1:])
                        else:
                            effective_shape = arr_spec.shape
                        group_path = f"{group_name}/{arr_spec.name}"
                        root = writer._open_root()
                        path_parts = [p for p in group_path.split("/") if p]
                        current = root
                        for part in path_parts[:-1]:
                            if part not in current:
                                current = None
                                break
                            current = current[part]
                        arr_name = path_parts[-1]
                        array_exists = current is not None and arr_name in current
                        _eff_filters, _eff_serializer, _eff_compressors = (
                            derive_effective_codecs_for_spec(arr_spec, template_config)
                        )
                        arr = writer.ensure_group(
                            group_path,
                            shape=effective_shape,
                            dtype=arr_spec.dtype,
                            fill_value=arr_spec.fill_value,
                            chunks=arr_spec.chunks,
                            shards=arr_spec.shards,
                            attrs=arr_spec.attrs,
                            dimension_names=_schema_dimension_names(arr_spec, time_coord_name),
                            filters=_eff_filters,
                            serializer=_eff_serializer,
                            compressors=_eff_compressors,
                        )
                        if not array_exists:
                            newly_created = True
                        if existing_shape is None:
                            existing_shape = getattr(arr, "shape", None) if array_exists else None
                        if array_exists:
                            try:
                                writer.verify_array_spec(group_path, arr_spec, expected_time_count)
                            except SchemaDriftError as exc:
                                if (
                                    arr_spec.time_indexed
                                    and arr.ndim > 0
                                    and arr.shape[0] < expected_time_count
                                ):
                                    raise SchemaSizeMismatchError(
                                        f"Schema drift for group {group_name}: existing array shape[0]={arr.shape[0]} "
                                        f"< global_expected={expected_time_count}. Run `firecube zarr preallocate` first."
                                    ) from exc
                                raise
                    writer.set_group_attrs(group_name, getattr(group_spec, "attrs", None))
                    log.warning(
                        "Parallel evidence: stage=schema_verify group=%s existing_shape=%s expected_shape=%s status=%s",
                        group_name,
                        existing_shape,
                        global_expected.get(group_name, "static-only"),
                        "created" if newly_created else "verified",
                    )
                break  # claim released normally -> success
            except ClaimConflictError:
                # Another pod holds the claim. Re-check WITHOUT the claim:
                # they may have finished while we waited.
                if group_schema_satisfied(
                    _make_writer(group_name),
                    group_name,
                    group_spec.arrays,
                    expected_time_count,
                ):
                    log.warning(
                        "Parallel evidence: stage=schema_verify group=%s expected_shape=%s status=%s",
                        group_name,
                        global_expected.get(group_name, "static-only"),
                        "converged_after_conflict",
                    )
                    break  # schema is done; move to next group
                if _schema_attempt == _schema_max_retries:
                    raise  # propagate; pod must fail
                time.sleep(_schema_backoff + random.random() * _schema_backoff)
                _schema_backoff *= 2


@dataclass(frozen=True)
class ZarrArraySpec:
    """Specification for a single Zarr array within a group."""

    name: str
    shape: tuple[int, ...]
    dtype: np.dtype[Any] | type[np.generic] | type[Any] | str
    chunks: tuple[int, ...] | None = None
    fill_value: Any = None
    expected_time_count: int | None = None
    shards: tuple[int, ...] | None = None
    attrs: Mapping[str, Any] | None = None
    dimension_names: tuple[str, ...] | None = None
    time_indexed: bool = True
    filters: tuple[dict, ...] | None = None
    """Per-array Zarr v3 filter codecs (ArrayArrayCodec). Each entry: ``{"name": ..., "configuration": {...}}``.
    None means inherit from template default. Requires template ``zarr_compression=True``.
    """
    serializer: dict | None = None
    """Per-array Zarr v3 serializer codec (ArrayBytesCodec). E.g. ``{"name": "bytes"}``.
    None means inherit from template default. Requires template ``zarr_compression=True``.
    """
    compressors: tuple[dict, ...] | None = None
    """Per-array Zarr v3 compressor codecs (BytesBytesCodec). Each entry: ``{"name": ..., "configuration": {...}}``.
    Empty tuple ``()`` = explicitly uncompressed for this array ("compress-except-X" pattern).
    None means inherit from template default. Requires template ``zarr_compression=True``.
    """

    def __post_init__(self) -> None:
        if self.expected_time_count is not None and self.expected_time_count < 0:
            raise ValueError(
                f"expected_time_count must be non-negative, got {self.expected_time_count}"
            )
        if self.expected_time_count is not None and not self.time_indexed:
            raise ValueError(
                "ZarrArraySpec.expected_time_count is only valid when time_indexed=True; "
                "set expected_time_count=None for static arrays "
                f"(got expected_time_count={self.expected_time_count!r}, time_indexed={self.time_indexed!r})."
            )
        if self.shards is not None and len(self.shards) != len(self.shape):
            raise ValueError(
                f"shards length {len(self.shards)} must match shape rank {len(self.shape)}"
            )
        if self.dimension_names is not None and len(self.dimension_names) != len(self.shape):
            raise ValueError(
                f"dimension_names length {len(self.dimension_names)} must match shape rank {len(self.shape)}"
            )
        if self.attrs is not None and not isinstance(self.attrs, Mapping):
            raise ValueError(f"attrs must be a Mapping, got {type(self.attrs).__name__}")
        if self.filters is not None:
            if not isinstance(self.filters, tuple):
                raise ValueError(
                    f"ZarrArraySpec.filters must be a tuple of dicts, got {type(self.filters).__name__}"
                )
            for i, f in enumerate(self.filters):
                if not isinstance(f, dict):
                    raise ValueError(
                        f"ZarrArraySpec.filters[{i}] must be a dict, got {type(f).__name__}"
                    )
        if self.serializer is not None:
            if not isinstance(self.serializer, dict):
                raise ValueError(
                    f"ZarrArraySpec.serializer must be a dict, got {type(self.serializer).__name__}"
                )
            if "name" not in self.serializer:
                raise ValueError("ZarrArraySpec.serializer must have a 'name' key")
        if self.compressors is not None:
            if not isinstance(self.compressors, tuple):
                raise ValueError(
                    "ZarrArraySpec.compressors must be a tuple of dicts (or empty tuple for uncompressed), "
                    f"got {type(self.compressors).__name__}"
                )
            for i, c in enumerate(self.compressors):
                if not isinstance(c, dict):
                    raise ValueError(
                        f"ZarrArraySpec.compressors[{i}] must be a dict, got {type(c).__name__}"
                    )


@dataclass(frozen=True)
class ZarrGroupSpec:
    """Specification for a Zarr group and its arrays.

    Returned by ``DirectZarrIngestor.zarr_schema()`` to describe the full layout.

    ``attrs`` is optional, convention-agnostic group-level metadata stamped onto
    the group's ``zarr.json`` at schema setup — e.g. dataset-level attributes a
    plugin chooses to publish. Firecube writes the mapping verbatim and does not
    interpret it (no convention is assumed); reserved firecube-internal attribute
    names are rejected at write time.
    """

    group: str
    arrays: list[ZarrArraySpec] = field(default_factory=list)
    coord_names: frozenset[str] = frozenset({"y", "x", "channel"})
    attrs: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.attrs is not None and not isinstance(self.attrs, Mapping):
            raise ValueError(f"attrs must be a Mapping, got {type(self.attrs).__name__}")


@dataclass(frozen=True)
class WriteIntent:
    """A single write operation to execute against the Zarr store.

    Produced by ``DirectZarrIngestor.build_write_intents()`` to describe
    what data should be written, where, and at which timestamp index.

    The ``kind`` field selects the write method on ``RegionZarrWriter``.
    Here, "timestamp" means the conceptual time/index axis, not the on-disk
    dimension name; the latter is configured separately via
    ``IndexedRegionStrategy.time_coord_name``.

    - ``"region"`` → ``write_region(group, array, ts_index, y_slice, data, channel_index=...)``
    - ``"1d"`` → ``write_1d(group, array, ts_index, data)``
    - ``"timestamp"`` → ``write_timestamp(group, ts_index, timestamp_val)``
    - ``"static"`` → ``write_static(group, array_name, data)`` (non-time-indexed; ``ts_index`` is ignored)
    """

    group: str
    array: str
    ts_index: int
    data: np.ndarray | Any
    kind: str = "region"
    y_slice: slice | None = None
    channel_index: int | None = None
    timestamp_val: Any = None
    """Conceptual time/index axis value for ``kind="timestamp"`` writes.

    "timestamp" is a stable plugin-contract token, not the on-disk dim name;
    the actual dim/coord name comes from ``IndexedRegionStrategy.time_coord_name``.
    """


class DirectZarrIngestor(BaseIngestor):
    """Abstract template for direct-Zarr region-based ingestors.

    Plugins that write directly to Zarr (bypassing xarray) should subclass
    this template and implement:

    - :meth:`zarr_schema` — declare groups and arrays.
    - :meth:`build_write_intents` — convert a batch into write operations.

    The template orchestrates store setup, write execution via a region write
    strategy, coverage tracking, and metrics aggregation.
    """

    template_config_class = ZarrTemplateConfig

    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = False
    """Explicit opt-in for within-group parallel ingestion.

    Set to True and override ``timestamp_to_ts_index()`` and
    ``global_expected_time_count()`` to enable slot-range parallelism.
    """

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        """Globally deterministic mapping from the conceptual time axis to ts_index.

        "timestamp" here is the stable contract token, not the on-disk dim name.
        """
        raise NotImplementedError(
            f"{type(self).__name__} did not implement timestamp_to_ts_index. "
            "Override this method and set SUPPORTS_SLOT_RANGE_PARALLELISM = True "
            "to enable parallel ingestion."
        )

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int] | None:
        """Return max ts_index + 1 per group across the planned run."""
        return None

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        """Return the slot-index model governing this product's time axes.

        Plugins that set ``SUPPORTS_SLOT_RANGE_PARALLELISM = True`` MUST
        override this method.  The returned model is persisted to the
        control plane and mirrored as Zarr root attributes; it becomes the
        canonical identity for all concurrent writers targeting this product.

        Default implementation raises :class:`NotImplementedError` so that
        non-parallel plugins calling this accidentally receive a clear error
        rather than silent incorrect behaviour.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.slot_index_model(ctx) must be overridden "
            "by plugins that set SUPPORTS_SLOT_RANGE_PARALLELISM = True."
        )

    def filter_items_to_slot_range(
        self,
        items: Sequence[Any],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> Sequence[Any]:
        """Filter source items to those whose ts_index falls in [slot_start, slot_end).

        Strongly recommended for parallel ingestion. The default passthrough
        returns all items unchanged; if ``build_write_intents()`` then emits any
        WriteIntent whose ts_index falls outside ``[slot_start, slot_end)``, the
        post-intent assertion will raise ``WriteIntentRangeError`` and FAIL the
        batch (it does NOT silently discard out-of-range intents).
        """
        return items

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Declaration-time validation for parallel ingestion opt-in."""
        super().__init_subclass__(**kwargs)
        if not cls.__dict__.get("SUPPORTS_SLOT_RANGE_PARALLELISM", False):
            return
        for method_name in (
            "timestamp_to_ts_index",
            "global_expected_time_count",
            "slot_index_model",
        ):
            base_method = getattr(DirectZarrIngestor, method_name)
            cls_method = getattr(cls, method_name)
            if cls_method is base_method:
                raise TypeError(
                    f"Class {cls.__name__} declares SUPPORTS_SLOT_RANGE_PARALLELISM=True "
                    f"but does not override {method_name}. "
                    f"Either override the method or set SUPPORTS_SLOT_RANGE_PARALLELISM=False."
                )

    @abstractmethod
    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        """Declare the Zarr store layout for this ingestor.

        Returns a list of group specifications describing every group and
        array that the ingestor may write to.  Called once per batch to
        ensure groups and arrays exist before writes begin.

        The declared groups are also the ingestor's write groups, so the
        schema must cover every group any ``WriteIntent`` targets.

        Examples:
            Declare one group with a time-indexed array and its time axis:

                def zarr_schema(self, ctx):
                    n_times = self.global_expected_time_count(ctx)
                    return [
                        ZarrGroupSpec(
                            group="FWI",
                            arrays=[
                                ZarrArraySpec(
                                    name="fire_risk",
                                    shape=(n_times, 550, 475),
                                    dtype="float32",
                                    chunks=(1, 550, 475),
                                    dimension_names=("timestamp", "y", "x"),
                                ),
                                ZarrArraySpec(
                                    name="timestamp",
                                    shape=(n_times,),
                                    dtype="int64",
                                    dimension_names=("timestamp",),
                                ),
                            ],
                        )
                    ]
        """

    def _cached_zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        cache = getattr(self, "_zarr_schema_cache_by_ctx", None)
        if cache is None:
            self._zarr_schema_cache_by_ctx = cache = {}
        key = id(getattr(ctx, "_ctx", ctx))
        if key not in cache:
            cache[key] = self.zarr_schema(ctx)
        return cache[key]

    def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
        """Return the sorted set of groups declared by ``zarr_schema(ctx)``.

        On this template the group set is schema-declared, not item-derived:
        writes are routed by ``WriteIntent.group``. Plugins should NOT
        override this method — a hand-rolled group list can disagree with
        the declared schema and break group/schema agreement.
        """
        _ = items  # group set is schema-declared, not item-derived
        specs = self._cached_zarr_schema(ctx)
        return sorted({spec.group for spec in specs})

    @abstractmethod
    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        """Convert a batch into a list of write operations.

        Each ``WriteIntent`` describes a single region write, 1-D write,
        or timestamp write.  The template will execute them in order via
        the configured region write strategy.

        Return an empty list to skip the batch.

        Every intent's ``group`` must exist in ``zarr_schema(ctx)``, and
        ``ts_index`` must fall inside the worker's slot range when
        slot-range parallelism is enabled.

        Examples:
            Emit the data write and its timestamp for each item:

                def build_write_intents(self, batch, ctx):
                    intents = []
                    for item in batch.items:
                        array, stamp = read_product(ctx.materialize(item))
                        ts_index = self.timestamp_to_ts_index(stamp, ctx)
                        intents.append(
                            WriteIntent(
                                group="FWI",
                                array="fire_risk",
                                ts_index=ts_index,
                                data=array,
                            )
                        )
                        intents.append(
                            WriteIntent(
                                group="FWI",
                                array="timestamp",
                                ts_index=ts_index,
                                data=None,
                                kind="timestamp",
                                timestamp_val=stamp,
                            )
                        )
                    return intents
        """

    def _ensure_slot_index_model_at_startup(self, ctx: PluginContext) -> None:
        """Ensure the product slot-index model is stamped before any array write."""
        if not getattr(type(self), "SUPPORTS_SLOT_RANGE_PARALLELISM", False):
            return
        if self._slot_index_model_stamped:
            return

        product = _ctx_product_name(ctx, self.name)
        run_id = str(ctx.run_id or ctx.option("run_id", "unknown"))
        slot_model = self.slot_index_model(ctx)
        self._chunk_manager.ensure_slot_index_model(
            product=product,
            model=slot_model,
            run_id=run_id,
        )
        self._slot_index_model_stamped = True

    def _verify_schema_at_pod_startup(self, ctx: PluginContext) -> None:
        """Verify global direct-Zarr schema once per pod process before batches run."""
        from firecube.ingestor.api import IndexedRegionStrategy

        if self._parallel_execution_state is None:
            return

        write_mode = self.engine_config.write_mode
        store_uri = self.resolve_output_uri(ctx, write_mode=write_mode)
        product = _ctx_product_name(ctx, self.name)
        run_id = str(ctx.run_id or ctx.option("run_id", "unknown"))

        schema = self._cached_zarr_schema(ctx)
        coord_names_by_group = {spec.group: spec.coord_names for spec in schema}
        strategy = IndexedRegionStrategy(
            store_uri=store_uri,
            schema=schema,
            coord_names_by_group=coord_names_by_group,
            time_coord_name=self._resolve_time_dim_name(),
            storage_config=self._chunk_manager.storage_config,
        )

        global_expected = self._parallel_execution_state.global_expected
        for group_name, expected_time_count in global_expected.items():
            if self._parallel_execution_state.schema_verified.get(group_name, False):
                continue
            filtered_group_specs = [spec for spec in schema if spec.group == group_name]
            if not filtered_group_specs:
                raise ConfigurationError(
                    f"_verify_schema_at_pod_startup: group '{group_name}' from global_expected "
                    f"is not declared in zarr_schema(). This should have been caught by "
                    f"validate_global_expected_subset_of_schema in the capability gate; "
                    f"this hard-fail is defense-in-depth."
                )
            _setup_global_zarr_schema(
                strategy=strategy,
                schema=filtered_group_specs,
                global_expected={group_name: expected_time_count},
                product=product,
                run_id=run_id,
                chunk_manager=self._chunk_manager,
                time_coord_name=self._resolve_time_dim_name(),
                template_config=getattr(self, "template_config", None),
            )
            self._parallel_execution_state.schema_verified[group_name] = True
            schema_hash = _compute_schema_hash(
                filtered_group_specs,
                {group_name: expected_time_count},
            )
            try:
                self._chunk_manager.record_schema_verification(
                    product=product,
                    run_id=run_id,
                    group=group_name,
                    plugin=self.name,
                    schema_hash=schema_hash,
                    verified_at=datetime.now(UTC).isoformat(),
                    expected_time_count=expected_time_count,
                    meta={"write_mode": str(write_mode)},
                )
            except Exception as exc:
                self._log.warning(
                    "Failed to record schema verification audit event for group %s: %s",
                    group_name,
                    exc,
                )

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> dict[str, Any]:
        """Aggregate run metrics using the default batch-merge policy."""
        return merge_batch_metrics(ctx, state)

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        """Execute direct-Zarr writes for one batch.

        Orchestrates: schema setup → intent generation → strategy execution →
        coverage and metrics assembly.
        """
        from firecube.ingestor.api import IndexedRegionStrategy

        try:
            write_mode = self.engine_config.write_mode
            store_uri = self.resolve_output_uri(ctx, write_mode=write_mode)
            product = _ctx_product_name(ctx, self.name)
            run_id = str(ctx.run_id or ctx.option("run_id", "unknown"))

            schema = self._cached_zarr_schema(ctx)

            # Validate per-array codec declarations against template before any write.
            # Lazy import avoids a circular import between direct_zarr and templates.config.
            template_cfg = getattr(self, "template_config", None)
            if template_cfg is not None:
                from firecube.ingestor.templates.config import (
                    validate_zarr_specs_against_template,
                )

                all_specs = [arr for group_spec in schema for arr in group_spec.arrays]
                validate_zarr_specs_against_template(all_specs, template_cfg)

            coord_names_by_group = {spec.group: spec.coord_names for spec in schema}

            intents = self.build_write_intents(batch, ctx)
            if not intents:
                return PipelineResult(
                    batch=batch,
                    outputs=OutputPaths(primary=str(store_uri)),
                    metrics={"count": 0},
                    success=True,
                )

            strategy = IndexedRegionStrategy(
                store_uri=store_uri,
                schema=schema,
                coord_names_by_group=coord_names_by_group,
                time_coord_name=self._resolve_time_dim_name(),
                storage_config=self._chunk_manager.storage_config,
            )
            codec_pipelines_by_array = {
                (group_spec.group, arr_spec.name): derive_effective_codecs_for_spec(
                    arr_spec,
                    getattr(self, "template_config", None),
                )
                for group_spec in schema
                for arr_spec in group_spec.arrays
            }

            group_to_intents: dict[str, list[WriteIntent]] = {}
            for intent in intents:
                group_to_intents.setdefault(intent.group, []).append(intent)

            def claim_for_group(group_name: str):
                """Acquire a schema setup claim for a group."""
                domain = WriteDomain(
                    product=product,
                    category="zarr_region",
                    name=f"{group_name}:schema",
                )
                return self._chunk_manager.acquire_claim(
                    product=product,
                    domain=domain,
                    owner_id=f"{run_id}:{group_name}",
                )

            def claim_for_slot(group_name: str, ts_index: int):
                """Acquire an intent-dispatch claim for a group timestamp slot."""
                domain = WriteDomain(
                    product=product,
                    category="zarr_region",
                    name=f"{group_name}:slot={ts_index}",
                )
                return self._chunk_manager.acquire_claim(
                    product=product,
                    domain=domain,
                    owner_id=f"{run_id}:{group_name}:slot={ts_index}",
                )

            slot_start = getattr(self.engine_config, "slot_start", None)
            slot_end = getattr(self.engine_config, "slot_end", None)
            slot_range = (
                (slot_start, slot_end)
                if isinstance(slot_start, int) and isinstance(slot_end, int)
                else None
            )

            if slot_range is not None and self.SUPPORTS_SLOT_RANGE_PARALLELISM:
                if self._parallel_execution_state is None:
                    raise ConfigurationError(
                        "Parallel mode active but _parallel_execution_state not initialized. "
                        "This indicates validate_parallel_capability() was not called. "
                        "Ensure the capability gate runs in BaseIngestor.run() before _process_batch."
                    )
                global_expected = self._parallel_execution_state.global_expected

                # Phase 3.1 T1: Strict coverage — every group receiving WriteIntents MUST be declared
                # in global_expected_time_count(). Without this, a group could be written to by
                # parallel pods but schema pre-allocation never happened, causing deadlock or corruption.
                # Extra groups in global_expected are ALLOWED (debug log only) — they are sidecars
                # (e.g., lat/lon groups that the plugin manages separately without slot-range intents).
                groups_with_intents: set[str] = set(group_to_intents.keys())
                groups_in_global: set[str] = set(global_expected.keys())
                missing_from_global = groups_with_intents - groups_in_global
                if missing_from_global:
                    raise ConfigurationError(
                        f"Parallel mode: groups {sorted(missing_from_global)} received WriteIntents "
                        f"but are not declared in global_expected_time_count() (declared: "
                        f"{sorted(groups_in_global)}). "
                        "Every group that receives writes must be declared in "
                        "global_expected_time_count() for safe parallel-pod schema pre-allocation."
                    )
                # Defense in depth: intent groups must also be in zarr_schema()
                groups_in_schema: set[str] = {spec.group for spec in schema}
                missing_from_schema = groups_with_intents - groups_in_schema
                if missing_from_schema:
                    raise ConfigurationError(
                        f"WriteIntents reference groups {sorted(missing_from_schema)} that are "
                        f"not declared in zarr_schema() ({sorted(groups_in_schema)}). "
                        "build_write_intents() must only emit intents for groups declared in "
                        "zarr_schema()."
                    )
                # Phase 3.3: extras_in_global cannot happen — validate_global_expected_subset_of_schema
                # in the capability gate ensures global_expected.keys() ⊆ zarr_schema() groups at startup.

            metrics = strategy.write_groups(
                group_to_intents=group_to_intents,
                schema=schema,
                claim_for_group=claim_for_group,
                claim_for_slot=claim_for_slot,
                slot_range=slot_range,
                slot_group=self.engine_config.slot_group,
                codec_pipelines_by_array=codec_pipelines_by_array,
            )

            final_metrics = {
                "zarr": metrics,
                "coverage": metrics.get("coverage", []),
                "count": len(intents),
                "storage_handled": write_mode_policy(write_mode).storage_handled_by_engine,
            }

            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=str(store_uri), zarr=str(store_uri)),
                metrics=final_metrics,
                success=True,
            )

        except Exception as exc:
            self._log.exception("Direct Zarr batch processing failed")
            return PipelineResult(
                batch=batch, outputs=OutputPaths(primary=""), success=False, error=str(exc)
            )


__all__ = [
    "DirectZarrIngestor",
    "WriteIntent",
    "ZarrArraySpec",
    "ZarrGroupSpec",
    "_compute_schema_hash",
    "_setup_global_zarr_schema",
]
