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

"""Domain types and constants for the Firecube control plane.

Defines the data structures used throughout the control-plane subsystem:
WriteDomain, ChunkEvent, ClaimInfo, RunInfo, SpanCoverage, ChunkInfo,
DeletionPlan, and the build_span_entry factory function.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from firecube.core.errors import ManifestError
from firecube.core.slot_index import SlotAxis, SlotIndexModel

SCHEMA_VERSION = "v2"
CONTROL_DIRNAME = ".firecube"
LATEST_POINTER = "LATEST.json"
SNAPSHOT_DIRNAME = "snapshots"
RUNS_DIRNAME = "runs"
CLAIMS_DIRNAME = "claims"
SCHEMA_FILENAME = "schema.json"
DEFAULT_RUN_STALE_THRESHOLD_S = 3600
EVENT_RUN_STARTED_WITH_REPLACEMENT = "run_started_with_replacement"
EVENT_REPLACEMENT_COMMITTED = "replacement_committed"
EVENT_MAINTENANCE_STARTED = "maintenance_started"
EVENT_MAINTENANCE_COMPLETED = "maintenance_completed"
EVENT_MAINTENANCE_FAILED = "maintenance_failed"
EVENT_RUN_STARTED = "run_started"
EVENT_RUN_COMPLETED = "run_completed"
EVENT_RUN_FAILED = "run_failed"
EVENT_RUN_ABANDONED = "run_abandoned"
EVENT_SPAN_COMMITTED = "span_committed"
EVENT_SPAN_FAILED = "span_failed"
EVENT_SPAN_NOOP = "span_noop"
EVENT_RECORD_REPLACED = "record_replaced"
EVENT_RECORD_UPSERT = "record_upsert"
EVENT_SCHEMA_VERIFICATION = "schema_verification"
SLOT_INDEX_DIRNAME = "slot_index"
SLOT_INDEX_CURRENT_FILENAME = "current.json"
EVENT_SLOT_INDEX_MODEL_RECORDED = "slot_index_model_recorded"
EVENT_SLOT_INDEX_MODEL_VERIFIED = "slot_index_model_verified"
EVENT_INDEX_ENSURED = "index_ensured"
EVENT_CONSOLIDATED_TIME_COORD = "consolidated_time_coord"
INDEX_ENSURED_OUTCOME_CREATED = "created"
INDEX_ENSURED_OUTCOME_MATCHED_EXISTING = "matched_existing"
INDEX_ENSURED_OUTCOME_CONFLICT_REFUSED = "conflict_refused"
INDEX_ENSURED_OUTCOME_REBUILT = "rebuilt"
INDEX_ENSURED_OUTCOMES = frozenset(
    {
        INDEX_ENSURED_OUTCOME_CREATED,
        INDEX_ENSURED_OUTCOME_MATCHED_EXISTING,
        INDEX_ENSURED_OUTCOME_CONFLICT_REFUSED,
        INDEX_ENSURED_OUTCOME_REBUILT,
    }
)

# NOTE: legacy slot-index attrs live in core/slot_index.py alongside SlotIndexModel; resolved-index attrs live here alongside ResolvedIndexRecord. Both follow the pattern "co-locate reserved attr constants with the type that uses them".
INDEX_DIRNAME = "index"
INDEX_CURRENT_FILENAME = "current.json"
RESOLVED_INDEX_ATTR = "firecube_resolved_index"
RESOLVED_INDEX_IDENTITY_HASH_ATTR = "firecube_resolved_index_identity_hash"

MAINTENANCE_OP_DELETE = "delete"
MAINTENANCE_OP_SCRUB = "scrub"
MAINTENANCE_OP_ARCHIVE_RESTORE = "archive_restore"
MAINTENANCE_OPS = frozenset(
    {
        MAINTENANCE_OP_DELETE,
        MAINTENANCE_OP_SCRUB,
        MAINTENANCE_OP_ARCHIVE_RESTORE,
    }
)
MAINTENANCE_KIND = "maintenance"


def canonical_index_bytes(index: dict[str, Any]) -> bytes:
    """Return the canonical UTF-8 JSON encoding of a resolved-index payload."""

    return json.dumps(index, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


SourceRefKind = Literal["path", "uri", "identifier"]


@dataclass(frozen=True, slots=True, kw_only=True)
class ItemManifestEntry:
    """A single entry in a content-addressed item manifest.

    Manifest entries let the engine plan an ``IrregularTimeAxis`` axis once and
    hand deterministic per-item work to parallel workers without a second
    discovery pass. Each entry pins one source item to one axis coordinate via
    a content-address (``identity_hash``) plus a caller-defined stable
    reference (``source_ref``).

    The manifest is deterministic-planning-and-worker-reuse data only. It is
    NOT a general provenance system: no timestamps, user metadata, plugin
    versions, or processing lineage are stored here. Add those to a separate
    subsystem if they are ever needed.

    Attributes:
        identity_hash: Content-address of the item (SHA-256 hex of the item
            contents, or a caller-defined stable identity string). Must be
            unique within a manifest.
        coordinate_value: The resolved axis coordinate for this item. Type
            depends on the axis kind (ISO string, integer, etc.). Must be
            JSON-native (``str``, ``int``, ``float``, ``bool``, ``None``);
            non-native values will fail loudly at
            `canonical_index_bytes` serialisation.
        source_ref: A stable reference the caller can dereference to load the
            item at write time. Interpretation is fixed by ``source_ref_kind``.
            Must be non-empty.
        source_ref_kind: Declares how ``source_ref`` is interpreted, and the
            stability contract the caller promises:

            - ``"path"``: absolute filesystem path guaranteed stable for the
              cube's lifetime.
            - ``"uri"``: stable URL (e.g. ``s3://bucket/key``) guaranteed
              dereferenceable for the cube's lifetime.
            - ``"identifier"``: plugin-defined stable identifier resolvable by
              the plugin's own logic for the cube's lifetime.

            Callables that close over ``source_ref`` (e.g. lazy WriteIntent
            payloads) are safe if and only if the plugin honours the declared
            stability contract for the whole dispatch window.
    """

    identity_hash: str
    coordinate_value: Any
    source_ref: str
    source_ref_kind: SourceRefKind

    def to_canonical_dict(self) -> dict[str, Any]:
        """Return the deterministic JSON-native dict for canonical serialisation."""

        return {
            "identity_hash": self.identity_hash,
            "coordinate_value": self.coordinate_value,
            "source_ref": self.source_ref,
            "source_ref_kind": self.source_ref_kind,
        }


def validate_manifest_entries(entries: list[ItemManifestEntry]) -> None:
    """Validate the invariants of a content-addressed item manifest.

    Checks:

    - Every ``source_ref`` is non-empty.
    - No two entries share the same ``identity_hash``.
    - No two entries share the same ``coordinate_value`` (pairwise distinct).

    Args:
        entries: The manifest entries to validate.

    Raises:
        ValueError: On any invariant violation, with a message naming the
            duplicated value or the offending entry.
    """

    seen_hashes: set[str] = set()
    seen_coords: list[Any] = []
    for entry in entries:
        if not entry.source_ref:
            raise ValueError(
                "ItemManifestEntry.source_ref must be non-empty "
                f"(identity_hash={entry.identity_hash!r})"
            )
        if entry.identity_hash in seen_hashes:
            raise ValueError(
                f"duplicate identity_hash in manifest entries: {entry.identity_hash!r}"
            )
        seen_hashes.add(entry.identity_hash)
        # coordinate_value equality is checked linearly — the values may be
        # unhashable JSON structures (e.g. list, dict) so a set is unsafe.
        for existing in seen_coords:
            if existing == entry.coordinate_value:
                raise ValueError(
                    f"duplicate coordinate_value in manifest entries: {entry.coordinate_value!r}"
                )
        seen_coords.append(entry.coordinate_value)


def compute_resolved_index_identity_hash(
    index: dict[str, Any],
    items: Sequence[ItemManifestEntry] | Sequence[dict[str, Any]] | None = None,
) -> str:
    """Compute the SHA-256 identity hash for a resolved-index record.

    ASYMMETRIC by design (byte-parity requirement): when ``items`` is ``None``
    the hash is byte-identical to pre-manifest records
    (``sha256(canonical_index_bytes(index))``). When ``items`` is present,
    both ``index`` and the sorted ``items`` list fold into the hash. This
    preserves the ``identity_hash`` of existing ``RegularTimeAxis`` and
    ``IntegerAxis`` cubes across the schema addition and lets the manifest
    act as a freeze-detection mechanism for ``IrregularTimeAxis`` cubes:
    adding or removing an item changes the recorded hash.

    Items are sorted by ``identity_hash`` before hashing so the resulting
    value is invariant under input order.

    Args:
        index: The resolved-index payload dict (as returned by
            ``ResolvedIndex.canonical_index_payload``).
        items: Optional manifest entries. Accepts either
            ``ItemManifestEntry`` instances (canonicalised via
            ``entry.to_canonical_dict()``) or already-canonicalised dicts
            (as produced by `ResolvedIndexRecord.from_json_bytes`).

    Returns:
        Lowercase-hex SHA-256 digest (64 characters).
    """

    if items is None:
        return hashlib.sha256(canonical_index_bytes(index)).hexdigest()
    items_dicts: list[dict[str, Any]] = [
        entry.to_canonical_dict() if isinstance(entry, ItemManifestEntry) else entry
        for entry in items
    ]
    sorted_items = sorted(items_dicts, key=lambda entry: entry["identity_hash"])
    combined = {"index": index, "items": sorted_items}
    return hashlib.sha256(canonical_index_bytes(combined)).hexdigest()


@dataclass(frozen=True, slots=True)
class WriteDomain:
    """Stable conflict key for mutually exclusive physical writes."""

    product: str
    category: str
    name: str

    @property
    def identifier(self) -> str:
        """Return the canonical product:category:name identifier string."""
        return f"{self.product}:{self.category}:{self.name}"

    @property
    def claim_name(self) -> str:
        """Return the SHA-256 hashed filename for the claim file."""
        digest = hashlib.sha256(self.identifier.encode("utf-8")).hexdigest()
        return f"{digest}.json"


@dataclass(slots=True)
class ChunkEvent:
    """Authoritative immutable event stored in the product WAL."""

    event_id: str
    event_type: str
    product: str
    run_id: str
    timestamp: float
    record: dict[str, Any]
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the event to a JSON-compatible dict for WAL storage."""
        payload = {
            "schema_version": SCHEMA_VERSION,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "product": self.product,
            "run_id": self.run_id,
            "timestamp": float(self.timestamp),
            "record": dict(self.record),
        }
        if self.meta:
            payload["meta"] = dict(self.meta)
        return payload


IndexEnsuredOutcome = Literal[
    "created",
    "matched_existing",
    "conflict_refused",
    "rebuilt",
]


@dataclass(frozen=True, slots=True, kw_only=True)
class IndexEnsuredEvent:
    """WAL audit event for an ensured resolved index."""

    run_id: str
    product: str
    identity_hash: str
    axis_kinds: tuple[str, ...]
    groups: tuple[str, ...]
    outcome: IndexEnsuredOutcome
    timestamp: str

    def __post_init__(self) -> None:
        if not isinstance(self.axis_kinds, tuple):
            raise TypeError(f"axis_kinds must be a tuple, got {type(self.axis_kinds).__name__}")
        if not isinstance(self.groups, tuple):
            raise TypeError(f"groups must be a tuple, got {type(self.groups).__name__}")
        if self.outcome not in INDEX_ENSURED_OUTCOMES:
            raise ValueError(
                f"outcome must be one of {sorted(INDEX_ENSURED_OUTCOMES)!r}, got {self.outcome!r}"
            )
        object.__setattr__(self, "axis_kinds", tuple(sorted(self.axis_kinds)))
        object.__setattr__(self, "groups", tuple(sorted(self.groups)))

    def to_dict(self) -> dict[str, Any]:
        """Serialize the event to a JSON-compatible dict for WAL storage."""

        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "product": self.product,
            "identity_hash": self.identity_hash,
            "axis_kinds": list(self.axis_kinds),
            "groups": list(self.groups),
            "outcome": self.outcome,
            "timestamp": self.timestamp,
        }


@dataclass(slots=True)
class ClaimInfo:
    """Projected information for one write-domain claim."""

    product: str
    domain: str
    owner_id: str
    claim_path: str
    acquired_at: float
    last_heartbeat_at: float
    heartbeat_interval_s: int
    stale_threshold_s: int

    @property
    def stale(self) -> bool:
        """True if the claim has not received a heartbeat within the stale threshold."""
        return (time.time() - float(self.last_heartbeat_at)) > float(self.stale_threshold_s)


@dataclass(slots=True)
class ClearSweepResult:
    """Result of a bulk stale-claim clear operation."""

    previewed: list[str] = field(default_factory=list)
    cleared: list[str] = field(default_factory=list)
    skipped_fresh: list[str] = field(default_factory=list)
    skipped_missing: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AbandonSweepResult:
    """Result of a bulk stale-run abandonment operation."""

    previewed: list[str] = field(default_factory=list)
    abandoned: list[str] = field(default_factory=list)
    skipped_fresh: list[str] = field(default_factory=list)
    skipped_already_terminal: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RunInfo:
    """Projected information for one ingestion run."""

    product: str
    run_id: str
    status: str
    run_dir: str
    run_uri: str
    started_at: float
    updated_at: float
    completed_at: float | None
    events: int
    parts: int
    error: str | None = None
    stale_threshold_s: int = DEFAULT_RUN_STALE_THRESHOLD_S
    slot_range: tuple[int, int] | None = None
    """Half-open slot range [start, end) for parallel ingestion pods.

    None for single-pod (Phase 2 and pre-Phase-3) runs.
    """
    slot_group: str | None = None

    @property
    def stale(self) -> bool:
        """True if a non-terminal run has not been updated within the stale threshold."""
        if self.is_terminal:
            return False
        return (time.time() - float(self.updated_at)) > float(self.stale_threshold_s)

    @property
    def is_terminal(self) -> bool:
        """True if the run status is complete, failed, or abandoned."""
        return self.status in {"complete", "failed", "abandoned"}


@dataclass(slots=True)
class SpanCoverage:
    """Coverage information for a span record.

    Strategy-neutral: supports both xarray-append (``time_index_ranges``)
    and direct zarr-python region writes (``region_spec``).  At least one
    of ``time_index_ranges`` or ``region_spec`` should be set for
    meaningful coverage, but both default to ``None`` so construction
    never fails.
    """

    group: str
    arrays: list[str]
    time_index_ranges: list[list[int]] | None = None
    aligned: bool = True
    state_array: str | None = None
    state_deleted_value: int = 2
    time_min: str | None = None
    time_max: str | None = None
    region_spec: dict[str, Any] | None = None
    write_strategy: str | None = None
    time_dim_name: str | None = None

    @property
    def timestamps_written(self) -> int:
        """Total number of time indices covered by all ranges."""
        if not self.time_index_ranges:
            return 0
        return sum(end - start + 1 for start, end in self.time_index_ranges)


def build_span_entry(
    *,
    run_id: str,
    batch_id: str,
    group: str,
    meta: dict[str, Any],
    arrays: list[str],
    time_index_ranges: list[list[int]] | None = None,
    status: str = "active",
    reason: str | None = None,
    aligned: bool = True,
    state_array: str | None = None,
    state_deleted_value: int = 2,
    region_spec: dict[str, Any] | None = None,
    write_strategy: str | None = None,
    time_dim_name: str | None = None,
) -> dict[str, Any]:
    """Build a projected span record dict."""
    entry_meta = dict(meta)
    entry_meta["group"] = group
    entry_meta["batch_id"] = batch_id
    entry_meta["run_id"] = run_id

    effective_ranges = time_index_ranges or []
    span_payload: dict[str, Any] = {
        "arrays": arrays,
        "time_index_ranges": effective_ranges,
        "timestamps_written": sum(end - start + 1 for start, end in effective_ranges),
        "aligned": aligned,
        "state_array": state_array,
        "state_deleted_value": state_deleted_value,
    }
    if reason:
        span_payload["reason"] = reason
    if region_spec is not None:
        span_payload["region_spec"] = region_spec
    if write_strategy is not None:
        span_payload["write_strategy"] = write_strategy
    if time_dim_name is not None:
        span_payload["time_dim_name"] = time_dim_name

    return {
        "key": f"span_{run_id}_{batch_id}_{group}".strip("_"),
        "type": "span",
        "size": 0,
        "timestamp": time.time(),
        "status": status,
        "meta": entry_meta,
        "span": span_payload,
        "schema_version": SCHEMA_VERSION,
    }


@dataclass
class ChunkInfo:
    """Materialized current or historical control-plane record."""

    key: str
    product: str
    chunk_type: str
    size: int
    timestamp: float
    manifest_path: str
    status: str | None = None
    replaces: float | None = None
    replaced_at: float | None = None
    meta: dict[str, Any] | None = None
    record: dict[str, Any] | None = None

    @property
    def datetime(self) -> datetime:
        """Convert the chunk timestamp to a datetime object."""
        return datetime.fromtimestamp(self.timestamp)

    @property
    def size_mb(self) -> float:
        """Return size in megabytes."""
        return self.size / (1024 * 1024)

    @property
    def is_active(self) -> bool:
        """True if the chunk is not replaced or deleted."""
        return self.status not in {"replaced", "deleted"}

    @property
    def timestamps_written(self) -> int:
        """Extract timestamps_written from the embedded span record, or 0 if absent."""
        if not isinstance(self.record, dict):
            return 0
        span = self.record.get("span", {})
        if not isinstance(span, dict):
            return 0
        return int(span.get("timestamps_written", 0))


@dataclass
class DeletionPlan:
    """Plan for chunk deletion operations."""

    chunks: list[ChunkInfo]
    total_size: int
    products_affected: set[str]
    manifest_files: set[str]

    @property
    def count(self) -> int:
        """Number of chunks in the deletion plan."""
        return len(self.chunks)

    @property
    def size_mb(self) -> float:
        """Total size in megabytes."""
        return self.total_size / (1024 * 1024)

    @property
    def size_gb(self) -> float:
        """Total size in gigabytes."""
        return self.total_size / (1024 * 1024 * 1024)


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolvedIndexRecord:
    """On-disk record for an engine-resolved index payload.

    The engine writes it to ``.firecube/index/current.json`` after the first
    successful resolution. Subsequent runs read it back and verify that the
    declared ``IndexSpec`` produces the same ``identity_hash`` before
    writing. Inspect the record with ``firecube zarr index show`` and
    regenerate it from a plugin declaration with
    ``firecube zarr index rebuild``.

    Optional ``items`` carries a content-addressed manifest for
    ``IrregularTimeAxis`` cubes. It is omitted from the wire format and
    ``identity_hash`` for regular / integer axes so those cubes stay
    byte-identical to pre-manifest records
    (see `compute_resolved_index_identity_hash`).
    """

    schema_version: str = "v1"
    recorded_at: str
    recorded_by_run_id: str
    identity_hash: str
    index: dict[str, Any]
    items: tuple[ItemManifestEntry, ...] | None = None

    def __post_init__(self) -> None:
        if len(self.identity_hash) != 64 or not all(
            c in "0123456789abcdef" for c in self.identity_hash
        ):
            raise ValueError(
                f"identity_hash must be a 64-character lowercase hex string, "
                f"got {self.identity_hash!r} (length {len(self.identity_hash)})"
            )
        if self.items is not None and not isinstance(self.items, tuple):
            object.__setattr__(self, "items", tuple(self.items))

    def to_json_bytes(self) -> bytes:
        """Serialise to the on-disk wire format (deterministic, UTF-8 JSON).

        ASYMMETRIC: the ``items`` key is omitted when
        `items` is ``None`` so records for regular / integer axes serialise
        byte-identically to pre-manifest records.
        """

        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "recorded_at": self.recorded_at,
            "recorded_by_run_id": self.recorded_by_run_id,
            "identity_hash": self.identity_hash,
            "index": self.index,
        }
        if self.items is not None:
            payload["items"] = sorted(
                (entry.to_canonical_dict() for entry in self.items),
                key=lambda entry: str(entry["coordinate_value"]),
            )
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, data: bytes) -> ResolvedIndexRecord:
        """Parse and validate an on-disk resolved-index record."""

        try:
            parsed = json.loads(data)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError, ValueError) as exc:
            raise ManifestError(f"resolved-index record is not valid JSON: {exc}") from exc

        if not isinstance(parsed, dict):
            raise ManifestError(
                f"resolved-index record must decode to a JSON object, got {type(parsed).__name__}"
            )

        required = {
            "schema_version",
            "recorded_at",
            "recorded_by_run_id",
            "identity_hash",
            "index",
        }
        missing = required - set(parsed)
        if missing:
            raise ManifestError(f"resolved-index record missing required fields: {sorted(missing)}")

        if parsed["schema_version"] != "v1":
            raise ManifestError(
                f"resolved-index record schema_version must be 'v1', got {parsed['schema_version']!r}"
            )

        try:
            index = parsed["index"]
            if not isinstance(index, dict):
                raise TypeError(f"expected dict, got {type(index).__name__}")
            items_raw = parsed.get("items")
            items_parsed = _parse_manifest_items(items_raw) if items_raw is not None else None
            recomputed = compute_resolved_index_identity_hash(index, items_parsed)
        except (TypeError, ValueError) as exc:
            raise ManifestError(
                f"resolved-index record has invalid index structure: {exc}"
            ) from exc

        stored_hash = parsed["identity_hash"]
        if stored_hash != recomputed:
            raise ManifestError(
                "resolved-index record identity-hash mismatch: "
                f"stored={stored_hash!r} recomputed={recomputed!r} "
                "(corrupt on-disk record or tampering)"
            )

        return cls(
            schema_version=parsed["schema_version"],
            recorded_at=parsed["recorded_at"],
            recorded_by_run_id=parsed["recorded_by_run_id"],
            identity_hash=stored_hash,
            index=index,
            items=tuple(items_parsed) if items_parsed is not None else None,
        )


def _parse_manifest_items(raw: Any) -> list[ItemManifestEntry]:
    """Parse a JSON-decoded ``items`` field back into ``ItemManifestEntry`` instances."""

    if not isinstance(raw, list):
        raise TypeError(f"items must be a JSON array, got {type(raw).__name__}")
    parsed: list[ItemManifestEntry] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise TypeError(f"items[{i}] must be a JSON object, got {type(entry).__name__}")
        missing = {"identity_hash", "coordinate_value", "source_ref", "source_ref_kind"} - set(
            entry
        )
        if missing:
            raise ValueError(f"items[{i}] missing required fields: {sorted(missing)}")
        kind = entry["source_ref_kind"]
        if kind not in {"path", "uri", "identifier"}:
            raise ValueError(
                f"items[{i}].source_ref_kind must be 'path', 'uri', or 'identifier'; got {kind!r}"
            )
        parsed.append(
            ItemManifestEntry(
                identity_hash=entry["identity_hash"],
                coordinate_value=entry["coordinate_value"],
                source_ref=entry["source_ref"],
                source_ref_kind=kind,
            )
        )
    return parsed


@dataclass(frozen=True, slots=True)
class SlotIndexModelRecord:
    """On-disk record for a product's persisted slot-index model.

    Stored at ``.firecube/slot_index/current.json``. The control plane is the
    sole authority for this record; Zarr root attrs are a mirror-only surface.
    """

    model: SlotIndexModel
    identity_hash: str
    schema_version: str
    recorded_at: str
    recorded_by_run_id: str

    def __post_init__(self) -> None:
        if len(self.identity_hash) != 64 or not all(
            c in "0123456789abcdef" for c in self.identity_hash
        ):
            raise ValueError(
                f"identity_hash must be a 64-character lowercase hex string, "
                f"got {self.identity_hash!r} (length {len(self.identity_hash)})"
            )

    def to_json_bytes(self) -> bytes:
        """Serialise to the on-disk wire format (deterministic, UTF-8 JSON)."""
        payload = {
            "schema_version": self.schema_version,
            "recorded_at": self.recorded_at,
            "recorded_by_run_id": self.recorded_by_run_id,
            "identity_hash": self.identity_hash,
            "model": json.loads(self.model.canonical_bytes()),
        }
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, data: bytes) -> SlotIndexModelRecord:
        """Parse and validate an on-disk slot-index record.

        After parsing, this recomputes ``model.identity_hash`` from the embedded
        model payload and asserts it equals the stored ``identity_hash`` field.
        On mismatch it raises `ManifestError` with a message containing
        ``"identity-hash mismatch"`` and both the stored and recomputed values.

        The cross-check ensures corrupt or tampered records are rejected before
        they propagate into the precedence matrix — the matrix compares the
        stored hash against a plugin-supplied hash, so a divorced stored hash
        would poison every downstream comparison.
        """
        try:
            parsed = json.loads(data)
        except (json.JSONDecodeError, ValueError) as exc:
            raise ManifestError(f"slot-index record is not valid JSON: {exc}") from exc

        required = {
            "schema_version",
            "recorded_at",
            "recorded_by_run_id",
            "identity_hash",
            "model",
        }
        missing = required - set(parsed)
        if missing:
            raise ManifestError(f"slot-index record missing required fields: {sorted(missing)}")

        try:
            model_data = parsed["model"]
            groups = {
                k: SlotAxis(cadence_s=v["cadence_s"], mode=v["mode"])
                for k, v in model_data.get("groups", {}).items()
            }
            model = SlotIndexModel(
                name=model_data["name"],
                epoch=model_data["epoch"],
                groups=groups,
                time_unit=model_data.get("time_unit"),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise ManifestError(f"slot-index record has invalid model structure: {exc}") from exc

        stored_hash = parsed["identity_hash"]
        recomputed = model.identity_hash
        if stored_hash != recomputed:
            raise ManifestError(
                "slot-index record identity-hash mismatch: "
                f"stored={stored_hash!r} recomputed={recomputed!r} "
                "(corrupt on-disk record or tampering)"
            )

        return cls(
            model=model,
            identity_hash=stored_hash,
            schema_version=parsed["schema_version"],
            recorded_at=parsed["recorded_at"],
            recorded_by_run_id=parsed["recorded_by_run_id"],
        )
