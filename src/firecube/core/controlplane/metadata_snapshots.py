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

"""Metadata snapshots of a product, kept under .firecube/metadata/.

A snapshot captures the exact bytes of every metadata document of a store,
never a chunk. Layout below .firecube/metadata/:

  blobs/sha256/<aa>/<hex>   the bytes of one document, keyed by their sha256
  manifests/<hex>.json      the path-sorted document list; <hex> hashes the file
  records/<hex>.json        one snapshot event; <hex> hashes the file
  refs/latest.json          the id of the newest record, published last

Blobs, manifests and records are created once and never rewritten. Records and
refs name other files by id, never by an absolute path. Only the ChunkManager
facade calls the I/O functions here.
"""

from __future__ import annotations

import json
import posixpath
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal

from firecube.core.controlplane._content_addressed import (
    HEX_ID,
    create,
    publish,
    read_verified,
    require_id,
    sha256_hex,
)
from firecube.core.controlplane.types import (
    CONTROL_DIRNAME,
    METADATA_DIRNAME,
    canonical_json_bytes,
)
from firecube.core.errors import ControlPlaneCorruptionError, ManifestError
from firecube.core.filesystem.protocol import StorageFilesystemFull
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.metadata_documents import (
    ABSENT,
    MetadataDocument,
    MetadataDocumentKind,
    NodeView,
    enumerate_metadata_documents,
    parse_node_document,
)

SnapshotConsistency = Literal["observation", "coherent"]
"""How a snapshot was taken: ``observation`` on a live store, ``coherent`` under the exclusive claim."""

SNAPSHOT_SCHEMA_VERSION: Final = "v1"
LATEST: Final = "latest"
CONSISTENCY_OBSERVATION: Final = "observation"
CONSISTENCY_COHERENT: Final = "coherent"

BLOBS_DIRNAME: Final = "blobs"
BLOB_ALGORITHM: Final = "sha256"
MANIFESTS_DIRNAME: Final = "manifests"
RECORDS_DIRNAME: Final = "records"
REFS_DIRNAME: Final = "refs"
LATEST_REF_FILENAME: Final = "latest.json"

_DIGEST_PREFIX: Final = f"{BLOB_ALGORITHM}:"
_CONSISTENCIES: Final = frozenset({CONSISTENCY_OBSERVATION, CONSISTENCY_COHERENT})
_CONTROL_KINDS: Final = frozenset(
    {
        MetadataDocumentKind.FIRECUBE_SCHEMA,
        MetadataDocumentKind.FIRECUBE_INDEX,
        MetadataDocumentKind.FIRECUBE_SLOT_INDEX,
    }
)
_NODE_DOCUMENT_KINDS: Final = frozenset(
    {
        MetadataDocumentKind.V3_GROUP,
        MetadataDocumentKind.V3_ARRAY,
        MetadataDocumentKind.V3_UNPARSED,
        MetadataDocumentKind.V2_GROUP,
        MetadataDocumentKind.V2_ARRAY,
        MetadataDocumentKind.V2_ATTRS,
    }
)
_V3_NODE: Final = "zarr.json"
_V2_ARRAY: Final = ".zarray"
_V2_GROUP: Final = ".zgroup"
_V2_ATTRS: Final = ".zattrs"
_INCLUDED_KINDS: Final = tuple(sorted(kind.value for kind in MetadataDocumentKind))


@dataclass(frozen=True)
class ManifestEntry:
    """One captured document in a manifest.

    Attributes:
        path: Store-relative path, ``/``-separated, without a leading slash.
        digest: ``sha256:<hex>`` of the document bytes; ``<hex>`` names its blob.
        size: Length of the document in bytes.
        kind: What the document is.
    """

    path: str
    digest: str
    size: int
    kind: MetadataDocumentKind


@dataclass(frozen=True)
class SnapshotRecord:
    """One snapshot event, as stored in ``records/<snapshot_id>.json``.

    Attributes:
        snapshot_id: The sha256 hex of the record file.
        parent: The id that ``refs/latest.json`` named when the snapshot was taken, or ``None``.
        manifest: The sha256 hex of the manifest file.
        run_id: The claim holder of a coherent snapshot; ``None`` for an observation.
        trigger: Why the snapshot was taken, as the caller named it.
        taken_at: Unix time in seconds when capture started.
        consistency: ``observation`` or ``coherent``.
        included_kinds: The document kinds the capture reads.
    """

    snapshot_id: str
    parent: str | None
    manifest: str
    run_id: str | None
    trigger: str
    taken_at: float
    consistency: SnapshotConsistency
    included_kinds: tuple[str, ...]


@dataclass(frozen=True)
class Snapshot:
    """A snapshot record with its manifest entries.

    Attributes:
        record: The snapshot record.
        documents: The captured documents, sorted by path.
    """

    record: SnapshotRecord
    documents: tuple[ManifestEntry, ...]


@dataclass(frozen=True)
class NodeDiff:
    """The parsed differences of one Zarr node between two snapshots.

    A pair holds the before and the after value. ``ABSENT`` marks a key one side lacks.

    Attributes:
        path: Node path, ``""`` for the root.
        attributes_added: Attributes only the after side holds.
        attributes_removed: Attributes only the before side holds.
        attributes_changed: Attributes both sides hold with different JSON values.
        dimension_names: The changed dimension names, or ``None`` when equal.
        fill_value: The changed fill value, or ``None`` when equal.
        physical: The changed storage keys.
        extensions: The changed keys outside the core keys of the format.
    """

    path: str
    attributes_added: Mapping[str, Any]
    attributes_removed: Mapping[str, Any]
    attributes_changed: Mapping[str, tuple[Any, Any]]
    dimension_names: tuple[Any, Any] | None
    fill_value: tuple[Any, Any] | None
    physical: Mapping[str, tuple[Any, Any]]
    extensions: Mapping[str, tuple[Any, Any]]


@dataclass(frozen=True)
class SnapshotDiff:
    """The differences between two snapshots.

    Attributes:
        before: The id of the earlier snapshot.
        after: The id of the later snapshot.
        added: Paths only the after snapshot holds.
        removed: Paths only the before snapshot holds.
        changed: Paths both hold with different bytes.
        nodes: The parsed differences of nodes present on both sides, sorted by path.
        unparsed: Changed document paths whose node does not parse; they keep the path-level diff.
    """

    before: str
    after: str
    added: tuple[str, ...]
    removed: tuple[str, ...]
    changed: tuple[str, ...]
    nodes: tuple[NodeDiff, ...]
    unparsed: tuple[str, ...]


def metadata_root(product_root: StorageUri) -> StorageUri:
    """Return the ``.firecube/metadata/`` directory of a product root."""
    return product_root.join(CONTROL_DIRNAME, METADATA_DIRNAME)


def _blob_uri(root: StorageUri, blob_id: str) -> StorageUri:
    return root.join(BLOBS_DIRNAME, BLOB_ALGORITHM, blob_id[:2], blob_id)


def _manifest_uri(root: StorageUri, manifest_id: str) -> StorageUri:
    return root.join(MANIFESTS_DIRNAME, f"{manifest_id}.json")


def _record_uri(root: StorageUri, snapshot_id: str) -> StorageUri:
    return root.join(RECORDS_DIRNAME, f"{snapshot_id}.json")


def _latest_uri(root: StorageUri) -> StorageUri:
    return root.join(REFS_DIRNAME, LATEST_REF_FILENAME)


def _load_json(data: bytes, uri: StorageUri) -> dict[str, Any]:
    try:
        content = json.loads(data)
    except (ValueError, RecursionError) as exc:
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not valid JSON: {exc}") from exc
    if not isinstance(content, dict):
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not a JSON object")
    if content.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise ControlPlaneCorruptionError(
            f"Unsupported metadata snapshot schema version in {uri.to_str()}: "
            f"{content.get('schema_version')!r}"
        )
    return content


def write_blob(fs: StorageFilesystemFull, root: StorageUri, data: bytes) -> str:
    """Store *data* as a blob unless it exists, and return its sha256 hex."""
    blob_id = sha256_hex(data)
    create(fs, _blob_uri(root, blob_id), data)
    return blob_id


def read_blob(fs: StorageFilesystemFull, root: StorageUri, blob_id: str) -> bytes:
    """Return the bytes of a blob, refusing a missing blob or bytes that do not match its id."""
    blob_id = require_id(blob_id, what="blob")
    uri = _blob_uri(root, blob_id)
    try:
        return read_verified(fs, uri, blob_id)
    except FileNotFoundError as exc:
        raise ControlPlaneCorruptionError(f"metadata blob {uri.to_str()} is missing") from exc


def captured_documents(documents: Iterable[MetadataDocument]) -> list[MetadataDocument]:
    """Keep node documents and the control documents; drop any other ``.firecube/`` path."""
    prefix = f"{CONTROL_DIRNAME}/"
    return [
        document
        for document in documents
        if not document.path.startswith(prefix) or document.kind in _CONTROL_KINDS
    ]


def build_manifest(documents: Iterable[MetadataDocument]) -> tuple[ManifestEntry, ...]:
    """Return the manifest entries of *documents*, sorted by path."""
    entries = (
        ManifestEntry(path=doc.path, digest=doc.digest, size=len(doc.raw), kind=doc.kind)
        for doc in documents
    )
    return tuple(sorted(entries, key=lambda entry: entry.path))


def manifest_bytes(entries: Sequence[ManifestEntry]) -> bytes:
    """Return the canonical JSON bytes of a manifest; their sha256 hex is the manifest id."""
    return canonical_json_bytes(
        {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "documents": [
                {
                    "path": entry.path,
                    "digest": entry.digest,
                    "size": entry.size,
                    "kind": entry.kind.value,
                }
                for entry in entries
            ],
        }
    )


def _check_take(*, run_id: str | None, trigger: str, consistency: str) -> None:
    if consistency not in _CONSISTENCIES:
        raise ValueError(f"consistency must be 'observation' or 'coherent', not {consistency!r}")
    if not isinstance(trigger, str) or not trigger:
        raise ValueError("trigger must be a non-empty string")
    if consistency == CONSISTENCY_OBSERVATION and run_id is not None:
        raise ValueError("an observation snapshot records no run id; pass run_id=None")
    if consistency == CONSISTENCY_COHERENT and (not isinstance(run_id, str) or not run_id):
        raise ValueError("a coherent snapshot needs the run id that holds the exclusive claim")


def _read_latest_id(fs: StorageFilesystemFull, root: StorageUri) -> str | None:
    uri = _latest_uri(root)
    try:
        data = fs.read_bytes(uri)
    except FileNotFoundError:
        return None
    snapshot_id = _load_json(data, uri).get("snapshot")
    if not isinstance(snapshot_id, str) or not HEX_ID.fullmatch(snapshot_id):
        raise ControlPlaneCorruptionError(f"{uri.to_str()} names no snapshot id")
    return snapshot_id


def take_snapshot(
    fs: StorageFilesystemFull,
    product_root: StorageUri,
    *,
    run_id: str | None,
    trigger: str,
    consistency: SnapshotConsistency,
    verify_holder: Callable[[], None],
) -> SnapshotRecord:
    """Capture the store's metadata documents and publish a new record as ``refs/latest.json``.

    A coherent snapshot calls *verify_holder* before and after the capture, so a refusal
    writes nothing. Blobs and a manifest that already exist are not rewritten.

    Raises:
        ValueError: The arguments do not fit *consistency*.
        ControlPlaneCorruptionError: ``refs/latest.json`` does not parse; nothing is written.
    """
    _check_take(run_id=run_id, trigger=trigger, consistency=consistency)
    coherent = consistency == CONSISTENCY_COHERENT
    if coherent:
        verify_holder()
    taken_at = time.time()
    documents = captured_documents(enumerate_metadata_documents(fs, product_root))
    if coherent:
        verify_holder()

    root = metadata_root(product_root)
    parent = _read_latest_id(fs, root)
    manifest_raw = manifest_bytes(build_manifest(documents))
    manifest_id = sha256_hex(manifest_raw)
    manifest_uri = _manifest_uri(root, manifest_id)
    # A manifest is written after all its blobs and nothing deletes a blob.
    if not fs.exists(manifest_uri):
        for raw in {document.digest: document.raw for document in documents}.values():
            write_blob(fs, root, raw)
        create(fs, manifest_uri, manifest_raw)

    payload: dict[str, Any] = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "parent": parent,
        "manifest": manifest_id,
        "run_id": run_id,
        "trigger": trigger,
        "taken_at": taken_at,
        "consistency": consistency,
        "scope": {"included_kinds": list(_INCLUDED_KINDS)},
    }
    record_raw = canonical_json_bytes(payload)
    snapshot_id = sha256_hex(record_raw)
    create(fs, _record_uri(root, snapshot_id), record_raw)
    latest = {"schema_version": SNAPSHOT_SCHEMA_VERSION, "snapshot": snapshot_id}
    publish(fs, _latest_uri(root), canonical_json_bytes(latest))
    return _record_from_payload(snapshot_id, payload, _record_uri(root, snapshot_id))


def _record_from_payload(
    snapshot_id: str, payload: Mapping[str, Any], uri: StorageUri
) -> SnapshotRecord:
    try:
        parent = payload["parent"]
        run_id = payload["run_id"]
        consistency = payload["consistency"]
        if consistency not in _CONSISTENCIES:
            raise ValueError(f"consistency {consistency!r}")
        return SnapshotRecord(
            snapshot_id=snapshot_id,
            parent=None if parent is None else require_id(parent, what="snapshot"),
            manifest=require_id(payload["manifest"], what="manifest"),
            run_id=None if run_id is None else str(run_id),
            trigger=str(payload["trigger"]),
            taken_at=float(payload["taken_at"]),
            consistency=consistency,
            included_kinds=tuple(str(kind) for kind in payload["scope"]["included_kinds"]),
        )
    except (KeyError, TypeError, ValueError, ManifestError) as exc:
        raise ControlPlaneCorruptionError(
            f"{uri.to_str()} is not a snapshot record: {exc}"
        ) from exc


def resolve_snapshot_id(fs: StorageFilesystemFull, root: StorageUri, snapshot_id: str) -> str:
    """Return the record id that *snapshot_id* names; ``latest`` reads ``refs/latest.json``.

    Raises:
        ManifestError: The id is malformed, or ``latest`` is asked of a product with no snapshot.
    """
    if snapshot_id != LATEST:
        return require_id(snapshot_id, what="snapshot")
    latest_id = _read_latest_id(fs, root)
    if latest_id is None:
        raise ManifestError(f"no metadata snapshot exists under {root.to_str()}")
    return latest_id


def read_record(fs: StorageFilesystemFull, root: StorageUri, snapshot_id: str) -> SnapshotRecord:
    """Read and verify one snapshot record.

    Raises:
        ManifestError: No record has that id.
        ControlPlaneCorruptionError: The record does not match its id or does not parse.
    """
    snapshot_id = resolve_snapshot_id(fs, root, snapshot_id)
    uri = _record_uri(root, snapshot_id)
    try:
        data = read_verified(fs, uri, snapshot_id)
    except FileNotFoundError as exc:
        raise ManifestError(f"no metadata snapshot {snapshot_id} under {root.to_str()}") from exc
    return _record_from_payload(snapshot_id, _load_json(data, uri), uri)


def _read_manifest(
    fs: StorageFilesystemFull, root: StorageUri, manifest_id: str
) -> tuple[ManifestEntry, ...]:
    uri = _manifest_uri(root, manifest_id)
    try:
        content = _load_json(read_verified(fs, uri, manifest_id), uri)
    except FileNotFoundError as exc:
        raise ControlPlaneCorruptionError(f"metadata manifest {uri.to_str()} is missing") from exc
    try:
        return tuple(
            ManifestEntry(
                path=str(item["path"]),
                digest=str(item["digest"]),
                size=int(item["size"]),
                kind=MetadataDocumentKind(item["kind"]),
            )
            for item in content["documents"]
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not a manifest: {exc}") from exc


def read_snapshot(fs: StorageFilesystemFull, root: StorageUri, snapshot_id: str) -> Snapshot:
    """Read one snapshot record and its manifest entries."""
    record = read_record(fs, root, snapshot_id)
    return Snapshot(record=record, documents=_read_manifest(fs, root, record.manifest))


def list_records(fs: StorageFilesystemFull, root: StorageUri) -> list[SnapshotRecord]:
    """Read every snapshot record, sorted by ``taken_at`` and then by id."""
    records_dir = root.join(RECORDS_DIRNAME)
    if not fs.exists(records_dir):
        return []
    records: list[SnapshotRecord] = []
    for uri in fs.find(records_dir):
        name = uri.path.rstrip("/").rpartition("/")[2]
        stem, _, suffix = name.rpartition(".")
        # Anything else is an interrupted local write's temporary.
        if suffix == "json" and HEX_ID.fullmatch(stem):
            records.append(read_record(fs, root, stem))
    return sorted(records, key=lambda record: (record.taken_at, record.snapshot_id))


def _blob_id(entry: ManifestEntry) -> str:
    if not entry.digest.startswith(_DIGEST_PREFIX):
        raise ControlPlaneCorruptionError(
            f"manifest entry {entry.path} has digest {entry.digest!r}"
        )
    return entry.digest[len(_DIGEST_PREFIX) :]


def _json_equal(before: Any, after: Any) -> bool:
    if before is ABSENT or after is ABSENT:
        return before is after
    return canonical_json_bytes({"value": before}) == canonical_json_bytes({"value": after})


def _changed_keys(
    before: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, tuple[Any, Any]]:
    changed: dict[str, tuple[Any, Any]] = {}
    for key in sorted({*before, *after}):
        old, new = before.get(key, ABSENT), after.get(key, ABSENT)
        if not _json_equal(old, new):
            changed[key] = (old, new)
    return changed


def _attribute_map(view: NodeView) -> Mapping[str, Any]:
    if view.attributes is ABSENT:
        return {}
    if not isinstance(view.attributes, dict):
        raise ValueError(f"the attributes of node {view.path!r} are not a JSON object")
    return view.attributes


def _facet(before: Any, after: Any) -> tuple[Any, Any] | None:
    return None if _json_equal(before, after) else (before, after)


def _node_diff(before: NodeView, after: NodeView) -> NodeDiff | None:
    old_attrs, new_attrs = _attribute_map(before), _attribute_map(after)
    changed = _changed_keys(old_attrs, new_attrs)
    diff = NodeDiff(
        path=after.path,
        attributes_added={k: new for k, (old, new) in changed.items() if old is ABSENT},
        attributes_removed={k: old for k, (old, new) in changed.items() if new is ABSENT},
        attributes_changed={
            k: (old, new)
            for k, (old, new) in changed.items()
            if old is not ABSENT and new is not ABSENT
        },
        dimension_names=_facet(before.dimension_names, after.dimension_names),
        fill_value=_facet(before.fill_value, after.fill_value),
        physical=_changed_keys(before.physical, after.physical),
        extensions=_changed_keys(before.extensions, after.extensions),
    )
    facets = (diff.dimension_names, diff.fill_value)
    if not changed and facets == (None, None) and not (diff.physical or diff.extensions):
        return None
    return diff


def _nodes(entries: Iterable[ManifestEntry]) -> dict[str, dict[str, ManifestEntry]]:
    nodes: dict[str, dict[str, ManifestEntry]] = {}
    for entry in entries:
        if entry.kind in _NODE_DOCUMENT_KINDS:
            node = nodes.setdefault(posixpath.dirname(entry.path), {})
            node[posixpath.basename(entry.path)] = entry
    return nodes


def _entry_digest(entry: ManifestEntry | None) -> str | None:
    return None if entry is None else entry.digest


def _view(node: Mapping[str, ManifestEntry], read: Callable[[str], bytes]) -> NodeView:
    """Parse a node from its captured documents; raise ``ValueError`` when it does not parse."""

    def document(entry: ManifestEntry) -> MetadataDocument:
        raw = read(_blob_id(entry))
        return MetadataDocument(path=entry.path, kind=entry.kind, raw=raw, digest=entry.digest)

    primary = node.get(_V3_NODE) or node.get(_V2_ARRAY) or node.get(_V2_GROUP)
    if primary is None:
        raise ValueError("the node has no node document")
    v2 = primary.kind in {MetadataDocumentKind.V2_ARRAY, MetadataDocumentKind.V2_GROUP}
    attrs = node.get(_V2_ATTRS) if v2 else None
    return parse_node_document(document(primary), attrs=None if attrs is None else document(attrs))


def diff_snapshots(before: Snapshot, after: Snapshot, read: Callable[[str], bytes]) -> SnapshotDiff:
    """Diff two snapshots by path, then parse each node whose documents changed.

    *read* returns a blob's bytes by id. A node that does not parse on either side
    keeps only its path-level diff, listed under ``unparsed``.
    """
    old = {entry.path: entry for entry in before.documents}
    new = {entry.path: entry for entry in after.documents}
    changed = sorted(
        path for path in old.keys() & new.keys() if old[path].digest != new[path].digest
    )

    old_nodes, new_nodes = _nodes(before.documents), _nodes(after.documents)
    nodes: list[NodeDiff] = []
    unparsed: list[str] = []
    for node_path in sorted(old_nodes.keys() & new_nodes.keys()):
        old_node, new_node = old_nodes[node_path], new_nodes[node_path]
        touched = sorted(
            posixpath.join(node_path, name)
            for name in old_node.keys() | new_node.keys()
            if _entry_digest(old_node.get(name)) != _entry_digest(new_node.get(name))
        )
        if not touched:
            continue
        try:
            diff = _node_diff(_view(old_node, read), _view(new_node, read))
        except (ValueError, RecursionError):
            unparsed.extend(touched)
            continue
        if diff is not None:
            nodes.append(diff)

    return SnapshotDiff(
        before=before.record.snapshot_id,
        after=after.record.snapshot_id,
        added=tuple(sorted(new.keys() - old.keys())),
        removed=tuple(sorted(old.keys() - new.keys())),
        changed=tuple(changed),
        nodes=tuple(nodes),
        unparsed=tuple(unparsed),
    )
