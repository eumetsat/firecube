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

"""Zarr metadata documents of a store, captured as bytes and read as JSON.

The module lists, hashes, parses and patches the metadata documents of Zarr v2
and v3 stores. It parses the JSON itself and never round-trips a document
through zarr-python's models, which drop or add keys. Nothing here writes to a
store.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Any, Final

from firecube.core.controlplane.types import (
    CONTROL_DIRNAME,
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    SCHEMA_FILENAME,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
)

if TYPE_CHECKING:
    from firecube.core.filesystem.protocol import StorageFilesystem
    from firecube.core.storage.uri import StorageUri

__all__ = [
    "ABSENT",
    "MetadataDocument",
    "MetadataDocumentKind",
    "NodeView",
    "enumerate_metadata_documents",
    "metadata_document_digest",
    "must_understand",
    "parse_node_document",
    "patch_attributes",
]


class _Absent(Enum):
    ABSENT = "ABSENT"

    def __repr__(self) -> str:
        return "ABSENT"


ABSENT: Final = _Absent.ABSENT
"""Marks a key the document does not hold, apart from a null value and from any default."""


class MetadataDocumentKind(StrEnum):
    """The kind of a metadata document, set by its name and, for ``zarr.json``, its content."""

    V3_GROUP = "v3_group"
    V3_ARRAY = "v3_array"
    V3_UNPARSED = "v3_unparsed"
    V2_GROUP = "v2_group"
    V2_ARRAY = "v2_array"
    V2_ATTRS = "v2_attrs"
    V2_CONSOLIDATED = "v2_consolidated"
    FIRECUBE_SCHEMA = "firecube_schema"
    FIRECUBE_INDEX = "firecube_index"
    FIRECUBE_SLOT_INDEX = "firecube_slot_index"


@dataclass(frozen=True)
class MetadataDocument:
    """One metadata document, exactly as the store holds it.

    Attributes:
        path: Store-relative path, ``/``-separated, without a leading slash.
        kind: What the document is.
        raw: The bytes as read. Nothing re-serialises them.
        digest: ``sha256:<hex>`` of ``raw``.
    """

    path: str
    kind: MetadataDocumentKind
    raw: bytes
    digest: str


@dataclass(frozen=True)
class NodeView:
    """The parsed content of one Zarr node.

    Every field that reads a key holds ``ABSENT`` when the document lacks it.
    Nothing fills a default.

    Attributes:
        path: Node path, ``""`` for the root.
        zarr_format: ``3`` or ``2``, from the document kind.
        node_type: ``"group"`` or ``"array"``.
        document: The parsed node JSON: ``zarr.json``, ``.zgroup`` or ``.zarray``.
        attributes: The v3 ``attributes`` value, or the parsed v2 ``.zattrs``.
        dimension_names: The v3 ``dimension_names``, or the v2 ``_ARRAY_DIMENSIONS`` attribute.
        fill_value: The array's ``fill_value``. Always ``ABSENT`` for a group.
        physical: The present storage keys of the format, in document order. Empty for a group.
        extensions: The present top-level keys outside the core keys, in document order.
    """

    path: str
    zarr_format: int
    node_type: str
    document: Mapping[str, Any]
    attributes: Any
    dimension_names: Any
    fill_value: Any
    physical: Mapping[str, Any]
    extensions: Mapping[str, Any]


_V3_NODE = "zarr.json"
_V2_GROUP = ".zgroup"
_V2_ARRAY = ".zarray"
_V2_ATTRS = ".zattrs"
_V2_CONSOLIDATED = ".zmetadata"

_CONTROL_DOCUMENTS: Final[tuple[tuple[str, MetadataDocumentKind], ...]] = (
    (posixpath.join(CONTROL_DIRNAME, SCHEMA_FILENAME), MetadataDocumentKind.FIRECUBE_SCHEMA),
    (
        posixpath.join(CONTROL_DIRNAME, INDEX_DIRNAME, INDEX_CURRENT_FILENAME),
        MetadataDocumentKind.FIRECUBE_INDEX,
    ),
    (
        posixpath.join(CONTROL_DIRNAME, SLOT_INDEX_DIRNAME, SLOT_INDEX_CURRENT_FILENAME),
        MetadataDocumentKind.FIRECUBE_SLOT_INDEX,
    ),
)

_V3_GROUP_CORE: Final = frozenset({"zarr_format", "node_type", "attributes"})
_V3_ARRAY_PHYSICAL: Final = (
    "shape",
    "data_type",
    "chunk_grid",
    "chunk_key_encoding",
    "codecs",
    "storage_transformers",
)
_V3_ARRAY_CORE: Final = _V3_GROUP_CORE | {*_V3_ARRAY_PHYSICAL, "fill_value", "dimension_names"}
_V2_GROUP_CORE: Final = frozenset({"zarr_format"})
_V2_ARRAY_PHYSICAL: Final = (
    "shape",
    "chunks",
    "dtype",
    "order",
    "compressor",
    "filters",
    "dimension_separator",
)
_V2_ARRAY_CORE: Final = _V2_GROUP_CORE | {*_V2_ARRAY_PHYSICAL, "fill_value"}
_V2_DIMENSIONS_ATTR = "_ARRAY_DIMENSIONS"

_NODE_KINDS: Final = frozenset(
    {
        MetadataDocumentKind.V3_GROUP,
        MetadataDocumentKind.V3_ARRAY,
        MetadataDocumentKind.V2_GROUP,
        MetadataDocumentKind.V2_ARRAY,
    }
)


def metadata_document_digest(raw: bytes) -> str:
    """Return ``sha256:<hex>`` of *raw*."""
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _document(path: str, kind: MetadataDocumentKind, raw: bytes) -> MetadataDocument:
    return MetadataDocument(path=path, kind=kind, raw=raw, digest=metadata_document_digest(raw))


def _read_optional(fs: StorageFilesystem, uri: StorageUri) -> bytes | None:
    """Read *uri* in one GET, or return ``None`` when the object is absent."""
    try:
        return fs.read_bytes(uri)
    except FileNotFoundError:
        return None


def _v3_kind(raw: bytes) -> MetadataDocumentKind:
    """Classify a ``zarr.json`` by its ``node_type``; capture anything else as unparsed."""
    try:
        content = json.loads(raw)
    except (ValueError, RecursionError):
        return MetadataDocumentKind.V3_UNPARSED
    if isinstance(content, dict):
        if content.get("node_type") == "group":
            return MetadataDocumentKind.V3_GROUP
        if content.get("node_type") == "array":
            return MetadataDocumentKind.V3_ARRAY
    return MetadataDocumentKind.V3_UNPARSED


def _read_image(fs: StorageFilesystem, node_uri: StorageUri, node: str) -> list[MetadataDocument]:
    """Read a group's ``.zmetadata`` as bytes; its content never decides membership."""
    image_raw = _read_optional(fs, node_uri.join(_V2_CONSOLIDATED))
    if image_raw is None:
        return []
    path = posixpath.join(node, _V2_CONSOLIDATED)
    return [_document(path, MetadataDocumentKind.V2_CONSOLIDATED, image_raw)]


def _read_node(
    fs: StorageFilesystem, node_uri: StorageUri, node: str
) -> tuple[list[MetadataDocument], bool]:
    """Read the documents of the node at *node* by name.

    Returns:
        The documents found, and whether the node is a group whose children may be listed.
    """
    raw = _read_optional(fs, node_uri.join(_V3_NODE))
    if raw is not None:
        kind = _v3_kind(raw)
        is_group = kind == MetadataDocumentKind.V3_GROUP
        found = [_document(posixpath.join(node, _V3_NODE), kind, raw)]
        if is_group:
            found.extend(_read_image(fs, node_uri, node))
        return found, is_group

    group_raw = _read_optional(fs, node_uri.join(_V2_GROUP))
    array_raw = _read_optional(fs, node_uri.join(_V2_ARRAY))
    if group_raw is None and array_raw is None:
        return [], False

    found: list[MetadataDocument] = []
    if group_raw is not None:
        found.append(
            _document(posixpath.join(node, _V2_GROUP), MetadataDocumentKind.V2_GROUP, group_raw)
        )
    if array_raw is not None:
        found.append(
            _document(posixpath.join(node, _V2_ARRAY), MetadataDocumentKind.V2_ARRAY, array_raw)
        )
    attrs_raw = _read_optional(fs, node_uri.join(_V2_ATTRS))
    if attrs_raw is not None:
        found.append(
            _document(posixpath.join(node, _V2_ATTRS), MetadataDocumentKind.V2_ATTRS, attrs_raw)
        )

    # A directory holding both .zgroup and .zarray counts as an array: its listing
    # would return chunk keys.
    is_group = array_raw is None
    if is_group:
        found.extend(_read_image(fs, node_uri, node))
    return found, is_group


def _child_directories(fs: StorageFilesystem, group_uri: StorageUri) -> list[str]:
    """List the names of the directories one level below a group."""
    # ``ls`` is on both drivers but not on the protocol. ``refresh=True`` bypasses the
    # s3fs listing cache, which never expires.
    entries = fs.ls(group_uri, detail=True, refresh=True)  # pyright: ignore[reportAttributeAccessIssue]
    names: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("type") not in {"directory", "dir"}:
            continue
        leaf = str(entry.get("name") or "").rstrip("/").rpartition("/")[2]
        if leaf:
            names.append(leaf)
    return names


def enumerate_metadata_documents(
    fs: StorageFilesystem, store_root: StorageUri
) -> list[MetadataDocument]:
    """Read every metadata document of the store at *store_root*, sorted by path.

    The walk reads node documents by name and lists group directories only, one
    level at a time. It never lists an array directory and never takes membership
    from a consolidated image. Under ``.firecube/`` it reads ``schema.json``,
    ``index/current.json`` and ``slot_index/current.json`` when present. An
    absent object is skipped; every other read or listing error propagates.

    Args:
        fs: The driver bound to the store.
        store_root: The store root.

    Returns:
        The documents, sorted by ``MetadataDocument.path``.
    """
    documents: list[MetadataDocument] = []
    pending = [""]
    while pending:
        node = pending.pop()
        node_uri = store_root.join(node)
        found, is_group = _read_node(fs, node_uri, node)
        documents.extend(found)
        if not is_group:
            continue
        for leaf in _child_directories(fs, node_uri):
            if not node and leaf == CONTROL_DIRNAME:
                continue
            pending.append(posixpath.join(node, leaf))

    for path, kind in _CONTROL_DOCUMENTS:
        raw = _read_optional(fs, store_root.join(path))
        if raw is not None:
            documents.append(_document(path, kind, raw))

    return sorted(documents, key=lambda document: document.path)


def _node_path(document: MetadataDocument) -> str:
    return posixpath.dirname(document.path)


def _present(mapping: Mapping[str, Any], keys: Collection[str]) -> dict[str, Any]:
    return {key: value for key, value in mapping.items() if key in keys}


def _outside(mapping: Mapping[str, Any], keys: Collection[str]) -> dict[str, Any]:
    return {key: value for key, value in mapping.items() if key not in keys}


def _parse_v3(node: MetadataDocument) -> NodeView:
    content = json.loads(node.raw)
    expected = "group" if node.kind == MetadataDocumentKind.V3_GROUP else "array"
    if not isinstance(content, dict) or content.get("node_type") != expected:
        raise ValueError(f"{node.path} is not a v3 {expected} document")
    is_array = expected == "array"
    return NodeView(
        path=_node_path(node),
        zarr_format=3,
        node_type=expected,
        document=content,
        attributes=content.get("attributes", ABSENT),
        dimension_names=content.get("dimension_names", ABSENT) if is_array else ABSENT,
        fill_value=content.get("fill_value", ABSENT) if is_array else ABSENT,
        physical=_present(content, _V3_ARRAY_PHYSICAL) if is_array else {},
        extensions=_outside(content, _V3_ARRAY_CORE if is_array else _V3_GROUP_CORE),
    )


def _parse_v2(node: MetadataDocument, attrs: MetadataDocument | None) -> NodeView:
    content = json.loads(node.raw)
    if not isinstance(content, dict):
        raise ValueError(f"{node.path} is not a JSON object")
    is_array = node.kind == MetadataDocumentKind.V2_ARRAY
    attributes: Any = ABSENT if attrs is None else json.loads(attrs.raw)
    dimension_names: Any = ABSENT
    if is_array and isinstance(attributes, dict):
        dimension_names = attributes.get(_V2_DIMENSIONS_ATTR, ABSENT)
    return NodeView(
        path=_node_path(node),
        zarr_format=2,
        node_type="array" if is_array else "group",
        document=content,
        attributes=attributes,
        dimension_names=dimension_names,
        fill_value=content.get("fill_value", ABSENT) if is_array else ABSENT,
        physical=_present(content, _V2_ARRAY_PHYSICAL) if is_array else {},
        extensions=_outside(content, _V2_ARRAY_CORE if is_array else _V2_GROUP_CORE),
    )


def _check_attrs(node: MetadataDocument, attrs: MetadataDocument | None) -> None:
    if attrs is None:
        return
    if node.kind not in {MetadataDocumentKind.V2_GROUP, MetadataDocumentKind.V2_ARRAY}:
        raise ValueError(f"{node.path} is a {node.kind} document and takes no .zattrs")
    if attrs.kind != MetadataDocumentKind.V2_ATTRS:
        raise ValueError(f"{attrs.path} is a {attrs.kind} document, not .zattrs")
    if _node_path(attrs) != _node_path(node):
        raise ValueError(f"{attrs.path} does not belong to the node of {node.path}")


def parse_node_document(
    node: MetadataDocument, *, attrs: MetadataDocument | None = None
) -> NodeView:
    """Parse a v3 or v2 node document into a view.

    Args:
        node: A ``v3_group``, ``v3_array``, ``v2_group`` or ``v2_array`` document.
        attrs: The ``.zattrs`` of a v2 node, when the store holds one.

    Returns:
        The node's view. An extension never makes it fail.

    Raises:
        ValueError: The kind is not a node kind, *attrs* does not fit *node*, or the JSON
            does not match the kind.
    """
    if node.kind not in _NODE_KINDS:
        raise ValueError(f"{node.path} is a {node.kind} document, not a node document")
    _check_attrs(node, attrs)
    if node.kind in {MetadataDocumentKind.V3_GROUP, MetadataDocumentKind.V3_ARRAY}:
        return _parse_v3(node)
    return _parse_v2(node, attrs)


def must_understand(view: NodeView) -> tuple[str, ...]:
    """List the v3 extension keys whose value is not an object with ``"must_understand": false``.

    A v2 view lists none.
    """
    if view.zarr_format != 3:
        return ()
    return tuple(
        key
        for key, value in view.extensions.items()
        if not (isinstance(value, dict) and value.get("must_understand") is False)
    )


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    content: dict[str, Any] = {}
    for key, value in pairs:
        if key in content:
            raise ValueError(f"duplicate JSON key {key!r}; a rewrite would drop one value")
        content[key] = value
    return content


def _load_object(document: MetadataDocument) -> dict[str, Any]:
    """Parse *document* as a JSON object, refusing a duplicate key that a rewrite would lose."""
    content = json.loads(document.raw, object_pairs_hook=_unique_keys)
    if not isinstance(content, dict):
        raise ValueError(f"{document.path} is not a JSON object")
    return content


def _patched(
    current: Mapping[str, Any], updates: Mapping[str, Any], remove: Collection[str]
) -> dict[str, Any]:
    result = {key: value for key, value in current.items() if key not in remove}
    result.update(updates)
    return result


def _dump(content: Mapping[str, Any]) -> bytes:
    return json.dumps(content, indent=2, ensure_ascii=False, allow_nan=True).encode("utf-8")


def patch_attributes(
    node: MetadataDocument,
    *,
    attrs: MetadataDocument | None = None,
    set: Mapping[str, Any],
    remove: Collection[str] = (),
) -> tuple[str, bytes]:
    """Return the path and new bytes of the one document an attribute change rewrites.

    A v3 node rewrites its ``zarr.json``. A v2 node rewrites its ``.zattrs``, or
    creates one when *attrs* is ``None``. Pass the node's ``.zattrs`` when the
    store holds one. Every other key keeps its position and value. A key that
    *set* adds goes last.

    Args:
        node: A ``v3_group``, ``v3_array``, ``v2_group`` or ``v2_array`` document.
        attrs: The ``.zattrs`` of a v2 node, when the store holds one.
        set: Attributes to add or replace.
        remove: Attribute names to drop. An absent name is ignored.

    Returns:
        The store-relative path and the bytes to write there.

    Raises:
        ValueError: The node holds an extension it must understand, a name is in both
            *set* and *remove*, the attributes are not a JSON object, or a document repeats
            a key.
    """
    view = parse_node_document(node, attrs=attrs)
    pending = must_understand(view)
    if pending:
        raise ValueError(
            f"{node.path} holds extensions that must be understood: {', '.join(pending)}"
        )
    conflicting = sorted(frozenset(set) & frozenset(remove))
    if conflicting:
        raise ValueError(f"attributes both set and removed: {', '.join(conflicting)}")

    if view.zarr_format == 3:
        content = _load_object(node)
        current = content.get("attributes", {})
        if not isinstance(current, dict):
            raise ValueError(f"{node.path} has attributes that are not a JSON object")
        content["attributes"] = _patched(current, set, remove)
        return node.path, _dump(content)

    if attrs is None:
        return posixpath.join(view.path, _V2_ATTRS), _dump(_patched({}, set, remove))
    return attrs.path, _dump(_patched(_load_object(attrs), set, remove))
