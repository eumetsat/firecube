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

"""Inventory manifests of a product, kept under .firecube/releases/inventories/.

An inventory lists the objects of a product at one cut: every key with its size
and either the sha256 of its bytes or its S3 version. Layout below
.firecube/releases/:

  inventories/<hex>.json   one inventory; <hex> is the sha256 of the file

An inventory file is created once and never rewritten, and a read checks its
bytes against its name. An inventory leaves out `.firecube/claims/` and the
temporaries of firecube's own writers, and refuses a key it cannot record
literally. Only the ChunkManager facade calls the I/O functions here.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from typing import Any, Final

from firecube.core.controlplane._content_addressed import (
    makedirs,
    read_verified,
    require_id,
    sha256_hex,
)
from firecube.core.controlplane.types import (
    CLAIMS_DIRNAME,
    CONTROL_DIRNAME,
    INVENTORIES_DIRNAME,
    INVENTORY_KIND_DIGEST,
    Inventory,
    InventoryObject,
    canonical_json_bytes,
)
from firecube.core.errors import ControlPlaneCorruptionError, ManifestError, Message
from firecube.core.filesystem.protocol import StorageFilesystemFull
from firecube.core.filesystem.publication import is_publication_temp
from firecube.core.storage.uri import StorageUri

INVENTORY_SCHEMA_VERSION: Final = "v1"

_CONTROL_PREFIX: Final = f"{CONTROL_DIRNAME}/"
_CLAIMS_PREFIX: Final = f"{CONTROL_DIRNAME}/{CLAIMS_DIRNAME}/"
_DIGEST_PREFIX: Final = "sha256:"
_OBJECT_FIELDS: Final = ("digest", "version_id", "etag")

# The local atomic writer of the fsspec driver stages each control-plane file under
# this name (tempfile.mkstemp) beside its destination, and only below .firecube/.
_ATOMIC_WRITER_TEMP: Final = re.compile(r"\.firecube-atomic-[a-z0-9_]+\.tmp")
# obstore's local store stages a write to <name> as <name>#<n> and renames it; a killed
# writer leaves the staging file. Its own listing hides <name>#<n>, the fsspec one does not.
_LOCAL_STAGING_SUFFIX: Final = re.compile(r"#[0-9]+\Z")
# s3fs reads `<key>?versionId=<id>` as version <id> of <key>, so such a key never reads back.
_S3_VERSION_SUFFIX: Final = "?versionId="


def inventories_root(releases_root: StorageUri) -> StorageUri:
    """Return the `inventories/` directory below a product's `.firecube/releases/` directory."""
    return releases_root.join(INVENTORIES_DIRNAME)


def _inventory_uri(releases_root: StorageUri, inventory_id: str) -> StorageUri:
    return inventories_root(releases_root).join(f"{inventory_id}.json")


def _product_key(product_root: StorageUri, uri: StorageUri) -> str:
    """Return the key of a listed object below *product_root*; refuse an object outside it."""
    prefix = product_root.path.rstrip("/") + "/"
    same_store = (uri.protocol, uri.authority) == (product_root.protocol, product_root.authority)
    if not same_store or not uri.path.startswith(prefix) or uri.path == prefix:
        raise ManifestError(
            f"the listing of {product_root.to_str()} returned {uri.to_str()}, "
            "which is not an object of the product"
        )
    return uri.path[len(prefix) :]


def _is_writer_temporary(key: str) -> bool:
    """Return whether *key* names a publication temporary or an atomic-writer temporary."""
    name = key.rsplit("/", 1)[-1]
    if is_publication_temp(key):
        return True
    return key.startswith(_CONTROL_PREFIX) and _ATOMIC_WRITER_TEMP.fullmatch(name) is not None


def excluded_from_inventory(product_root: StorageUri, key: str) -> bool:
    """Return whether an inventory of *product_root* leaves *key* out, or refuse the key.

    Claims, local publication temporaries and atomic-writer temporaries below
    `.firecube/` are left out.

    Raises:
        ManifestError: On s3 the key contains `?versionId=`; on a remote product its
            last segment is a publication temporary's name; on a local product its
            last segment ends in `#<digits>`, the name of an interrupted staged write.
    """
    product_uri = product_root.to_str()
    if product_root.protocol == "s3" and _S3_VERSION_SUFFIX in key:
        raise ManifestError(
            Message(
                "The key {key!r} of {product_uri} contains {suffix!r}, so it cannot be read "
                "back literally and no inventory is taken. Rename the object.",
                key=key,
                product_uri=product_uri,
                suffix=_S3_VERSION_SUFFIX,
            )
        )
    # Both drivers leave literal publication temporaries out of a listing, and a remote
    # write never stages one, so a remote key of that shape is a key the listing rewrote.
    if product_root.protocol != "file" and is_publication_temp(key):
        raise ManifestError(
            Message(
                "The listing of {product_uri} returned {key!r}. A listing never returns a "
                "publication temporary's name, so the store holds a key with an empty or '.' "
                "segment that the listing rewrote into it. Rename that object; no inventory "
                "is taken.",
                key=key,
                product_uri=product_uri,
            )
        )
    if key.startswith(_CLAIMS_PREFIX) or _is_writer_temporary(key):
        return True
    if product_root.protocol == "file" and _LOCAL_STAGING_SUFFIX.search(key.rsplit("/", 1)[-1]):
        raise ManifestError(
            Message(
                "The file {key!r} of {product_uri} ends in '#<digits>', the name an "
                "interrupted local write leaves behind, so no inventory is taken. Remove it "
                "if it is a leftover, or rename it if it is product data.",
                key=key,
                product_uri=product_uri,
            )
        )
    return False


def _read_listed(fs: StorageFilesystemFull, product_root: StorageUri, uri: StorageUri) -> bytes:
    """Read a listed object; refuse one the listing returned but the store does not hold."""
    try:
        return fs.read_bytes(uri)
    except FileNotFoundError as exc:
        if product_root.protocol == "file":
            message = Message(
                "{uri} was listed and then vanished; the product changed while its "
                "inventory was taken.",
                uri=uri.to_str(),
                product_uri=product_root.to_str(),
            )
        else:
            message = Message(
                "{uri} was listed and then not found. The product changed while its "
                "inventory was taken, or it holds a key with an empty, '.' or '..' segment, "
                "which the listing rewrites.",
                uri=uri.to_str(),
                product_uri=product_root.to_str(),
            )
        raise ManifestError(message) from exc


def _unlisted_local_files(product_root: StorageUri, listed: set[str]) -> list[str]:
    """Return the sorted keys of the regular files below a local product that *listed* lacks.

    The walk follows no symlink and skips `.firecube/claims/` and firecube's own temporaries.
    """
    root = product_root.path.rstrip("/") or "/"
    unlisted: list[str] = []
    pending = [""]
    while pending:
        prefix = pending.pop()
        try:
            with os.scandir(os.path.join(root, prefix)) as scan:
                entries = list(scan)
        except FileNotFoundError:
            continue  # a directory removed since the listing; its files are not on disk
        for entry in entries:
            key = f"{prefix}{entry.name}"
            if entry.is_dir(follow_symlinks=False):
                if f"{key}/" != _CLAIMS_PREFIX:
                    pending.append(f"{key}/")
            elif (
                entry.is_file(follow_symlinks=False)
                and key not in listed
                and not _is_writer_temporary(key)
            ):
                unlisted.append(key)
    return sorted(unlisted)


def _refuse_unlisted_local_files(product_root: StorageUri, listed: set[str]) -> None:
    """Refuse a local product with a regular file on disk that the driver's listing missed.

    Raises:
        ManifestError: Such a file exists; the message names the first and the count.
    """
    unlisted = _unlisted_local_files(product_root, listed)
    if unlisted:
        raise ManifestError(
            Message(
                "Files of {product_uri} on disk that its storage driver does not list: "
                "{count}, the first {key!r}. No inventory is taken. The obstore driver does "
                "not list a name that ends in '#<digits>': rename such a file. A file written "
                "while the inventory is taken is not listed either.",
                product_uri=product_root.to_str(),
                count=len(unlisted),
                key=unlisted[0],
            )
        )


def build_digest_inventory(fs: StorageFilesystemFull, product_root: StorageUri) -> Inventory:
    """List every object of the product and record its size and sha256 digest.

    Each object is read whole, once, by the URI the listing returned. On a remote
    product an excluded key is read too, so a rewritten key cannot hide among them.
    On a local product one walk of the directory tree checks that the listing missed no file.

    Raises:
        ManifestError: The listing returned an object outside the product, a key twice,
            a key `excluded_from_inventory` refuses, or an object that is not there; or
            a local product holds a regular file the listing missed.
    """
    entries: list[InventoryObject] = []
    seen: set[str] = set()
    listed = [(uri, _product_key(product_root, uri)) for uri in fs.find(product_root)]
    if product_root.protocol == "file":
        # A driver's listing may hide a name (obstore: `<name>#<digits>`); the walk does not.
        _refuse_unlisted_local_files(product_root, {key for _uri, key in listed})
    for uri, key in listed:
        if key in seen:
            raise ManifestError(
                Message(
                    "The listing of {product_uri} returned the key {key!r} twice: the store "
                    "holds keys with an empty, '.' or '..' segment, which the listing rewrites. "
                    "Rename those objects; no inventory is taken.",
                    product_uri=product_root.to_str(),
                    key=key,
                )
            )
        seen.add(key)
        if excluded_from_inventory(product_root, key):
            if product_root.protocol != "file":
                _read_listed(fs, product_root, uri)
            continue
        data = _read_listed(fs, product_root, uri)
        digest = f"{_DIGEST_PREFIX}{sha256_hex(data)}"
        entries.append(InventoryObject(key=key, size=len(data), digest=digest))
    entries.sort(key=lambda entry: entry.key)
    return Inventory(kind=INVENTORY_KIND_DIGEST, objects=tuple(entries))


def inventory_payload(inventory: Inventory) -> dict[str, Any]:
    """Return the JSON object stored for an inventory; absent optional fields are left out."""
    objects: list[dict[str, Any]] = []
    for item in inventory.objects:
        entry: dict[str, Any] = {"key": item.key, "size": item.size}
        for name in _OBJECT_FIELDS:
            value = getattr(item, name)
            if value is not None:
                entry[name] = value
        if item.delete_marker:
            entry["delete_marker"] = True
        objects.append(entry)
    payload: dict[str, Any] = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "kind": inventory.kind,
        "objects": objects,
    }
    if inventory.bucket_versioning is not None:
        payload["bucket_versioning"] = inventory.bucket_versioning
    if isinstance(inventory.lifecycle, Mapping):
        payload["lifecycle"] = dict(inventory.lifecycle)
    elif inventory.lifecycle is not None:
        payload["lifecycle"] = inventory.lifecycle
    return payload


def write_inventory(fs: StorageFilesystemFull, root: StorageUri, inventory: Inventory) -> str:
    """Create the inventory file once below *root*, the releases directory, and return its id.

    The id is the sha256 hex of the canonical JSON bytes. The bytes appear whole
    under their final name or not at all. An existing file with that name is
    read back and must hold the same bytes.

    Raises:
        ControlPlaneCorruptionError: A file with that name holds other bytes; it is kept.
    """
    data = canonical_json_bytes(inventory_payload(inventory))
    inventory_id = sha256_hex(data)
    uri = _inventory_uri(root, inventory_id)
    makedirs(fs, uri.parent())
    try:
        fs.atomic_writer.write_atomic(uri, data)
    except FileExistsError:
        read_verified(fs, uri, inventory_id)
    return inventory_id


def _load_json(data: bytes, uri: StorageUri) -> dict[str, Any]:
    try:
        content = json.loads(data)
    except (ValueError, RecursionError) as exc:
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not valid JSON: {exc}") from exc
    if not isinstance(content, dict):
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not a JSON object")
    if content.get("schema_version") != INVENTORY_SCHEMA_VERSION:
        raise ControlPlaneCorruptionError(
            f"Unsupported inventory schema version in {uri.to_str()}: "
            f"{content.get('schema_version')!r}"
        )
    return content


def _object_from_payload(item: Any) -> InventoryObject:
    if not isinstance(item, dict):
        raise TypeError("an inventory entry is not a JSON object")
    return InventoryObject(
        key=item["key"],
        size=item["size"],
        digest=item.get("digest"),
        version_id=item.get("version_id"),
        etag=item.get("etag"),
        delete_marker=item.get("delete_marker", False),
    )


def _inventory_from_payload(content: Mapping[str, Any], uri: StorageUri) -> Inventory:
    try:
        objects = content["objects"]
        if not isinstance(objects, list):
            raise TypeError("objects is not a list")
        return Inventory(
            kind=content["kind"],
            objects=tuple(_object_from_payload(item) for item in objects),
            bucket_versioning=content.get("bucket_versioning"),
            lifecycle=content.get("lifecycle"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not an inventory: {exc}") from exc


def read_inventory(fs: StorageFilesystemFull, root: StorageUri, inventory_id: str) -> Inventory:
    """Read one inventory below *root*, the releases directory, and verify its bytes against its id.

    Raises:
        ManifestError: The id is not 64 lowercase hex digits, or no inventory has it.
        ControlPlaneCorruptionError: The file's bytes do not match its id, or it does not parse.
    """
    inventory_id = require_id(inventory_id, what="release inventory")
    uri = _inventory_uri(root, inventory_id)
    try:
        data = read_verified(fs, uri, inventory_id)
    except FileNotFoundError as exc:
        raise ManifestError(f"no inventory {inventory_id} under {root.to_str()}") from exc
    return _inventory_from_payload(_load_json(data, uri), uri)
