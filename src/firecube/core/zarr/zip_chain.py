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

"""Restore a chain of zip archives, a full base and the deltas above it, onto a product.

`resolve_chain` follows the marker of each archive down to the full one and checks
every link before anything is written. `apply_chain` restores the base, applies each
delta in order, and reads every restored object back against its archive entry.
Archive files are local and read directly; the product is written through its session.

Each restored entry must hold the bytes `resolve_chain` hashed. A restore refuses a link
whose file changed since then, and a local target with a symlink on a restored key.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import IO, TYPE_CHECKING, Any, BinaryIO, Final

import zarr

from firecube.core.controlplane.types import CLAIMS_DIRNAME, CONTROL_DIRNAME
from firecube.core.errors import FirecubeError, Message
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import local_path_from_target, require_target_is_product
from firecube.core.zarr.zip_archive import (
    MARKER_ENTRY,
    _dimension_names,
    _prepare_target,
    _refuse_invalid_metadata,
    _refuse_unsafe_entries,
    _walk_live,
    read_zip_marker,
)

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession

MODE_FULL: Final = "full"
MODE_DELTA: Final = "delta"
MAX_CHAIN_LINKS: Final = 10_000
"""The most archives a chain may hold; a longer one is refused as broken."""

_MARKER_KEYS: Final = frozenset(
    {
        "version",
        "mode",
        "base",
        "base_uri",
        "base_digest",
        "inventory",
        "metadata_manifest",
        "deleted",
    }
)
_HEX_ID: Final = re.compile(r"[0-9a-f]{64}")
_DIGEST: Final = re.compile(r"sha256:[0-9a-f]{64}")
_CLAIMS_PREFIX: Final = f"{CONTROL_DIRNAME}/{CLAIMS_DIRNAME}/"
_CONTROL_PREFIX: Final = f"{CONTROL_DIRNAME}/"
_READ_BLOCK: Final = 1 << 20


class ArchiveChainError(FirecubeError):
    """A chain of zip archives cannot be restored, or its restore does not match the archives.

    Attributes:
        archive: The archive the restore was asked for, the newest link.
        broken_at: The local path of the archive where the chain breaks.
        reason: What is wrong with that link.
    """

    def __init__(self, message: object, *, archive: str, broken_at: str, reason: Message) -> None:
        self.archive = archive
        self.broken_at = broken_at
        self.reason = reason
        super().__init__(message)


@dataclass(frozen=True)
class ChainLink:
    """One checked archive of a chain.

    Attributes:
        path: The local path of the archive file.
        digest: The `sha256:<hex>` digest of the whole file when it was checked.
        version: The release version the marker names.
        mode: `"full"` or `"delta"`.
        base: The version of the base archive, or `None` for a full archive.
        entries: The `sha256:<hex>` digest of each product key the archive holds.
            The marker, directory entries and claim files are not product keys.
        deleted: The product keys a delta removes from its base, in marker order.
        marker: The parsed marker.
    """

    path: Path
    digest: str
    version: str
    mode: str
    base: str | None
    entries: Mapping[str, str]
    deleted: tuple[str, ...]
    marker: Mapping[str, Any]


def _broken(archive: str, broken_at: Path | str, reason: Message) -> ArchiveChainError:
    return ArchiveChainError(
        Message(
            "Cannot restore the archive chain of {archive}: it breaks at {broken_at}: {reason}",
            archive=archive,
            broken_at=str(broken_at),
            reason=reason,
        ),
        archive=archive,
        broken_at=str(broken_at),
        reason=reason,
    )


def _stream_digest(stream: IO[bytes]) -> str:
    digest = hashlib.sha256()
    while block := stream.read(_READ_BLOCK):
        digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def _file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return _stream_digest(stream)


def deleted_key_problem(key: object) -> str | None:
    """Return why `key` cannot be a deleted key of a delta marker, or `None` when it can.

    A chain restore refuses a marker that lists such a key, so a release never writes one.
    """
    if not isinstance(key, str):
        return "is not a string"
    if not key:
        return "is empty"
    if key.startswith("/"):
        return "is absolute"
    if "\\" in key:
        return "holds a backslash"
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in key):
        return "holds a control character"
    if any(segment in ("", ".", "..") for segment in key.split("/")):
        return "has an empty, '.' or '..' segment"
    if key.startswith(_CLAIMS_PREFIX):
        return "names a claim file"
    if key == MARKER_ENTRY:
        return "names the archive marker"
    return None


def marker_problem(marker: Mapping[str, Any]) -> str | None:
    """Return why `marker` is not a valid archive marker, or `None` when it is.

    A chain restore and `validate_zip` both apply this check.
    """
    if set(marker) != _MARKER_KEYS:
        missing = sorted(_MARKER_KEYS.difference(marker))
        extra = sorted(set(marker).difference(_MARKER_KEYS))
        return f"its keys differ from the eight a marker holds (missing {missing}, extra {extra})"
    version, mode = marker["version"], marker["mode"]
    if not isinstance(version, str) or not version:
        return "its version is not a non-empty string"
    if mode not in (MODE_FULL, MODE_DELTA):
        return f"its mode is {mode!r}, not 'full' or 'delta'"
    for name in ("inventory", "metadata_manifest"):
        value = marker[name]
        if not isinstance(value, str) or not _HEX_ID.fullmatch(value):
            return f"its {name} is not a 64-digit lowercase hex id"
    deleted = marker["deleted"]
    if not isinstance(deleted, list):
        return "its deleted entry is not a list"
    if mode == MODE_FULL:
        if any(marker[name] is not None for name in ("base", "base_uri", "base_digest")):
            return "it is full but names a base"
        if deleted:
            return "it is full but lists deleted keys"
        return None
    base, base_uri, base_digest = marker["base"], marker["base_uri"], marker["base_digest"]
    if not isinstance(base, str) or not base:
        return "it is a delta whose base is not a non-empty string"
    if not isinstance(base_uri, str) or not base_uri:
        return "it is a delta whose base_uri is not a non-empty string"
    if not isinstance(base_digest, str) or not _DIGEST.fullmatch(base_digest):
        return "it is a delta whose base_digest is not a sha256:<hex> digest"
    for key in deleted:
        if (problem := deleted_key_problem(key)) is not None:
            return f"its deleted key {key!r} {problem}"
    if len(set(deleted)) != len(deleted):
        return "it lists a deleted key twice"
    return None


def _checked_marker(path: Path, archive: str) -> Mapping[str, Any]:
    """Return the marker of the archive at `path` after checking its shape.

    Raises:
        ArchiveChainError: The marker is absent, unreadable, not a JSON object, or malformed.
    """
    try:
        marker = read_zip_marker(str(path))
    except (ValueError, zipfile.BadZipFile, OSError) as exc:
        raise _broken(archive, path, Message("{error}", error=str(exc))) from exc
    if marker is None:
        raise _broken(
            archive, path, Message("the archive has no {entry} marker", entry=MARKER_ENTRY)
        )
    if (problem := marker_problem(marker)) is not None:
        raise _broken(
            archive,
            path,
            Message(
                "its {entry} marker is invalid: {problem}", entry=MARKER_ENTRY, problem=problem
            ),
        )
    return marker


def admit_marker(path: str) -> Mapping[str, Any] | None:
    """Return the checked marker of a zip archive, or `None` when the archive holds no marker entry.

    A present marker must have the eight keys, their types and a mode of `"full"` or `"delta"`.
    A missing file, or one that is not a zip archive, returns `None`; the zip restore refuses it.

    Args:
        path: Local path or `file://` URI of the archive.

    Raises:
        ArchiveChainError: The marker entry is unreadable, not a JSON object, or malformed.
    """
    archive_path = local_path_from_target(path)
    try:
        with zipfile.ZipFile(archive_path) as zf:
            if MARKER_ENTRY not in zf.namelist():
                return None
    except (zipfile.BadZipFile, OSError):
        return None
    return _checked_marker(archive_path, str(path))


def _read_link(path: Path, digest: str, archive: str) -> ChainLink:
    """Check one archive file and return it as a link.

    Every entry is read whole, so a CRC mismatch refuses the archive as `testzip` would.

    Raises:
        ArchiveChainError: The file is not a zip archive, an entry is unsafe or corrupt,
            a `zarr.json` entry is not valid Zarr v3 metadata, a full archive has no root
            `zarr.json`, or the marker is absent or malformed.
    """
    try:
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
            _refuse_unsafe_entries(names, str(path))
            keys = sorted(
                name
                for name in names
                if not name.endswith("/")
                and not name.startswith(_CLAIMS_PREFIX)
                and name != MARKER_ENTRY
            )
            product_keys = set(keys)
            entries: dict[str, str] = {}
            # Every member, claims and directories included, is read whole for its CRC.
            for info in zf.infolist():
                with zf.open(info) as entry:
                    entry_digest = _stream_digest(entry)
                if info.filename in product_keys:
                    entries[info.filename] = entry_digest
            entries = {key: entries[key] for key in keys}
            _refuse_invalid_metadata(zf, keys, str(path))
    except (ValueError, zipfile.BadZipFile, OSError) as exc:
        raise _broken(archive, path, Message("{error}", error=str(exc))) from exc
    marker = _checked_marker(path, archive)
    if marker["mode"] == MODE_FULL and "zarr.json" not in entries:
        raise _broken(archive, path, Message("the full archive has no root zarr.json"))
    return ChainLink(
        path=path,
        digest=digest,
        version=marker["version"],
        mode=marker["mode"],
        base=marker["base"],
        entries=MappingProxyType(entries),
        deleted=tuple(marker["deleted"]),
        marker=MappingProxyType(dict(marker)),
    )


def _base_path(link: ChainLink, archive: str) -> Path:
    """Return the literal local path the delta `link` names as its base archive.

    Raises:
        ArchiveChainError: `base_uri` is not a `file://` URI.
    """
    base_uri = link.marker["base_uri"]
    try:
        parsed = StorageUri.parse(base_uri)
    except ValueError as exc:
        reason = Message(
            "its base_uri {base_uri!r} is not a URI: {error}", base_uri=base_uri, error=str(exc)
        )
        raise _broken(archive, link.path, reason) from exc
    if parsed.protocol != "file":
        reason = Message("its base_uri {base_uri!r} is not a local file:// URI", base_uri=base_uri)
        raise _broken(archive, link.path, reason)
    return Path(parsed.path)


def _refuse_inconsistent_deletes(links: Sequence[ChainLink], archive: str) -> None:
    """Replay the key sets base first; refuse a delta that deletes a key its base state lacks.

    A delta that both holds and deletes a key is refused as well.
    """
    present = set(links[0].entries)
    for link in links[1:]:
        for key in link.deleted:
            if key in link.entries:
                reason = Message("it both holds and deletes {key!r}", key=key)
                raise _broken(archive, link.path, reason)
            if key not in present:
                reason = Message(
                    "it deletes {key!r}, which the restored state of base {base!r} does not hold",
                    key=key,
                    base=link.base,
                )
                raise _broken(archive, link.path, reason)
        present.difference_update(link.deleted)
        present.update(link.entries)


def resolve_chain(path: str) -> list[ChainLink]:
    """Return the checked chain of archives below `path`, the full base first.

    The call follows each delta's `base_uri` down to a full archive and writes nothing.
    Each base must exist, have the marker's `base_digest` and name the marker's `base`
    version. Each archive must pass the zip restore's entry, CRC and metadata checks.
    Each `deleted` key must be a relative product key that its base state holds.

    Args:
        path: Local path or `file://` URI of the newest archive.

    Returns:
        The links, the full archive first and the archive at `path` last.

    Raises:
        ArchiveChainError: A link is missing, changed, malformed or repeated, a deleted
            key is unsafe or absent from its base state, or the chain holds more than
            `MAX_CHAIN_LINKS` archives.
    """
    archive = str(path)
    current = local_path_from_target(archive)
    expected_digest: str | None = None
    expected_version: str | None = None
    seen_paths: set[str] = set()
    seen_digests: set[str] = set()
    seen_versions: set[str] = set()
    newest_first: list[ChainLink] = []
    while True:
        if len(newest_first) >= MAX_CHAIN_LINKS:
            reason = Message("the chain holds more than {limit} archives", limit=MAX_CHAIN_LINKS)
            raise _broken(archive, current, reason)
        real = os.path.realpath(current)
        if real in seen_paths:
            reason = Message("the archive file appears twice in the chain, a cycle")
            raise _broken(archive, current, reason)
        if not current.is_file():
            raise _broken(archive, current, Message("the archive file does not exist"))
        try:
            digest = _file_digest(current)
        except OSError as exc:
            raise _broken(archive, current, Message("{error}", error=str(exc))) from exc
        if expected_digest is not None and digest != expected_digest:
            reason = Message(
                "the file has digest {digest}, not the {expected} its delta records",
                digest=digest,
                expected=expected_digest,
            )
            raise _broken(archive, current, reason)
        if digest in seen_digests:
            raise _broken(archive, current, Message("the archive repeats a later link, a cycle"))
        link = _read_link(current, digest, archive)
        if expected_version is not None and link.version != expected_version:
            reason = Message(
                "it is release {version!r}, not the base {expected!r} its delta names",
                version=link.version,
                expected=expected_version,
            )
            raise _broken(archive, current, reason)
        if link.version in seen_versions:
            reason = Message("release {version!r} appears twice, a cycle", version=link.version)
            raise _broken(archive, current, reason)
        seen_paths.add(real)
        seen_digests.add(digest)
        seen_versions.add(link.version)
        newest_first.append(link)
        if link.mode == MODE_FULL:
            break
        expected_digest = link.marker["base_digest"]
        expected_version = link.base
        current = _base_path(link, archive)
    links = newest_first[::-1]
    _refuse_inconsistent_deletes(links, archive)
    return links


def _summary(
    session: StorageSession, links: Sequence[ChainLink], target: str, keys: Sequence[str]
) -> dict[str, Any]:
    """Return the restore summary of the restored product, in the zip restore's shape."""
    product_uri = session.product.product_uri
    members = (
        _walk_live(session.zarr.create_store(product_uri, mode="r").zarr_kwargs())
        if "zarr.json" in keys
        else []
    )
    groups = [name for name, node in members if isinstance(node, zarr.Group)]
    arrays = {name: node for name, node in members if isinstance(node, zarr.Array)}
    coordinates: set[str] = set()
    for name, node in arrays.items():
        group, _, var = name.rpartition("/")
        if tuple(_dimension_names(node)) == (var,):
            coordinates.add(name)
        for coord in str(node.attrs.get("coordinates", "")).split():
            coordinates.add(f"{group}/{coord}" if group else coord)
    return {
        "source": str(links[-1].path),
        "target": str(local_path_from_target(target)),
        "groups": groups,
        "variables": sorted(n for n in arrays if n not in coordinates),
        "coordinates": sorted(n for n in arrays if n in coordinates),
        "keys_restored": len(keys),
        "controlplane_restored": any(k.startswith(_CONTROL_PREFIX) for k in keys),
        "chain": [link.version for link in links],
    }


def _verify_restored(
    session: StorageSession,
    expected: Mapping[str, tuple[ChainLink, str]],
    removed: Mapping[str, ChainLink],
    archive: str,
) -> None:
    """Read every restored object back against the digest of the entry that wrote it.

    Raises:
        ArchiveChainError: An object is missing or holds other bytes, or a deleted key
            is still present.
    """
    product_uri = session.product.product_uri
    for key, (link, digest) in expected.items():
        try:
            with session.open(product_uri.join(key), "rb") as stream:
                restored = _stream_digest(stream)
        except FileNotFoundError:
            restored = None
        if restored != digest:
            reason = Message(
                "the restored object {key!r} does not hold the archive entry's bytes", key=key
            )
            raise _broken(archive, link.path, reason)
    root = _local_root(session)
    for key, link in removed.items():
        if _holds_object(session, root, key):
            reason = Message("the deleted key {key!r} is still present after the restore", key=key)
            raise _broken(archive, link.path, reason)


def _local_root(session: StorageSession) -> Path | None:
    """Return the local directory of the session's product, or `None` for a remote product."""
    product_uri = session.product.product_uri
    # An object store such as s3 has no symlinks, so only a local product is checked.
    return Path(product_uri.path) if product_uri.protocol == "file" else None


def _holds_object(session: StorageSession, root: Path | None, key: str) -> bool:
    """Return whether an object, not a directory, is at `key` of the session's product.

    A later key may turn the path of a deleted key into a directory, as a rank change does.
    """
    if root is None:
        return session.exists(session.product.product_uri.join(key))
    try:
        return not stat.S_ISDIR((root / key).lstat().st_mode)
    except (FileNotFoundError, NotADirectoryError):
        return False


def _symlink_reason(node: Path, key: str) -> Message:
    return Message(
        "the target holds a symlink at {path} on the path of {key!r}; "
        "the restore would follow it outside the product",
        path=str(node),
        key=key,
    )


def _refuse_symlink(root: Path | None, key: str, link: ChainLink, archive: str) -> None:
    """Refuse `key` when a component of its local path, the key itself included, is a symlink.

    The restore calls it immediately before each put and delete. Each component is read
    with `lstat`, so a symlinked product root still has its descendants checked.
    """
    if root is None:
        return
    node = root
    for part in key.split("/"):
        node = node / part
        try:
            mode = node.lstat().st_mode
        except (FileNotFoundError, NotADirectoryError):
            return  # nothing exists below a missing component or a file
        if stat.S_ISLNK(mode):
            raise _broken(archive, link.path, _symlink_reason(node, key))


def _remove_empty_parents(root: Path | None, key: str) -> None:
    """Remove the local directories a deleted `key` leaves empty, nearest first.

    A directory goes only while it is empty and not a symlink, and the product root
    never goes. The walk stops at the first directory that stays. A remote product passes.
    """
    if root is None:
        return
    parts = key.split("/")[:-1]
    while parts:
        node = root.joinpath(*parts)
        try:
            if not stat.S_ISDIR(node.lstat().st_mode):
                return
            node.rmdir()
        except FileNotFoundError:
            pass  # the driver removed it already
        except OSError:
            return  # it still holds something
        parts.pop()


def refuse_symlinked_target(links: Sequence[ChainLink], session: StorageSession) -> None:
    """Refuse a local target with a symlink on the path of any key the chain puts or deletes.

    The check writes nothing and reads each path component once. A remote product passes.

    Raises:
        ArchiveChainError: A component of such a key, the key itself included, is a symlink.
    """
    root = _local_root(session)
    if root is None or not links:
        return
    archive = str(links[-1].path)
    components: dict[str, tuple[ChainLink, str]] = {}
    for link in links:
        for key in (*link.entries, *link.deleted):
            parts = key.split("/")
            for end in range(1, len(parts) + 1):
                components.setdefault("/".join(parts[:end]), (link, key))
    for component in sorted(components):
        node = root / component
        try:
            mode = node.lstat().st_mode
        except (FileNotFoundError, NotADirectoryError):
            continue
        if stat.S_ISLNK(mode):
            link, key = components[component]
            raise _broken(archive, link.path, _symlink_reason(node, key))


def _open_unchanged(link: ChainLink, archive: str) -> BinaryIO:
    """Open the archive of `link` and return the handle once its bytes hash to `link.digest`.

    The caller reads the entries from this handle, so a file replaced after the check is
    never read. A rewrite in place of the open file is outside this check.

    Raises:
        ArchiveChainError: The file cannot be read or has changed since `resolve_chain` hashed it.
    """
    try:
        handle = link.path.open("rb")
    except OSError as exc:
        raise _broken(archive, link.path, Message("{error}", error=str(exc))) from exc
    try:
        digest = _stream_digest(handle)
    except OSError as exc:
        handle.close()
        raise _broken(archive, link.path, Message("{error}", error=str(exc))) from exc
    if digest != link.digest:
        handle.close()
        reason = Message(
            "the file has digest {digest}, not the {expected} it had when the chain was checked",
            digest=digest,
            expected=link.digest,
        )
        raise _broken(archive, link.path, reason)
    handle.seek(0)
    return handle


def _put_entries(
    session: StorageSession,
    zf: zipfile.ZipFile,
    link: ChainLink,
    root: Path | None,
    archive: str,
) -> None:
    """Put each product key of `link` from `zf`, refusing an entry whose bytes differ from its digest.

    The keys are the ones `resolve_chain` recorded, so an entry it did not see is never put.
    """
    product_uri = session.product.product_uri
    for key in sorted(link.entries):
        try:
            data = zf.read(key)
        except (KeyError, zipfile.BadZipFile, OSError) as exc:
            reason = Message("its entry {key!r} cannot be read: {error}", key=key, error=str(exc))
            raise _broken(archive, link.path, reason) from exc
        if f"sha256:{hashlib.sha256(data).hexdigest()}" != link.entries[key]:
            reason = Message("its entry {key!r} changed after the chain was checked", key=key)
            raise _broken(archive, link.path, reason)
        _refuse_symlink(root, key, link, archive)
        session.put_bytes(product_uri.join(key), data)


def apply_chain(
    links: Sequence[ChainLink],
    target: str,
    *,
    session: StorageSession,
    overwrite: bool = False,
    keep_run_id: str | None = None,
) -> dict[str, Any]:
    """Restore a resolved chain into the session's product, the full base first.

    Before any write, every archive must still have the digest `resolve_chain` recorded, and
    no symlink may lie on the local path of a restored key. The base then prepares the
    target as the zip restore does and puts its entries. Each delta first deletes its
    `deleted` keys, one literal object each, with the local directories that leaves empty.
    It then puts its entries. Every entry put must hash to its recorded digest. Last,
    every restored object is read back against its archive entry, and no object may
    remain at a deleted key.

    Args:
        links: The chain `resolve_chain` returned, the full archive first.
        target: Local path or `file://` URI of the product; it must name the session's product.
        session: Storage session bound to the target product.
        overwrite: Replace an existing Zarr store at `target`.
        keep_run_id: Run whose records an overwrite keeps; pass the restore's own run.

    Returns:
        The zip restore's summary of the restored product, plus `chain`, the versions
        applied in order.

    Raises:
        ValueError: `links` is empty or does not start with a full archive.
        ArchiveChainError: An archive or entry changed since `resolve_chain`, a symlink lies
            on a restored key's path, the target lacks a key a delta deletes, or a restored
            object does not match its archive entry.
        FileExistsError: `target` holds a Zarr store and `overwrite` is False.
        ConfigurationError: An overwrite finds a symlink at the target or on a kept path.
        Exception: The driver's error for a failed put or delete; the restore stops at the first.
    """
    if not links or links[0].mode != MODE_FULL:
        raise ValueError("A chain starts with a full archive")
    require_target_is_product(target, session.product.product_uri)
    archive = str(links[-1].path)
    root = _local_root(session)
    base = links[0]
    expected: dict[str, tuple[ChainLink, str]] = {
        key: (base, digest) for key, digest in base.entries.items()
    }
    removed: dict[str, ChainLink] = {}
    for link in links[1:]:
        _open_unchanged(link, archive).close()
    refuse_symlinked_target(links, session)
    with _open_unchanged(base, archive) as handle, zipfile.ZipFile(handle) as zf:
        _prepare_target(
            local_path_from_target(target), overwrite=overwrite, keep_run_id=keep_run_id
        )
        _put_entries(session, zf, base, root, archive)

    product_uri = session.product.product_uri
    for link in links[1:]:
        # Deletes come first: a replacement key may sit where a deleted key, or its
        # directory, was. `resolve_chain` refused a delta that both holds and deletes a key.
        for key in link.deleted:
            uri = product_uri.join(key)
            if not session.exists(uri):
                reason = Message(
                    "the target does not hold {key!r}, which the delta deletes", key=key
                )
                raise _broken(archive, link.path, reason)
            _refuse_symlink(root, key, link, archive)
            session.delete_literal(uri)
            _remove_empty_parents(root, key)
            expected.pop(key, None)
            removed[key] = link
        with _open_unchanged(link, archive) as handle, zipfile.ZipFile(handle) as zf:
            _put_entries(session, zf, link, root, archive)
        for key in link.entries:
            expected[key] = (link, link.entries[key])
            removed.pop(key, None)

    _verify_restored(session, expected, removed, archive)
    return _summary(session, links, target, sorted(expected))
