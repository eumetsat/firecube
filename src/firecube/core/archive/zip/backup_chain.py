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

"""Restore a backup chain, a full backup and the incremental backups above it, onto a product.

`resolve_backup_chain` follows the marker of each backup down to the full backup and
checks every backup before anything is written. `restore_backup_chain` restores the full
backup, applies each incremental backup in order, and reads every restored object back
against its archive entry. Backup files are local ZIP archives and read directly; the
product is written through its session.

Each restored entry must hold the bytes `resolve_backup_chain` hashed. A restore refuses a
backup whose file changed since then, and a local target with a symlink on a restored key.
"""

from __future__ import annotations

import hashlib
import os
import stat
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import IO, TYPE_CHECKING, Any, BinaryIO, Final

import zarr

from firecube.core.archive.zip.archive import (
    _dimension_names,
    _prepare_target,
    _refuse_invalid_metadata,
    _refuse_unsafe_entries,
    _walk_live,
)
from firecube.core.archive.zip.marker import (
    MARKER_ENTRY,
    MODE_FULL,
    marker_problem,
    read_zip_marker,
)
from firecube.core.controlplane.types import CLAIMS_DIRNAME, CONTROL_DIRNAME
from firecube.core.errors import FirecubeError, Message
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import local_path_from_target, require_target_is_product

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession

MAX_CHAIN_BACKUPS: Final = 10_000
"""The most backups a backup chain may hold; a longer one is refused as broken."""

_CLAIMS_PREFIX: Final = f"{CONTROL_DIRNAME}/{CLAIMS_DIRNAME}/"
_CONTROL_PREFIX: Final = f"{CONTROL_DIRNAME}/"
_READ_BLOCK: Final = 1 << 20


class BackupChainError(FirecubeError):
    """A backup chain cannot be restored, or its restore does not match the backups.

    Attributes:
        archive: The archive the restore was asked for, the newest backup.
        broken_at: The local path of the backup where the backup chain breaks.
        reason: What is wrong with that backup.
    """

    def __init__(self, message: object, *, archive: str, broken_at: str, reason: Message) -> None:
        self.archive = archive
        self.broken_at = broken_at
        self.reason = reason
        super().__init__(message)


@dataclass(frozen=True)
class ChainBackup:
    """One checked backup of a backup chain.

    Attributes:
        path: The local path of the backup file.
        digest: The `sha256:<hex>` digest of the whole file when it was checked.
        version: The release version the marker names.
        mode: `"full"` or `"incremental"`.
        previous_release: The release of the previous backup, or `None` for a full backup.
        entries: The `sha256:<hex>` digest of each product key the backup holds.
            The marker, directory entries and claim files are not product keys.
        deleted_keys: The product keys an incremental backup removes, in marker order.
        marker: The parsed marker.
    """

    path: Path
    digest: str
    version: str
    mode: str
    previous_release: str | None
    entries: Mapping[str, str]
    deleted_keys: tuple[str, ...]
    marker: Mapping[str, Any]


def _broken(archive: str, broken_at: Path | str, reason: Message) -> BackupChainError:
    return BackupChainError(
        Message(
            "Cannot restore the backup chain of {archive}: it breaks at {broken_at}: {reason}",
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


def _checked_marker(path: Path, archive: str) -> Mapping[str, Any]:
    """Return the marker of the archive at `path` after checking its shape.

    Raises:
        BackupChainError: The marker is absent, unreadable, not a JSON object, or malformed.
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

    A present marker must have the eight keys, their types and a mode of `"full"` or
    `"incremental"`. A missing file, or one that is not a zip archive, returns `None`; the zip
    restore refuses it.

    Args:
        path: Local path or `file://` URI of the archive.

    Raises:
        BackupChainError: The marker entry is unreadable, not a JSON object, or malformed.
    """
    archive_path = local_path_from_target(path)
    try:
        with zipfile.ZipFile(archive_path) as zf:
            if MARKER_ENTRY not in zf.namelist():
                return None
    except (zipfile.BadZipFile, OSError):
        return None
    return _checked_marker(archive_path, str(path))


def _read_backup(path: Path, digest: str, archive: str) -> ChainBackup:
    """Check one backup file and return it as a backup of the chain.

    Every entry is read whole, so a CRC mismatch refuses the backup as `testzip` would.

    Raises:
        BackupChainError: The file is not a zip archive, an entry is unsafe or corrupt,
            a `zarr.json` entry is not valid Zarr v3 metadata, a full backup has no root
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
        raise _broken(archive, path, Message("the full backup has no root zarr.json"))
    return ChainBackup(
        path=path,
        digest=digest,
        version=marker["version"],
        mode=marker["mode"],
        previous_release=marker["previous_release"],
        entries=MappingProxyType(entries),
        deleted_keys=tuple(marker["deleted_keys"]),
        marker=MappingProxyType(dict(marker)),
    )


def _previous_backup_path(backup: ChainBackup, archive: str) -> Path:
    """Return the literal local path the incremental `backup` names as its previous backup.

    Raises:
        BackupChainError: `previous_backup_uri` is not a `file://` URI.
    """
    previous_backup_uri = backup.marker["previous_backup_uri"]
    try:
        parsed = StorageUri.parse(previous_backup_uri)
    except ValueError as exc:
        reason = Message(
            "its previous_backup_uri {previous_backup_uri!r} is not a URI: {error}",
            previous_backup_uri=previous_backup_uri,
            error=str(exc),
        )
        raise _broken(archive, backup.path, reason) from exc
    if parsed.protocol != "file":
        reason = Message(
            "its previous_backup_uri {previous_backup_uri!r} is not a local file:// URI",
            previous_backup_uri=previous_backup_uri,
        )
        raise _broken(archive, backup.path, reason)
    return Path(parsed.path)


def _refuse_inconsistent_deletes(backups: Sequence[ChainBackup], archive: str) -> None:
    """Replay the key sets, full backup first; refuse a deleted key the state before lacks.

    An incremental backup that both holds and deletes a key is refused as well.
    """
    present = set(backups[0].entries)
    for backup in backups[1:]:
        for key in backup.deleted_keys:
            if key in backup.entries:
                reason = Message("it both holds and deletes {key!r}", key=key)
                raise _broken(archive, backup.path, reason)
            if key not in present:
                reason = Message(
                    "it deletes {key!r}, which the restored state of previous release "
                    "{previous_release!r} does not hold",
                    key=key,
                    previous_release=backup.previous_release,
                )
                raise _broken(archive, backup.path, reason)
        present.difference_update(backup.deleted_keys)
        present.update(backup.entries)


def resolve_backup_chain(path: str) -> list[ChainBackup]:
    """Return the checked backup chain that ends at `path`, the full backup first.

    The call follows each incremental backup's `previous_backup_uri` down to a full backup
    and writes nothing. Each previous backup must exist, have the marker's
    `previous_backup_digest` and name the marker's `previous_release`. Each backup must
    pass the zip restore's entry, CRC and metadata checks. Each of the `deleted_keys` must
    be a relative product key that the state before it holds.

    Args:
        path: Local path or `file://` URI of the newest backup.

    Returns:
        The backups, the full backup first and the backup at `path` last.

    Raises:
        BackupChainError: A backup is missing, changed, malformed or repeated, a deleted
            key is unsafe or absent from the state before it, or the backup chain holds
            more than `MAX_CHAIN_BACKUPS` backups.
    """
    archive = str(path)
    current = local_path_from_target(archive)
    expected_digest: str | None = None
    expected_version: str | None = None
    seen_paths: set[str] = set()
    seen_digests: set[str] = set()
    seen_versions: set[str] = set()
    newest_first: list[ChainBackup] = []
    while True:
        if len(newest_first) >= MAX_CHAIN_BACKUPS:
            reason = Message(
                "the backup chain holds more than {limit} archives", limit=MAX_CHAIN_BACKUPS
            )
            raise _broken(archive, current, reason)
        real = os.path.realpath(current)
        if real in seen_paths:
            reason = Message("the archive file appears twice in the backup chain, a cycle")
            raise _broken(archive, current, reason)
        if not current.is_file():
            raise _broken(archive, current, Message("the archive file does not exist"))
        try:
            digest = _file_digest(current)
        except OSError as exc:
            raise _broken(archive, current, Message("{error}", error=str(exc))) from exc
        if expected_digest is not None and digest != expected_digest:
            reason = Message(
                "the file has digest {digest}, not the {expected} its incremental backup records",
                digest=digest,
                expected=expected_digest,
            )
            raise _broken(archive, current, reason)
        if digest in seen_digests:
            raise _broken(archive, current, Message("the archive repeats a later backup, a cycle"))
        backup = _read_backup(current, digest, archive)
        if expected_version is not None and backup.version != expected_version:
            reason = Message(
                "it is release {version!r}, not the previous release {expected!r} its "
                "incremental backup names",
                version=backup.version,
                expected=expected_version,
            )
            raise _broken(archive, current, reason)
        if backup.version in seen_versions:
            reason = Message("release {version!r} appears twice, a cycle", version=backup.version)
            raise _broken(archive, current, reason)
        seen_paths.add(real)
        seen_digests.add(digest)
        seen_versions.add(backup.version)
        newest_first.append(backup)
        if backup.mode == MODE_FULL:
            break
        expected_digest = backup.marker["previous_backup_digest"]
        expected_version = backup.previous_release
        current = _previous_backup_path(backup, archive)
    backups = newest_first[::-1]
    _refuse_inconsistent_deletes(backups, archive)
    return backups


def _summary(
    session: StorageSession, backups: Sequence[ChainBackup], target: str, keys: Sequence[str]
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
        "source": str(backups[-1].path),
        "target": str(local_path_from_target(target)),
        "groups": groups,
        "variables": sorted(n for n in arrays if n not in coordinates),
        "coordinates": sorted(n for n in arrays if n in coordinates),
        "keys_restored": len(keys),
        "controlplane_restored": any(k.startswith(_CONTROL_PREFIX) for k in keys),
        "backup_chain": [backup.version for backup in backups],
    }


def _verify_restored(
    session: StorageSession,
    expected: Mapping[str, tuple[ChainBackup, str]],
    removed: Mapping[str, ChainBackup],
    archive: str,
) -> None:
    """Read every restored object back against the digest of the entry that wrote it.

    Raises:
        BackupChainError: An object is missing or holds other bytes, or a deleted key
            is still present.
    """
    product_uri = session.product.product_uri
    for key, (backup, digest) in expected.items():
        try:
            with session.open(product_uri.join(key), "rb") as stream:
                restored = _stream_digest(stream)
        except FileNotFoundError:
            restored = None
        if restored != digest:
            reason = Message(
                "the restored object {key!r} does not hold the archive entry's bytes", key=key
            )
            raise _broken(archive, backup.path, reason)
    root = _local_root(session)
    for key, backup in removed.items():
        if _holds_object(session, root, key):
            reason = Message("the deleted key {key!r} is still present after the restore", key=key)
            raise _broken(archive, backup.path, reason)


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


def _refuse_symlink(root: Path | None, key: str, backup: ChainBackup, archive: str) -> None:
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
            raise _broken(archive, backup.path, _symlink_reason(node, key))


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


def refuse_symlinked_target(backups: Sequence[ChainBackup], session: StorageSession) -> None:
    """Refuse a local target with a symlink on the path of any key the backup chain touches.

    The check writes nothing and reads each path component once. A remote product passes.

    Raises:
        BackupChainError: A component of such a key, the key itself included, is a symlink.
    """
    root = _local_root(session)
    if root is None or not backups:
        return
    archive = str(backups[-1].path)
    components: dict[str, tuple[ChainBackup, str]] = {}
    for backup in backups:
        for key in (*backup.entries, *backup.deleted_keys):
            parts = key.split("/")
            for end in range(1, len(parts) + 1):
                components.setdefault("/".join(parts[:end]), (backup, key))
    for component in sorted(components):
        node = root / component
        try:
            mode = node.lstat().st_mode
        except (FileNotFoundError, NotADirectoryError):
            continue
        if stat.S_ISLNK(mode):
            backup, key = components[component]
            raise _broken(archive, backup.path, _symlink_reason(node, key))


def _open_unchanged(backup: ChainBackup, archive: str) -> BinaryIO:
    """Open the file of `backup` and return the handle once its bytes hash to `backup.digest`.

    The caller reads the entries from this handle, so a file replaced after the check is
    never read. A rewrite in place of the open file is outside this check.

    Raises:
        BackupChainError: The file cannot be read or has changed since
            `resolve_backup_chain` hashed it.
    """
    try:
        handle = backup.path.open("rb")
    except OSError as exc:
        raise _broken(archive, backup.path, Message("{error}", error=str(exc))) from exc
    try:
        digest = _stream_digest(handle)
    except OSError as exc:
        handle.close()
        raise _broken(archive, backup.path, Message("{error}", error=str(exc))) from exc
    if digest != backup.digest:
        handle.close()
        reason = Message(
            "the file has digest {digest}, not the {expected} it had when the backup chain "
            "was checked",
            digest=digest,
            expected=backup.digest,
        )
        raise _broken(archive, backup.path, reason)
    handle.seek(0)
    return handle


def _put_entries(
    session: StorageSession,
    zf: zipfile.ZipFile,
    backup: ChainBackup,
    root: Path | None,
    archive: str,
) -> None:
    """Put each product key of `backup` from `zf`; refuse an entry whose bytes differ.

    The keys are the ones `resolve_backup_chain` recorded, so an entry it did not see is
    never put.
    """
    product_uri = session.product.product_uri
    for key in sorted(backup.entries):
        try:
            data = zf.read(key)
        except (KeyError, zipfile.BadZipFile, OSError) as exc:
            reason = Message("its entry {key!r} cannot be read: {error}", key=key, error=str(exc))
            raise _broken(archive, backup.path, reason) from exc
        if f"sha256:{hashlib.sha256(data).hexdigest()}" != backup.entries[key]:
            reason = Message(
                "its entry {key!r} changed after the backup chain was checked", key=key
            )
            raise _broken(archive, backup.path, reason)
        _refuse_symlink(root, key, backup, archive)
        session.put_bytes(product_uri.join(key), data)


def restore_backup_chain(
    backups: Sequence[ChainBackup],
    target: str,
    *,
    session: StorageSession,
    overwrite: bool = False,
    keep_run_id: str | None = None,
) -> dict[str, Any]:
    """Restore a resolved backup chain into the session's product, the full backup first.

    Before any write, every backup must still have the digest `resolve_backup_chain`
    recorded, and no symlink may lie on the local path of a restored key. The full backup
    then prepares the target as the zip restore does and puts its entries. Each incremental
    backup first deletes its `deleted_keys`, one literal object each, with the local
    directories that leaves empty. It then puts its entries. Every entry put must hash to
    its recorded digest. Last, every restored object is read back against its archive
    entry, and no object may remain at a deleted key.

    Args:
        backups: The backup chain `resolve_backup_chain` returned, the full backup first.
        target: Local path or `file://` URI of the product; it must name the session's product.
        session: Storage session bound to the target product.
        overwrite: Replace an existing Zarr store at `target`.
        keep_run_id: Run whose records an overwrite keeps; pass the restore's own run.

    Returns:
        The zip restore's summary of the restored product, plus `backup_chain`, the
        versions applied in order.

    Raises:
        ValueError: `backups` is empty or does not start with a full backup.
        BackupChainError: A backup or entry changed since `resolve_backup_chain`, a symlink
            lies on a restored key's path, the target lacks a key an incremental backup
            deletes, or a restored object does not match its archive entry.
        FileExistsError: `target` holds a Zarr store and `overwrite` is False.
        ConfigurationError: An overwrite finds a symlink at the target or on a kept path.
        Exception: The driver's error for a failed put or delete; the restore stops at the first.
    """
    if not backups or backups[0].mode != MODE_FULL:
        raise ValueError("A backup chain starts with a full backup")
    require_target_is_product(target, session.product.product_uri)
    archive = str(backups[-1].path)
    root = _local_root(session)
    full_backup = backups[0]
    expected: dict[str, tuple[ChainBackup, str]] = {
        key: (full_backup, digest) for key, digest in full_backup.entries.items()
    }
    removed: dict[str, ChainBackup] = {}
    for backup in backups[1:]:
        _open_unchanged(backup, archive).close()
    refuse_symlinked_target(backups, session)
    with _open_unchanged(full_backup, archive) as handle, zipfile.ZipFile(handle) as zf:
        _prepare_target(
            local_path_from_target(target), overwrite=overwrite, keep_run_id=keep_run_id
        )
        _put_entries(session, zf, full_backup, root, archive)

    product_uri = session.product.product_uri
    for backup in backups[1:]:
        # Deletes come first: a replacement key may sit where a deleted key, or its
        # directory, was. `resolve_backup_chain` refused a backup that holds and deletes a key.
        for key in backup.deleted_keys:
            uri = product_uri.join(key)
            if not session.exists(uri):
                reason = Message(
                    "the target does not hold {key!r}, which the incremental backup deletes",
                    key=key,
                )
                raise _broken(archive, backup.path, reason)
            _refuse_symlink(root, key, backup, archive)
            session.delete_literal(uri)
            _remove_empty_parents(root, key)
            expected.pop(key, None)
            removed[key] = backup
        with _open_unchanged(backup, archive) as handle, zipfile.ZipFile(handle) as zf:
            _put_entries(session, zf, backup, root, archive)
        for key in backup.entries:
            expected[key] = (backup, backup.entries[key])
            removed.pop(key, None)

    _verify_restored(session, expected, removed, archive)
    return _summary(session, backups, target, sorted(expected))
