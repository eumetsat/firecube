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

"""Retain the objects of a release, and verify what a release retains.

`zip_retention` builds the step a release run calls after its snapshot. The step
writes the digest inventory, then a full or incremental backup of the inventoried
keys, and returns the record's `retention` object. `verify_retention` checks a
release's backup and the backup chain below it, or the listed object versions.
Backup files are local ZIP archives and read directly, outside the product's
storage session. Version retention is in `release_versions`.
"""

from __future__ import annotations

import hashlib
import os
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Any, Final, Literal

from firecube.core.archive.zip.marker import (
    MARKER_ENTRY,
    MODE_FULL,
    MODE_INCREMENTAL,
    deleted_key_problem,
)
from firecube.core.controlplane.types import INVENTORY_KIND_S3_VERSIONS
from firecube.core.errors import ControlPlaneCorruptionError, Message, Param
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.release import ReleaseRefusedError

if TYPE_CHECKING:
    from firecube.core.controlplane.manager import ChunkManager
    from firecube.core.controlplane.releases import StoredRelease
    from firecube.core.storage.session import StorageSession

RETENTION_KIND_ZIP: Final = "zip"
RETENTION_KIND_VERSIONS: Final = INVENTORY_KIND_S3_VERSIONS
ArchiveState = Literal["present", "missing", "changed"]
"""Whether a release's archive file exists and still has the recorded digest."""
BackupChainState = Literal["intact", "broken"]
"""Whether every previous backup below an incremental backup exists and has its digest."""
RecordedState = Literal["unchanged", "changed"]
"""Whether a bucket setting still equals the value the release recorded."""
InventoryState = Literal["intact", "lost"]
"""Whether every checked listed object version still exists."""
CheckedScope = Literal["all", "sample"]
"""Whether every listed object version was checked, or a random sample."""

# The Zarr v3 and v2 metadata documents; an incremental backup holds each one whatever its digest.
_METADATA_DOCUMENTS: Final = frozenset({"zarr.json", ".zgroup", ".zarray", ".zattrs", ".zmetadata"})
_DIGEST_PREFIX: Final = "sha256:"
_READ_BLOCK: Final = 1 << 20


@dataclass(frozen=True)
class RetentionVerification:
    """What a release retains, and whether its backups still hold it.

    Attributes:
        version: The release version.
        kind: `"zip"`, `"s3-versions"`, or `None` when the release retains nothing.
        archive: `"present"`, `"missing"` or `"changed"` for the release's own archive,
            or `None` when it retains no zip archive.
        backup_chain: `"intact"` when every previous backup below this one is present
            and unchanged, `"broken"` otherwise, or `None` when it retains no zip
            archive. A full backup has no previous backup, so its backup chain is intact.
        broken_at: The version of the first release, newest first, whose record or
            backup fails, or `None`.
        retention: For `"s3-versions"`, `"changed"` when the bucket's versioning state
            differs from the recorded one, else `"unchanged"`; otherwise `None`.
        lifecycle: For `"s3-versions"`, `"changed"` when the bucket's lifecycle
            configuration differs from the recorded one, else `"unchanged"`; otherwise `None`.
        inventory: For `"s3-versions"`, `"lost"` when a checked version is gone or no
            longer an object, else `"intact"`; otherwise `None`.
        lost: The number of checked versions that are lost, or `None`.
        lost_keys: The keys of the lost versions, sorted.
        checked: `"all"` when every listed version was checked, `"sample"` when a random
            sample was; `None` for a release without version retention.
        checked_count: The number of versions checked, or `None`.
    """

    version: str
    kind: str | None
    archive: ArchiveState | None = None
    backup_chain: BackupChainState | None = None
    broken_at: str | None = None
    retention: RecordedState | None = None
    lifecycle: RecordedState | None = None
    inventory: InventoryState | None = None
    lost: int | None = None
    lost_keys: tuple[str, ...] = ()
    checked: CheckedScope | None = None
    checked_count: int | None = None

    @property
    def holds(self) -> bool:
        """Whether nothing is retained, or what the release retains is intact."""
        if self.kind is None:
            return True
        if self.kind == RETENTION_KIND_VERSIONS:
            return (
                self.retention == "unchanged"
                and self.lifecycle == "unchanged"
                and self.inventory == "intact"
            )
        return self.archive == "present" and self.backup_chain == "intact"


def require_local_archive(archive: StorageUri, *, version: str) -> str:
    """Return the literal local path of `archive`.

    Raises:
        ReleaseRefusedError: `archive` is not a `file://` URI.
    """
    if archive.protocol != "file":
        raise ReleaseRefusedError(
            Message(
                "The archive {archive_uri} given as {archive} is not a local file. A release "
                "archive is written to a file:// URI; the product itself may be remote.",
                archive_uri=archive.to_str(),
                archive=Param("archive"),
            ),
            version=version,
        )
    return archive.path


def refuse_existing_archive(archive_path: str, *, version: str) -> None:
    """Refuse a release archive path that something exists at.

    A release never reuses or overwrites an archive, so its archive, inventory and
    record always come from one run.

    Raises:
        ReleaseRefusedError: A file, directory or link exists at `archive_path`.
    """
    if os.path.lexists(archive_path):
        raise ReleaseRefusedError(
            Message(
                "Something exists at {archive_path}, the path given as {archive}. A release "
                "never reuses or overwrites an archive: remove it or pick another path, then "
                "run again.",
                archive_path=archive_path,
                archive=Param("archive"),
            ),
            version=version,
        )


def _read_digest(stream: IO[bytes]) -> tuple[int, str]:
    """Return the length and the `sha256:<hex>` digest of what `stream` holds."""
    digest = hashlib.sha256()
    size = 0
    while block := stream.read(_READ_BLOCK):
        digest.update(block)
        size += len(block)
    return size, f"{_DIGEST_PREFIX}{digest.hexdigest()}"


def _file_digest(path: str) -> tuple[int, str]:
    """Return the size and the `sha256:<hex>` digest of a local file."""
    with open(path, "rb") as stream:
        return _read_digest(stream)


def retention_text(release: StoredRelease, name: str) -> str:
    """Return a text field of a release's retention object.

    Raises:
        ControlPlaneCorruptionError: The release retains nothing, or the field is not text.
    """
    value = None if release.retention is None else release.retention.get(name)
    if not isinstance(value, str) or not value:
        raise ControlPlaneCorruptionError(
            f"the retention of release {release.version!r} has no {name}"
        )
    return value


def _is_zip_retention(release: StoredRelease | None) -> bool:
    return (
        release is not None
        and release.retention is not None
        and release.retention.get("kind") == RETENTION_KIND_ZIP
    )


def _is_metadata_document(key: str) -> bool:
    return key.rpartition("/")[2] in _METADATA_DOCUMENTS


def _chain_length(previous_release: StoredRelease) -> int:
    retention = previous_release.retention
    length = None if retention is None else retention.get("chain_length")
    if isinstance(length, bool) or not isinstance(length, int) or length < 1:
        raise ControlPlaneCorruptionError(
            f"the retention of release {previous_release.version!r} has no valid chain_length"
        )
    return length


def _require_inventory_bytes(
    archive_path: str, digests: Mapping[str, str], keys: list[str], *, version: str
) -> None:
    """Refuse an archive whose entries are not exactly `keys` with the inventory's digests.

    Raises:
        ReleaseRefusedError: An entry is missing, extra, or holds other bytes than the
            inventory recorded; the archive file is kept.
    """

    def refuse(key: str) -> ReleaseRefusedError:
        return ReleaseRefusedError(
            Message(
                "The archive {archive_path} does not hold {key!r} as the inventory of release "
                "{version!r} recorded it: the product changed while the release ran. Stop "
                "every writer, remove the archive and run again.",
                archive_path=archive_path,
                key=key,
                version=version,
            ),
            version=version,
        )

    with zipfile.ZipFile(archive_path) as zf:
        names = sorted(name for name in zf.namelist() if name != MARKER_ENTRY)
        if names != keys:
            differing = sorted(set(names).symmetric_difference(keys))
            raise refuse(differing[0] if differing else MARKER_ENTRY)
        for name in names:
            with zf.open(name) as entry:
                _size, digest = _read_digest(entry)
            if digest != digests[name]:
                raise refuse(name)


def _refuse_unrestorable_deletes(
    deleted_keys: list[str], previous_release: str, *, version: str
) -> None:
    """Refuse an incremental backup whose marker lists a deleted key a restore refuses.

    Raises:
        ReleaseRefusedError: A deleted key fails the restore's admission; nothing is archived.
    """
    refused = [(key, problem) for key in deleted_keys if (problem := deleted_key_problem(key))]
    if not refused:
        return
    key, problem = refused[0]
    raise ReleaseRefusedError(
        Message(
            "The key {key!r} was deleted since release {previous_release!r} and {problem}, so "
            "it cannot be restored from a backup chain and no incremental backup lists it. "
            "Deleted keys refused: {count}. Write a full backup with {archive_full} instead.",
            key=key,
            previous_release=previous_release,
            problem=problem,
            count=len(refused),
            archive_full=Param("archive_full"),
        ),
        version=version,
    )


def zip_retention(
    *,
    session: StorageSession,
    manager: ChunkManager,
    product: str,
    version: str,
    run_id: str,
    archive: StorageUri,
    archive_full: bool,
    report: Callable[[str], None],
    warn: Callable[[str], None],
) -> Callable[[str], dict[str, Any]]:
    """Return the step that retains release `version` in a zip archive at `archive`.

    The step takes the snapshot id. It refuses an existing file at the archive path,
    then takes the digest inventory. It refuses an incremental backup that deletes a key
    a backup chain restore refuses, and otherwise writes the inventory. It writes a full
    backup of every inventoried key when the current release retains no zip archive or
    `archive_full` is set. Otherwise it writes an incremental backup of the keys that are
    new or changed since that release's inventory, plus every metadata document. The
    archive is written as its own run, with `run_id` as the holder of the exclusive
    claim. Each entry is checked against the inventory.

    Returns:
        The step; it returns the record's `retention` object.

    Raises:
        ReleaseRefusedError: `archive` is not a `file://` URI.
    """
    archive_path = require_local_archive(archive, version=version)

    def retain(snapshot_id: str) -> dict[str, Any]:
        from firecube.ingestor.runtime.archive import create_archive

        refuse_existing_archive(archive_path, version=version)
        current = manager.current_release(product)
        previous = current if not archive_full and _is_zip_retention(current) else None
        inventory = manager.take_digest_inventory(product)
        digests = {item.key: str(item.digest) for item in inventory.objects}
        if "zarr.json" not in digests:
            raise ReleaseRefusedError(
                Message(
                    "The product {product} has no Zarr v3 root document zarr.json, so it "
                    "cannot be retained in a zip archive.",
                    product=product,
                ),
                version=version,
            )
        if previous is None:
            keys = sorted(digests)
            deleted_keys: list[str] = []
        else:
            previous_inventory = manager.read_inventory(
                product, retention_text(previous, "inventory")
            )
            previous_digests = {item.key: item.digest for item in previous_inventory.objects}
            keys = sorted(
                key
                for key, digest in digests.items()
                if previous_digests.get(key) != digest or _is_metadata_document(key)
            )
            deleted_keys = sorted(set(previous_digests).difference(digests))
            _refuse_unrestorable_deletes(deleted_keys, previous.version, version=version)
        inventory_id = manager.write_inventory(product, inventory)
        report(f"inventory {inventory_id} lists {len(digests)} objects")
        mode = MODE_FULL if previous is None else MODE_INCREMENTAL
        marker: dict[str, Any] = {
            "version": version,
            "mode": mode,
            "previous_release": None if previous is None else previous.version,
            "previous_backup_uri": None if previous is None else retention_text(previous, "uri"),
            "previous_backup_digest": (
                None if previous is None else retention_text(previous, "digest")
            ),
            "inventory": inventory_id,
            "metadata_manifest": manager.read_snapshot(product, snapshot_id).record.manifest,
            "deleted_keys": deleted_keys,
        }
        created = create_archive(
            session=session,
            archive=archive.to_str(),
            archive_path=archive_path,
            archive_format="zip",
            warn=warn,
            holder_run_id=run_id,
            keys=keys,
            marker=marker,
        )
        _require_inventory_bytes(archive_path, digests, keys, version=version)
        size, digest = _file_digest(archive_path)
        report(f"{mode} backup {archive_path} holds {len(keys)} objects, {size} bytes")
        return {
            "kind": RETENTION_KIND_ZIP,
            "uri": archive.to_str(),
            "format": "zip",
            "size_bytes": size,
            "digest": digest,
            "mode": mode,
            "previous_release": marker["previous_release"],
            "chain_length": 1 if previous is None else _chain_length(previous) + 1,
            "deleted_key_count": len(deleted_keys),
            "inventory": inventory_id,
            "created_by_run": created.run_id,
        }

    return retain


def _archive_state(release: StoredRelease) -> ArchiveState:
    """Return whether the archive a release records exists and has the recorded digest.

    Raises:
        ControlPlaneCorruptionError: The retention lacks its URI or digest, or the URI
            is not a `file://` URI.
    """
    uri = retention_text(release, "uri")
    expected = retention_text(release, "digest")
    try:
        parsed = StorageUri.parse(uri)
    except ValueError as exc:
        raise ControlPlaneCorruptionError(
            f"the retention of release {release.version!r} names {uri!r}, not a URI: {exc}"
        ) from exc
    if parsed.protocol != "file":
        raise ControlPlaneCorruptionError(
            f"the retention of release {release.version!r} names {uri!r}, not a local file"
        )
    if not os.path.isfile(parsed.path):
        return "missing"
    _size, digest = _file_digest(parsed.path)
    return "present" if digest == expected else "changed"


def _first_broken_previous_backup(
    manager: ChunkManager, product: str, release: StoredRelease
) -> str | None:
    """Follow the previous releases of an incremental backup; return the first broken one.

    A release is broken when its record is missing, retains no zip archive, repeats an
    earlier release, has an unknown mode, or its backup is missing or changed.
    """
    seen = {release.version}
    backup_release = release
    while True:
        retention = backup_release.retention
        mode = None if retention is None else retention.get("mode")
        if mode == MODE_FULL:
            return None
        if mode != MODE_INCREMENTAL:
            return backup_release.version
        try:
            previous_release_version = retention_text(backup_release, "previous_release")
        except ControlPlaneCorruptionError:
            return backup_release.version
        previous_release = (
            None
            if previous_release_version in seen
            else manager.find_release(product, previous_release_version)
        )
        if previous_release is None or not _is_zip_retention(previous_release):
            return previous_release_version
        try:
            if _archive_state(previous_release) != "present":
                return previous_release_version
        except ControlPlaneCorruptionError:
            return previous_release_version
        seen.add(previous_release_version)
        backup_release = previous_release


def verify_retention(
    session: StorageSession, version: str | None = None, *, sample: int | None = None
) -> RetentionVerification:
    """Check what a release retains: its backup chain, or its listed object versions.

    For a zip release the call hashes each local archive file whole. For a
    version-retaining release it re-reads the bucket's versioning and lifecycle and
    sends one HEAD per listed object version, or per version of a random sample. A
    HEAD that fails counts as a lost version. The call writes nothing and takes no claim.

    Args:
        session: The storage session bound to the product.
        version: The release to check; `None` names the current release.
        sample: Check this many randomly chosen versions instead of all; a zip release
            ignores it.

    Raises:
        ValueError: `sample` is less than 1.
        ReleaseNotFoundError: No such release, or no current release.
        ReleaseRefusedError: The release retains object versions and the driver lists none.
        ControlPlaneCorruptionError: The release's retention object is malformed or of
            an unknown kind.
    """
    from firecube.ingestor.runtime.release_versions import verify_versions

    if sample is not None and sample < 1:
        raise ValueError(f"sample must be at least 1, not {sample}")
    manager = session.control_plane()
    product = session.product.product_name
    try:
        release = manager.read_release(product, version)
        if release.retention is None:
            return RetentionVerification(version=release.version, kind=None)
        kind = release.retention.get("kind")
        if kind == RETENTION_KIND_VERSIONS:
            return verify_versions(session, manager, release, sample)
        if kind != RETENTION_KIND_ZIP:
            raise ControlPlaneCorruptionError(
                f"release {release.version!r} names retention kind {kind!r}, which is not known"
            )
        archive = _archive_state(release)
        broken_at = _first_broken_previous_backup(manager, product, release)
        return RetentionVerification(
            version=release.version,
            kind=kind,
            archive=archive,
            backup_chain="intact" if broken_at is None else "broken",
            broken_at=broken_at,
        )
    finally:
        manager.close()
