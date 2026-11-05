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
writes the digest inventory, then a full or delta archive of the inventoried keys,
and returns the record's `retention` object. `versions_retention` builds the step
for an S3 product on a versioned bucket: it records the version id of every
object instead of copying bytes. `verify_retention` checks a release's archive and
the chain of base archives below it, or the listed object versions. Archive files
are local and read directly, outside the product's storage session.
"""

from __future__ import annotations

import hashlib
import os
import random
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Any, Final, Literal, cast

from firecube.core.controlplane.inventories import excluded_from_inventory
from firecube.core.controlplane.types import (
    INVENTORY_KIND_S3_VERSIONS,
    LIFECYCLE_UNAVAILABLE,
    Inventory,
    InventoryObject,
    canonical_json_bytes,
)
from firecube.core.errors import ControlPlaneCorruptionError, ManifestError, Message, Param
from firecube.core.filesystem.protocol import ObjectVersion, ObjectVersions
from firecube.core.filesystem.publication import is_publication_temp
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.release import ReleaseRefusedError

if TYPE_CHECKING:
    from firecube.core.controlplane.manager import ChunkManager
    from firecube.core.controlplane.releases import StoredRelease
    from firecube.core.storage.session import StorageSession

RETENTION_KIND_ZIP: Final = "zip"
RETENTION_KIND_VERSIONS: Final = INVENTORY_KIND_S3_VERSIONS
MODE_FULL: Final = "full"
MODE_DELTA: Final = "delta"
ArchiveState = Literal["present", "missing", "changed"]
"""Whether a release's archive file exists and still has the recorded digest."""
ChainState = Literal["intact", "broken"]
"""Whether every base archive below a delta archive exists and has its recorded digest."""
RecordedState = Literal["unchanged", "changed"]
"""Whether a bucket setting still equals the value the release recorded."""
InventoryState = Literal["intact", "lost"]
"""Whether every checked listed object version still exists."""
CheckedScope = Literal["all", "sample"]
"""Whether every listed object version was checked, or a random sample."""

# The Zarr v3 and v2 metadata documents; a delta archive holds each one whatever its digest.
_METADATA_DOCUMENTS: Final = frozenset({"zarr.json", ".zgroup", ".zarray", ".zattrs", ".zmetadata"})
_DIGEST_PREFIX: Final = "sha256:"
_READ_BLOCK: Final = 1 << 20


@dataclass(frozen=True)
class RetentionVerification:
    """What a release retains, and whether its archives still hold it.

    Attributes:
        version: The release version.
        kind: `"zip"`, `"s3-versions"`, or `None` when the release retains nothing.
        archive: `"present"`, `"missing"` or `"changed"` for the release's own archive,
            or `None` when it retains no zip archive.
        chain: `"intact"` when every base archive below this one is present and
            unchanged, `"broken"` otherwise, or `None` when it retains no zip archive. A
            full archive has no base, so its chain is intact.
        broken_at: The version of the first link, newest first, whose record or archive
            fails, or `None`.
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
    chain: ChainState | None = None
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
        return self.archive == "present" and self.chain == "intact"


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


def _retention_text(release: StoredRelease, name: str) -> str:
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


def _chain_length(base: StoredRelease) -> int:
    length = None if base.retention is None else base.retention.get("chain_length")
    if isinstance(length, bool) or not isinstance(length, int) or length < 1:
        raise ControlPlaneCorruptionError(
            f"the retention of release {base.version!r} has no valid chain_length"
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
    from firecube.core.archive.zip.marker import MARKER_ENTRY

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


def _refuse_unrestorable_deletes(deleted: list[str], base: str, *, version: str) -> None:
    """Refuse a delta whose marker would list a deleted key that a chain restore refuses.

    Raises:
        ReleaseRefusedError: A deleted key fails the restore's admission; nothing is archived.
    """
    from firecube.core.archive.zip.marker import deleted_key_problem

    refused = [(key, problem) for key in deleted if (problem := deleted_key_problem(key))]
    if not refused:
        return
    key, problem = refused[0]
    raise ReleaseRefusedError(
        Message(
            "The key {key!r} was deleted since release {base!r} and {problem}, so it cannot be "
            "restored from an archive chain and no delta archive lists it. Deleted keys "
            "refused: {count}. Write a full archive with {archive_full} instead.",
            key=key,
            base=base,
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
    then takes the digest inventory. It refuses a delta that deletes a key a chain
    restore refuses, and otherwise writes the inventory. It archives every inventoried key when
    the current release retains no zip archive or `archive_full` is set. Otherwise it
    archives the keys that are new or changed since that release's inventory, plus
    every metadata document. The archive is written as its own run, with `run_id` as
    the holder of the exclusive claim. Each entry is checked against the inventory.

    Returns:
        The step; it returns the record's `retention` object.

    Raises:
        ReleaseRefusedError: `archive` is not a `file://` URI.
    """
    archive_path = require_local_archive(archive, version=version)

    def retain(snapshot_id: str) -> dict[str, Any]:
        from firecube.ingestor.runtime.archive import create_archive

        refuse_existing_archive(archive_path, version=version)
        previous = manager.current_release(product)
        base = previous if not archive_full and _is_zip_retention(previous) else None
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
        if base is None:
            keys = sorted(digests)
            deleted: list[str] = []
        else:
            base_inventory = manager.read_inventory(product, _retention_text(base, "inventory"))
            base_digests = {item.key: item.digest for item in base_inventory.objects}
            keys = sorted(
                key
                for key, digest in digests.items()
                if base_digests.get(key) != digest or _is_metadata_document(key)
            )
            deleted = sorted(set(base_digests).difference(digests))
            _refuse_unrestorable_deletes(deleted, base.version, version=version)
        inventory_id = manager.write_inventory(product, inventory)
        report(f"inventory {inventory_id} lists {len(digests)} objects")
        mode = MODE_FULL if base is None else MODE_DELTA
        marker: dict[str, Any] = {
            "version": version,
            "mode": mode,
            "base": None if base is None else base.version,
            "base_uri": None if base is None else _retention_text(base, "uri"),
            "base_digest": None if base is None else _retention_text(base, "digest"),
            "inventory": inventory_id,
            "metadata_manifest": manager.read_snapshot(product, snapshot_id).record.manifest,
            "deleted": deleted,
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
        report(f"{mode} archive {archive_path} holds {len(keys)} objects, {size} bytes")
        return {
            "kind": RETENTION_KIND_ZIP,
            "uri": archive.to_str(),
            "format": "zip",
            "size_bytes": size,
            "digest": digest,
            "mode": mode,
            "base": marker["base"],
            "chain_length": 1 if base is None else _chain_length(base) + 1,
            "deleted_count": len(deleted),
            "inventory": inventory_id,
            "created_by_run": created.run_id,
        }

    return retain


def _versioned_filesystem(session: StorageSession, *, version: str) -> ObjectVersions:
    """Return the session's filesystem when it reports `ObjectVersions`.

    Raises:
        ReleaseRefusedError: The driver does not report it: the product is not on s3 or
            the driver is not fsspec.
    """
    fs = session.fs()
    if ObjectVersions not in fs.capabilities():
        raise ReleaseRefusedError(
            Message(
                "{retain_versions} needs an s3 product on the fsspec storage driver, and "
                "the driver {driver} on {product_uri} does not list object versions.",
                retain_versions=Param("retain_versions"),
                driver=session.driver.driver,
                product_uri=session.product.product_uri.to_str(),
            ),
            version=version,
        )
    return cast(ObjectVersions, fs)


def _no_usable_id(entry: ObjectVersion) -> bool:
    return entry.version_id is None or entry.version_id in {"", "null"}


def _version_inventory(session: StorageSession, *, version: str) -> Inventory:
    """List the versions below the product and return the inventory of its current objects.

    The listing is not atomic on any backend, so the inventory is coherent only
    while the operator keeps every writer of the product stopped. The release
    holds the exclusive maintenance claim, which excludes firecube's own writers
    and nothing else.

    A key counts when its entry with `is_latest` is an object. A key whose latest
    entry is a delete marker is recorded as a delete marker, not an object. A key
    with only older entries has no latest entry and is not listed: it was deleted
    at the cut.

    Raises:
        ReleaseRefusedError: The driver reports no object versions, the bucket's
            versioning is not enabled, the store does not say which entry is
            latest, or a current object has no usable version id.
        ManifestError: A key cannot be recorded literally.
    """
    fs = _versioned_filesystem(session, version=version)
    product_root = session.product.product_uri
    state = fs.bucket_versioning(product_root)
    if state != "enabled":
        raise ReleaseRefusedError(
            Message(
                "The bucket of {product_uri} has versioning {state}. {retain_versions} needs "
                "it enabled: an object written without it has no version id to record.",
                product_uri=product_root.to_str(),
                state=state,
                retain_versions=Param("retain_versions"),
            ),
            version=version,
        )
    lifecycle = fs.lifecycle(product_root)
    entries = fs.list_versions(product_root)
    if any(entry.is_latest is None for entry in entries):
        raise ReleaseRefusedError(
            Message(
                "The version listing of {product_uri} does not say which version of each "
                "key is current, so no inventory is taken.",
                product_uri=product_root.to_str(),
            ),
            version=version,
        )
    prefix = f"{product_root.path.strip('/')}/" if product_root.path.strip("/") else ""
    latest: dict[str, ObjectVersion] = {}
    for entry in entries:
        if not entry.is_latest:
            continue
        if not entry.key.startswith(prefix):
            raise ManifestError(
                f"the version listing of {product_root.to_str()} returned {entry.key!r}, "
                "which is not an object of the product"
            )
        key = entry.key[len(prefix) :]
        if any(segment in {"", ".", ".."} for segment in key.split("/")):
            raise ManifestError(
                Message(
                    "The key {key!r} of {product_uri} has an empty, '.' or '..' segment, so it "
                    "cannot be recorded literally and no inventory is taken. Rename the object.",
                    key=entry.key,
                    product_uri=product_root.to_str(),
                )
            )
        if is_publication_temp(key):
            continue
        if key in latest:
            raise ReleaseRefusedError(
                Message(
                    "The version listing of {product_uri} marks two versions of {key!r} as "
                    "current, so no inventory is taken.",
                    product_uri=product_root.to_str(),
                    key=key,
                ),
                version=version,
            )
        if not excluded_from_inventory(product_root, key):
            latest[key] = entry
    unusable = sorted(
        key
        for key, entry in latest.items()
        if (_no_usable_id(entry) if not entry.is_delete_marker else not entry.version_id)
    )
    if unusable and latest[unusable[0]].is_delete_marker:
        raise ReleaseRefusedError(
            Message(
                "The version listing of {product_uri} returned a delete marker for {key!r} "
                "with no version id, so no inventory is taken. Run again; if the store keeps "
                "answering so, it is not reporting delete markers correctly.",
                product_uri=product_root.to_str(),
                key=unusable[0],
            ),
            version=version,
        )
    if unusable:
        raise ReleaseRefusedError(
            Message(
                "{count} current objects of {product_uri} have no usable version id, the "
                "first is {key!r}: they were written before versioning was enabled. Copy "
                "each over itself to give it one, then run again.",
                count=len(unusable),
                product_uri=product_root.to_str(),
                key=unusable[0],
            ),
            version=version,
        )
    objects = tuple(
        InventoryObject(
            key=key,
            size=0 if entry.is_delete_marker else entry.size or 0,
            version_id=entry.version_id,
            etag=entry.etag,
            delete_marker=entry.is_delete_marker,
        )
        for key, entry in sorted(latest.items())
    )
    return Inventory(
        kind=INVENTORY_KIND_S3_VERSIONS,
        objects=objects,
        bucket_versioning=state,
        lifecycle=LIFECYCLE_UNAVAILABLE if lifecycle is None else lifecycle,
    )


def preflight_version_retention(session: StorageSession, *, version: str) -> None:
    """Refuse a release whose product cannot retain object versions; write nothing.

    The release run takes its cut again under the claim, so this only moves the
    refusals ahead of the stamp and the snapshot.

    Raises:
        ReleaseRefusedError: See `_version_inventory`.
        ManifestError: A key cannot be recorded literally.
    """
    _version_inventory(session, version=version)


def versions_retention(
    *,
    session: StorageSession,
    manager: ChunkManager,
    product: str,
    version: str,
    run_id: str,
    report: Callable[[str], None],
) -> Callable[[str], dict[str, Any]]:
    """Return the step that retains release `version` as the version ids of its objects.

    The step takes the snapshot id. It lists the versions of the product, writes
    the `s3-versions` inventory of the current objects and delete markers, and
    returns the record's `retention` object. It copies no bytes. The inventory is
    coherent only under the operator stop of every other writer.

    Raises:
        ReleaseRefusedError: See `_version_inventory`.
    """

    def retain(_snapshot_id: str) -> dict[str, Any]:
        inventory = _version_inventory(session, version=version)
        inventory_id = manager.write_inventory(product, inventory)
        count = sum(1 for item in inventory.objects if not item.delete_marker)
        markers = len(inventory.objects) - count
        report(
            f"inventory {inventory_id} lists {count} object versions and {markers} delete markers"
        )
        return {
            "kind": RETENTION_KIND_VERSIONS,
            "inventory": inventory_id,
            "bucket_versioning": inventory.bucket_versioning,
            "object_count": count,
            "created_by_run": run_id,
        }

    return retain


def _lifecycle_changed(recorded: Any, current: Mapping[str, Any] | None) -> bool:
    now = LIFECYCLE_UNAVAILABLE if current is None else dict(current)
    then = LIFECYCLE_UNAVAILABLE if recorded is None else recorded
    if isinstance(now, str) or isinstance(then, str):
        return now != then
    return canonical_json_bytes(dict(then)) != canonical_json_bytes(now)


def _verify_versions(
    session: StorageSession,
    manager: ChunkManager,
    release: StoredRelease,
    sample: int | None,
) -> RetentionVerification:
    """Check the bucket settings and the listed versions of a version-retaining release."""
    product = session.product.product_name
    inventory = manager.read_inventory(product, _retention_text(release, "inventory"))
    fs = _versioned_filesystem(session, version=release.version)
    product_root = session.product.product_uri
    retention = (
        "unchanged"
        if fs.bucket_versioning(product_root) == _retention_text(release, "bucket_versioning")
        else "changed"
    )
    lifecycle = (
        "changed"
        if _lifecycle_changed(inventory.lifecycle, fs.lifecycle(product_root))
        else "unchanged"
    )
    listed = [item for item in inventory.objects if not item.delete_marker]
    if sample is None or sample >= len(listed):
        targets, checked = listed, "all"
    else:
        targets, checked = random.sample(listed, sample), "sample"
    lost: list[str] = []
    for item in targets:
        assert item.version_id is not None  # an s3-versions inventory entry always has one
        try:
            found = fs.head_version(product_root.join(item.key), item.version_id)
        except OSError:
            lost.append(item.key)
            continue
        if found is None or found.is_delete_marker or found.size != item.size:
            lost.append(item.key)
    return RetentionVerification(
        version=release.version,
        kind=RETENTION_KIND_VERSIONS,
        retention=retention,
        lifecycle=lifecycle,
        inventory="lost" if lost else "intact",
        lost=len(lost),
        lost_keys=tuple(sorted(lost)),
        checked=checked,
        checked_count=len(targets),
    )


def _archive_state(release: StoredRelease) -> ArchiveState:
    """Return whether the archive a release records exists and has the recorded digest.

    Raises:
        ControlPlaneCorruptionError: The retention lacks its URI or digest, or the URI
            is not a `file://` URI.
    """
    uri = _retention_text(release, "uri")
    expected = _retention_text(release, "digest")
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


def _first_broken_base(manager: ChunkManager, product: str, release: StoredRelease) -> str | None:
    """Follow the bases of a delta release through the records; return the first broken link.

    A link is broken when its record is missing, retains no zip archive, repeats an
    earlier link, has an unknown mode, or its archive is missing or changed.
    """
    seen = {release.version}
    link = release
    while True:
        mode = None if link.retention is None else link.retention.get("mode")
        if mode == MODE_FULL:
            return None
        if mode != MODE_DELTA:
            return link.version
        try:
            base_version = _retention_text(link, "base")
        except ControlPlaneCorruptionError:
            return link.version
        base = None if base_version in seen else manager.find_release(product, base_version)
        if base is None or not _is_zip_retention(base):
            return base_version
        try:
            if _archive_state(base) != "present":
                return base_version
        except ControlPlaneCorruptionError:
            return base_version
        seen.add(base_version)
        link = base


def verify_retention(
    session: StorageSession, version: str | None = None, *, sample: int | None = None
) -> RetentionVerification:
    """Check what a release retains: its archive chain, or its listed object versions.

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
            return _verify_versions(session, manager, release, sample)
        if kind != RETENTION_KIND_ZIP:
            raise ControlPlaneCorruptionError(
                f"release {release.version!r} names retention kind {kind!r}, which is not known"
            )
        archive = _archive_state(release)
        broken_at = _first_broken_base(manager, product, release)
        return RetentionVerification(
            version=release.version,
            kind=kind,
            archive=archive,
            chain="intact" if broken_at is None else "broken",
            broken_at=broken_at,
        )
    finally:
        manager.close()
