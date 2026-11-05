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

"""Retain the objects of a release in a local zip archive, and verify what a release retains.

`zip_retention` builds the step a release run calls after its snapshot. The step
writes the digest inventory, then a full or delta archive of the inventoried keys,
and returns the record's `retention` object. `verify_retention` checks a release's
archive and the chain of base archives below it. Archive files are local and read
directly, outside the product's storage session.
"""

from __future__ import annotations

import hashlib
import os
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Any, Final, Literal

from firecube.core.errors import ControlPlaneCorruptionError, Message, Param
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.release import ReleaseRefusedError

if TYPE_CHECKING:
    from firecube.core.controlplane.manager import ChunkManager
    from firecube.core.controlplane.releases import StoredRelease
    from firecube.core.storage.session import StorageSession

RETENTION_KIND_ZIP: Final = "zip"
MODE_FULL: Final = "full"
MODE_DELTA: Final = "delta"
ArchiveState = Literal["present", "missing", "changed"]
"""Whether a release's archive file exists and still has the recorded digest."""
ChainState = Literal["intact", "broken"]
"""Whether every base archive below a delta archive exists and has its recorded digest."""

# The Zarr v3 and v2 metadata documents; a delta archive holds each one whatever its digest.
_METADATA_DOCUMENTS: Final = frozenset({"zarr.json", ".zgroup", ".zarray", ".zattrs", ".zmetadata"})
_DIGEST_PREFIX: Final = "sha256:"
_READ_BLOCK: Final = 1 << 20


@dataclass(frozen=True)
class RetentionVerification:
    """What a release retains, and whether its archives still hold it.

    Attributes:
        version: The release version.
        kind: `"zip"`, or `None` when the release retains nothing.
        archive: `"present"`, `"missing"` or `"changed"` for the release's own archive,
            or `None` when it retains nothing.
        chain: `"intact"` when every base archive below this one is present and
            unchanged, `"broken"` otherwise, or `None` when it retains nothing. A full
            archive has no base, so its chain is intact.
        broken_at: The version of the first link, newest first, whose record or archive
            fails, or `None`.
    """

    version: str
    kind: str | None
    archive: ArchiveState | None = None
    chain: ChainState | None = None
    broken_at: str | None = None

    @property
    def holds(self) -> bool:
        """Whether nothing is retained, or the archive is present and its chain intact."""
        return self.kind is None or (self.archive == "present" and self.chain == "intact")


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
    from firecube.core.zarr.zip_archive import MARKER_ENTRY

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
    then takes and writes the digest inventory. It archives every inventoried key when
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


def verify_retention(session: StorageSession, version: str | None = None) -> RetentionVerification:
    """Check the archive of a release and the chain of base archives below it.

    The call hashes each local archive file whole, reads the records only, writes
    nothing and takes no claim.

    Args:
        session: The storage session bound to the product.
        version: The release to check; `None` names the current release.

    Raises:
        ReleaseNotFoundError: No such release, or no current release.
        ControlPlaneCorruptionError: The release's retention object is malformed or of
            an unknown kind.
    """
    manager = session.control_plane()
    product = session.product.product_name
    try:
        release = manager.read_release(product, version)
        if release.retention is None:
            return RetentionVerification(version=release.version, kind=None)
        kind = release.retention.get("kind")
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
