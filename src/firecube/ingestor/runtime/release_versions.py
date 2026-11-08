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

"""Retain the object versions of a release, and verify them.

`versions_retention` builds the step for an S3 product on a versioned bucket: it
records the version id of every object instead of copying bytes.
`preflight_version_retention` moves the refusals of that step ahead of the stamp.
`verify_versions` checks the bucket settings and the listed versions of a release
that retains them.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

from firecube.core.controlplane.inventories import excluded_from_inventory
from firecube.core.controlplane.types import (
    INVENTORY_KIND_S3_VERSIONS,
    LIFECYCLE_UNAVAILABLE,
    Inventory,
    InventoryObject,
    canonical_json_bytes,
)
from firecube.core.errors import ManifestError, Message, Param
from firecube.core.filesystem.protocol import ObjectVersion, ObjectVersions
from firecube.core.filesystem.publication import is_publication_temp
from firecube.ingestor.runtime.release import ReleaseRefusedError
from firecube.ingestor.runtime.release_retention import (
    RETENTION_KIND_VERSIONS,
    RetentionVerification,
    retention_text,
)

if TYPE_CHECKING:
    from firecube.core.controlplane.manager import ChunkManager
    from firecube.core.controlplane.releases import StoredRelease
    from firecube.core.storage.session import StorageSession


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


def preflight_version_retention(session: StorageSession, *, version: str) -> dict[str, Any]:
    """Refuse a release whose product cannot retain object versions; write nothing.

    The release run takes its cut again under the claim, so this only moves the
    refusals ahead of the stamp and the snapshot.

    Returns:
        The retention the cut taken now would record, without an inventory id or run.

    Raises:
        ReleaseRefusedError: See `_version_inventory`.
        ManifestError: A key cannot be recorded literally.
    """
    inventory = _version_inventory(session, version=version)
    count = sum(1 for item in inventory.objects if not item.delete_marker)
    return {
        "kind": RETENTION_KIND_VERSIONS,
        "bucket_versioning": inventory.bucket_versioning,
        "object_count": count,
        "delete_marker_count": len(inventory.objects) - count,
    }


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


def verify_versions(
    session: StorageSession,
    manager: ChunkManager,
    release: StoredRelease,
    sample: int | None,
) -> RetentionVerification:
    """Check the bucket settings and the listed versions of a version-retaining release."""
    product = session.product.product_name
    inventory = manager.read_inventory(product, retention_text(release, "inventory"))
    fs = _versioned_filesystem(session, version=release.version)
    product_root = session.product.product_uri
    retention = (
        "unchanged"
        if fs.bucket_versioning(product_root) == retention_text(release, "bucket_versioning")
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
