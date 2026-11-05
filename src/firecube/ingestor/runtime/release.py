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

"""Create and verify named releases of a product as recorded maintenance runs.

`create_release_for_product` stamps the release on the root group, takes a coherent
snapshot, optionally retains the product's objects in a local zip archive, writes the
release record and makes it current, under the product's exclusive maintenance claim.
`release_in_run` does the same inside a migration's claim and run, without an archive.
`verify_release_for_product` compares the live metadata with a release.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, Literal

from firecube.core.controlplane.types import (
    LAST_MODIFIER_ATTR,
    MAINTENANCE_OP_MIGRATE,
    MAINTENANCE_OP_RELEASE_CREATE,
    RELEASE_ATTR,
    canonical_json_bytes,
)
from firecube.core.errors import FirecubeError, Message, Param
from firecube.core.zarr.metadata_documents import ABSENT
from firecube.ingestor.contracts.migration import AttributePatch, ReleaseRecord

if TYPE_CHECKING:
    from firecube.core.controlplane.manager import ChunkManager
    from firecube.core.controlplane.releases import ReleaseVerification, StoredRelease
    from firecube.core.storage.session import StorageSession
    from firecube.core.storage.uri import StorageUri
    from firecube.core.zarr.metadata_documents import MetadataDocument

_RUN_PREFIX: Final = "release-"
_SNAPSHOT_TRIGGER: Final = "release-create"
ReleaseOutcome = Literal["created", "resumed", "already-current"]
"""What a release create did: wrote the record, made an existing record current, or nothing."""

OUTCOME_CREATED: Final = "created"
OUTCOME_RESUMED: Final = "resumed"
OUTCOME_ALREADY_CURRENT: Final = "already-current"


def _ignore_line(line: str) -> None:
    """Discard a progress or warning line."""


def _error_text(exc: BaseException) -> str:
    """Return the `Message` an exception was raised with, else its text."""
    first = exc.args[0] if exc.args else None
    return first if isinstance(first, Message) else str(exc)


class ReleaseRefusedError(FirecubeError):
    """The executor refuses to create the release.

    Attributes:
        version: The release version.
    """

    def __init__(self, message: str, *, version: str) -> None:
        self.version = version
        super().__init__(message)


class ReleaseConflictError(ReleaseRefusedError):
    """A record of the version exists, and the live documents differ from its snapshot.

    A version names one state of the store, so the record is never replaced.

    Attributes:
        snapshot: The snapshot id the existing record names.
        paths: The paths that differ: added, removed or changed.
    """

    def __init__(
        self, message: str, *, version: str, snapshot: str, paths: tuple[str, ...]
    ) -> None:
        self.snapshot = snapshot
        self.paths = paths
        super().__init__(message, version=version)


@dataclass(frozen=True)
class ReleaseCreateResult:
    """What `create_release_for_product` did.

    Attributes:
        outcome: `"created"` when this run wrote the record, `"resumed"` when it made an
            existing record current, `"already-current"` when nothing was left to do.
        run_id: The maintenance run, or `None` when the release was already current.
        release: The release record.
        documents: The metadata documents the run wrote, in write order.
    """

    outcome: ReleaseOutcome
    run_id: str | None
    release: ReleaseRecord
    documents: tuple[str, ...]


@dataclass(frozen=True)
class ReleaseListing:
    """The releases of a product.

    Attributes:
        releases: Every release record, oldest first.
        current: The version of the current release, or `None` when there is none.
    """

    releases: tuple[ReleaseRecord, ...]
    current: str | None


def _frozen(value: Any) -> Any:
    """Return `value` with every mapping read-only and every list a tuple."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _frozen(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_frozen(item) for item in value)
    return value


def to_release_record(stored: StoredRelease) -> ReleaseRecord:
    """Return the public release record of a stored one."""
    return ReleaseRecord(
        version=stored.version,
        created_at=stored.created_at,
        previous=stored.previous,
        snapshot=stored.snapshot,
        runs_since_previous=stored.runs_since_previous,
        producer=None if stored.producer is None else MappingProxyType(dict(stored.producer)),
        producer_status=stored.producer_status,
        note=stored.note,
        created_by_run=stored.created_by_run,
        retention=None if stored.retention is None else _frozen(stored.retention),
    )


def _check_version(version: str, note: str | None) -> None:
    """Refuse a version or note that cannot be stored, before anything is written."""
    from firecube.core.controlplane.releases import release_key

    if note is not None and not isinstance(note, str):
        raise TypeError(f"a release note is a string, not {type(note).__name__}")
    # A lone surrogate cannot be encoded as UTF-8, so neither the record nor the stamp could hold it.
    release_key(version).encode("utf-8")
    canonical_json_bytes({"version": version, "note": note})


def _require_match(manager: ChunkManager, product: str, existing: StoredRelease) -> None:
    """Refuse when the live documents differ from the snapshot the existing record names."""
    verification = manager.verify_release(product, existing.version)
    if verification.matches:
        return
    paths = (*verification.added, *verification.removed, *verification.changed)
    raise ReleaseConflictError(
        f"release {existing.version!r} exists and names snapshot {existing.snapshot}, and "
        f"the store's metadata no longer matches it at {', '.join(paths)}; a version names "
        "one state of the store, so choose another version",
        version=existing.version,
        snapshot=existing.snapshot,
        paths=paths,
    )


def _checkpoints(
    *,
    manager: ChunkManager,
    product: str,
    version: str,
    run_id: str,
    note: str | None,
    take_snapshot: Callable[[], str],
    stamp: Callable[[], None] | None,
    report: Callable[[str], None],
    retain: Callable[[str], Mapping[str, Any]] | None = None,
    archive: StorageUri | None = None,
) -> tuple[StoredRelease, ReleaseOutcome]:
    """Finish a release from its first missing checkpoint: stamp, snapshot, retention, record, ref.

    It keeps an existing record whose snapshot matches the live documents and that
    retains `archive`, when given, and then publishes only a pending ref.
    `take_snapshot` and `retain` run only when no record exists; `retain` takes the
    snapshot id and returns the record's retention object.
    """
    existing = manager.find_release(product, version)
    if existing is not None:
        _require_match(manager, product, existing)
        if archive is not None:
            # Another run may have recorded the version between admission and the claim.
            _require_recorded_archive(existing, archive)
        current = manager.current_release(product)
        if current is not None and current.version == version:
            return existing, OUTCOME_ALREADY_CURRENT
        manager.publish_current_release(product, version, run_id=run_id)
        report(f"release {version!r} recorded before; made it current")
        return existing, OUTCOME_RESUMED
    if stamp is not None:
        stamp()
    snapshot_id = take_snapshot()
    retention = None if retain is None else retain(snapshot_id)
    stored = manager.create_release(
        product,
        version=version,
        snapshot_id=snapshot_id,
        run_id=run_id,
        note=note,
        retention=retention,
    )
    report(f"recorded release {version!r} at snapshot {snapshot_id}")
    manager.publish_current_release(product, version, run_id=run_id)
    report(f"release {version!r} is current")
    return stored, OUTCOME_CREATED


def release_in_run(
    *,
    manager: ChunkManager,
    product: str,
    version: str,
    run_id: str,
    take_snapshot: Callable[[], str],
) -> ReleaseRecord:
    """Record release `version` inside a migration's claim and run; a `ReleaseCreator`.

    The migration has stamped the root group already. A record of `version` that
    matches the live documents is kept and made current; `take_snapshot` then never runs.

    Raises:
        ReleaseVersionUnstorableError: A storage driver cannot write, list or read the
            record of `version`. The call writes nothing.
        ReleaseConflictError: A record of `version` exists, and the live documents differ
            from its snapshot.
        ManifestError: `run_id` does not hold the exclusive maintenance claim.
    """
    # A direct caller gets the same refusal as migrate, which already ran it first.
    manager.check_release_version(product, version)
    stored, _outcome = _checkpoints(
        manager=manager,
        product=product,
        version=version,
        run_id=run_id,
        note=None,
        take_snapshot=take_snapshot,
        stamp=None,
        report=_ignore_line,
    )
    return to_release_record(stored)


def _root_attributes(documents: list[MetadataDocument]) -> Mapping[str, Any]:
    from firecube.ingestor.runtime.migrate import node_views

    for view in node_views(documents):
        if view.path == "" and isinstance(view.attributes, dict):
            return view.attributes
        if view.path == "" and view.attributes is ABSENT:
            return {}
    return {}


def _stamp_release(
    *,
    session: StorageSession,
    manager: ChunkManager,
    product: str,
    version: str,
    run_id: str,
    report: Callable[[str], None],
) -> tuple[str, ...]:
    """Write `firecube_release` and `firecube_last_modifier` on the root group, the image last.

    A root group that a release run of `version` stamped already only gets its stale
    root image rebuilt. Returns the paths written.

    Raises:
        ReleaseRefusedError: A migration that did not complete stamped `version`.
    """
    from firecube.ingestor.runtime.migrate import (
        apply_metadata_plan,
        open_metadata_target,
        plan_consolidated_image,
        plan_documents,
    )

    documents = manager.enumerate_metadata_documents(product)
    attrs = _root_attributes(documents)
    modifier = attrs.get(LAST_MODIFIER_ATTR)
    if not isinstance(modifier, dict):
        modifier = {}
    operation = modifier.get("operation")
    stamped = attrs.get(RELEASE_ATTR) == version
    if stamped and operation == MAINTENANCE_OP_MIGRATE:
        raise ReleaseRefusedError(
            f"migration run {modifier.get('run_id')} of step {modifier.get('step_id')!r} "
            f"stamped release {version!r} and did not complete; run that step again",
            version=version,
        )
    if (
        stamped
        and operation == MAINTENANCE_OP_RELEASE_CREATE
        and modifier.get("release") == version
    ):
        report(
            f"run {modifier.get('run_id')} stamped release {version!r} before; "
            "resuming from the snapshot"
        )
        planned = plan_consolidated_image(documents)
    else:
        stamp = {
            "operation": MAINTENANCE_OP_RELEASE_CREATE,
            "run_id": run_id,
            "release": version,
            "at": datetime.now(UTC).isoformat(),
        }
        patch = AttributePatch("", set={RELEASE_ATTR: version, LAST_MODIFIER_ATTR: stamp})
        planned = plan_documents(documents, (patch,))
    if not planned:
        return ()
    target = open_metadata_target(session=session, manager=manager)
    written = apply_metadata_plan(target=target, owner_id=run_id, documents=planned)
    report(f"wrote {', '.join(written) or 'no documents'}")
    return written


def _close(manager: ChunkManager, warn: Callable[[str], None]) -> None:
    try:
        manager.close()
    except Exception as exc:
        warn(Message("Warning: control plane not closed: {error}", error=_error_text(exc)))


def _admit_archive(version: str, archive: StorageUri | None, archive_full: bool) -> str | None:
    """Return the local path of the release archive, or `None` without one.

    Raises:
        ReleaseRefusedError: `archive_full` is set without `archive`, or `archive` is
            not a `file://` URI.
    """
    if archive is None:
        if archive_full:
            raise ReleaseRefusedError(
                Message(
                    "{archive_full} needs {archive}: it chooses the kind of the archive to write.",
                    archive_full=Param("archive_full"),
                    archive=Param("archive"),
                ),
                version=version,
            )
        return None
    from firecube.ingestor.runtime.release_retention import require_local_archive

    return require_local_archive(archive, version=version)


def _require_recorded_archive(existing: StoredRelease, archive: StorageUri) -> None:
    """Refuse an archive that the existing record of the version does not retain.

    Raises:
        ReleaseRefusedError: The record retains nothing, or another archive.
    """
    recorded = None if existing.retention is None else existing.retention.get("uri")
    if recorded == archive.to_str():
        return
    raise ReleaseRefusedError(
        Message(
            "Release {version!r} exists and retains {recorded}, not the archive {archive_uri} "
            "given as {archive}. A record is never rewritten, so choose another version.",
            version=existing.version,
            recorded="no archive" if recorded is None else recorded,
            archive_uri=archive.to_str(),
            archive=Param("archive"),
        ),
        version=existing.version,
    )


def create_release_for_product(
    *,
    session: StorageSession,
    version: str,
    note: str | None = None,
    report: Callable[[str], None] = _ignore_line,
    warn: Callable[[str], None] = _ignore_line,
    archive: StorageUri | None = None,
    archive_full: bool = False,
) -> ReleaseCreateResult:
    """Create release `version` of the session's product as one recorded maintenance run.

    Under the exclusive maintenance claim, the run writes `firecube_release` and
    `firecube_last_modifier` on the root group, the root consolidated image last. It
    then takes a coherent snapshot. With `archive`, it writes the digest inventory and
    a zip archive of the product's objects. It then writes the release record and
    publishes `refs/current.json`. A rerun resumes after the last checkpoint reached.
    A version names one state: an existing record whose snapshot differs from the
    live metadata is refused.

    The archive is full when the current release retains no zip archive or
    `archive_full` is set. Otherwise it is a delta: the objects new or changed since
    that release, every metadata document, and the deleted keys in its marker.

    Args:
        session: The storage session bound to the product.
        version: The release name, stored verbatim; only uniqueness is required.
        note: Free text kept in the record.
        report: Receives progress lines.
        warn: Receives each bookkeeping warning line, "Warning: " included.
        archive: The `file://` URI of the zip archive to write; nothing may exist there.
        archive_full: Write a full archive even when a delta is possible.

    Returns:
        The outcome, the run and the release record.

    Raises:
        TypeError: `version` or `note` is not a string.
        UnicodeEncodeError: `version` or `note` cannot be encoded as UTF-8.
        ReleaseVersionUnstorableError: A storage driver cannot write, list or read the
            record of `version`. Nothing is written.
        ReleaseRefusedError: The store has no control plane, a migration that did not
            complete stamped `version`, `archive` is not local, something exists at it,
            `archive_full` comes without it, an existing record of `version` retains
            another archive, or the archived bytes differ from the inventory.
        ReleaseConflictError: A record of `version` exists, and the live metadata differs
            from its snapshot.
        ClaimConflictError: Another owner holds the exclusive maintenance claim.
        ExclusiveMaintenanceRefusedError: Other claims or unfinished runs exist.
        MetadataStateError: A document changed while the run wrote.
        ArchiveConversionError: The archive could not be written; the run is recorded failed.
    """
    _check_version(version, note)
    archive_path = _admit_archive(version, archive, archive_full)
    manager = session.control_plane()
    product = session.product.product_name
    try:
        # Refuse an unstorable record name before a driver reads it or the root is stamped.
        manager.check_release_version(product, version)
        if not manager.has_control_plane(product):
            raise ReleaseRefusedError(
                f"the store of product {product} has no control plane; adopt it before "
                "creating a release",
                version=version,
            )
        existing = manager.find_release(product, version)
        if existing is None and archive_path is not None:
            from firecube.ingestor.runtime.release_retention import refuse_existing_archive

            # Refused before any claim, run or inventory; the step checks again under the claim.
            refuse_existing_archive(archive_path, version=version)
        if existing is not None:
            # Read-only refusal or no-op before any claim or run.
            _require_match(manager, product, existing)
            if archive is not None:
                _require_recorded_archive(existing, archive)
            current = manager.current_release(product)
            if current is not None and current.version == version:
                report(f"release {version!r} is current already")
                return ReleaseCreateResult(
                    outcome=OUTCOME_ALREADY_CURRENT,
                    run_id=None,
                    release=to_release_record(existing),
                    documents=(),
                )
        run_id = f"{_RUN_PREFIX}{uuid.uuid4()}"
        handle = manager.acquire_exclusive_maintenance(product=product, run_id=run_id)
        try:
            return _run(
                session=session,
                manager=manager,
                product=product,
                version=version,
                note=note,
                run_id=run_id,
                report=report,
                warn=warn,
                archive=archive,
                archive_full=archive_full,
            )
        finally:
            try:
                handle.release()
            except Exception as exc:
                warn(
                    Message(
                        "Warning: claim {claim_domain} not released: {error}",
                        claim_domain=handle.info.domain,
                        error=_error_text(exc),
                    )
                )
    finally:
        _close(manager, warn)


def _run(
    *,
    session: StorageSession,
    manager: ChunkManager,
    product: str,
    version: str,
    note: str | None,
    run_id: str,
    report: Callable[[str], None],
    warn: Callable[[str], None],
    archive: StorageUri | None,
    archive_full: bool,
) -> ReleaseCreateResult:
    """Record the run, finish the release from its checkpoints, and record the outcome."""
    scope_meta: dict[str, Any] = {
        "run_kind": MAINTENANCE_OP_RELEASE_CREATE,
        "version": version,
        "note": note,
    }
    retain: Callable[[str], Mapping[str, Any]] | None = None
    if archive is not None:
        from firecube.ingestor.runtime.release_retention import zip_retention

        scope_meta.update(archive=archive.to_str(), archive_full=archive_full)
        retain = zip_retention(
            session=session,
            manager=manager,
            product=product,
            version=version,
            run_id=run_id,
            archive=archive,
            archive_full=archive_full,
            report=report,
            warn=warn,
        )
    started = False
    try:
        manager.record_maintenance_started(
            product=product, run_id=run_id, op=MAINTENANCE_OP_RELEASE_CREATE, scope_meta=scope_meta
        )
        started = True
        written: list[str] = []

        def stamp() -> None:
            written.extend(
                _stamp_release(
                    session=session,
                    manager=manager,
                    product=product,
                    version=version,
                    run_id=run_id,
                    report=report,
                )
            )

        def take_snapshot() -> str:
            snapshot = manager.take_snapshot(
                product, run_id=run_id, trigger=_SNAPSHOT_TRIGGER, consistency="coherent"
            )
            report(f"snapshot {snapshot.snapshot_id}")
            return snapshot.snapshot_id

        stored, outcome = _checkpoints(
            manager=manager,
            product=product,
            version=version,
            run_id=run_id,
            note=note,
            take_snapshot=take_snapshot,
            stamp=stamp,
            report=report,
            retain=retain,
            archive=archive,
        )
        scope_meta.update(
            outcome=outcome,
            snapshot=stored.snapshot,
            previous=stored.previous,
            documents_written=len(written),
        )
        manager.record_maintenance_completed(
            product=product, run_id=run_id, op=MAINTENANCE_OP_RELEASE_CREATE, scope_meta=scope_meta
        )
    except Exception as exc:
        try:
            if not started:
                manager.record_maintenance_started(
                    product=product,
                    run_id=run_id,
                    op=MAINTENANCE_OP_RELEASE_CREATE,
                    scope_meta=scope_meta,
                )
            manager.record_maintenance_failed(
                product=product,
                run_id=run_id,
                op=MAINTENANCE_OP_RELEASE_CREATE,
                scope_meta=scope_meta,
                error=str(exc),
            )
        except Exception as record_exc:
            warn(
                Message(
                    "Warning: release failure not recorded: {error}",
                    error=_error_text(record_exc),
                )
            )
        raise
    report(f"release {version!r} {outcome} in run {run_id}")
    return ReleaseCreateResult(
        outcome=outcome,
        run_id=run_id,
        release=to_release_record(stored),
        documents=tuple(written),
    )


def verify_release_for_product(
    *, session: StorageSession, version: str | None = None
) -> ReleaseVerification:
    """Compare the live metadata documents of the product with a release's snapshot.

    It detects metadata drift only: added, removed and changed documents. An edit of
    chunk bytes is not detected. The call writes nothing and takes no claim.

    Args:
        session: The storage session bound to the product.
        version: The release to compare; `None` names the current release.

    Raises:
        ReleaseNotFoundError: No such release, or no current release.
    """
    manager = session.control_plane()
    try:
        return manager.verify_release(session.product.product_name, version)
    finally:
        manager.close()


def list_releases_for_product(*, session: StorageSession) -> ReleaseListing:
    """Return every release of the product, oldest first, and the current version.

    The call writes nothing and takes no claim.
    """
    manager = session.control_plane()
    product = session.product.product_name
    try:
        current = manager.current_release(product)
        return ReleaseListing(
            releases=tuple(to_release_record(item) for item in manager.list_releases(product)),
            current=None if current is None else current.version,
        )
    finally:
        manager.close()


def read_release_for_product(
    *, session: StorageSession, version: str | None = None
) -> ReleaseRecord:
    """Return release `version` of the product; `None` names the current release.

    Raises:
        ReleaseNotFoundError: No such release, or no current release.
    """
    manager = session.control_plane()
    try:
        return to_release_record(manager.read_release(session.product.product_name, version))
    finally:
        manager.close()
