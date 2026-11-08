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

"""Create and restore Zip (.zip) and Tensogram (.tgm) archives as recorded maintenance runs.

`create_archive` writes a product or a plain Zarr store to one local archive file.
`restore_archive` writes a local archive back to a product.
`read_restore_source` checks an archive and reads the product label it records.
`restore_label` names the label a restore takes.
Both writers hold the product's maintenance claim while they write.
With `dry_run`, each makes its refusals from reads only and returns its plan.
Bookkeeping warnings go to the caller's `warn` callable.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from firecube.core.controlplane.types import (
    CONTROL_DIRNAME,
    MAINTENANCE_OP_ARCHIVE_CREATE,
    MAINTENANCE_OP_ARCHIVE_RESTORE,
    SCHEMA_FILENAME,
    WriteDomain,
)
from firecube.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    FirecubeError,
    Message,
    Param,
    Placeholder,
    ProductLabelMismatchError,
    ProductUnderMaintenanceError,
)
from firecube.core.uris import is_remote_target, local_path_from_target
from firecube.ingestor.runtime.recording import SpanRecorder
from firecube.ingestor.types.context import IngestResult
from firecube.ingestor.types.result_metrics import OutputPaths

if TYPE_CHECKING:
    from firecube.core.archive.zip.backup_chain import ChainBackup
    from firecube.core.product.open import Product

_ARCHIVE_FORMATS = ("zip", "tensogram")


class ArchiveClaimError(FirecubeError):
    """The product's claims refuse the archive operation.

    Attributes:
        product: The logical product name.
        operation: The maintenance operation refused, `"archive_create"` or `"archive_restore"`.
    """

    def __init__(self, message: str, *, product: str, operation: str) -> None:
        self.product = product
        self.operation = operation
        super().__init__(message)


class ArchiveConversionError(FirecubeError):
    """The conversion between the store and the archive failed.

    The message is the original error's text, and the original error is the cause.

    Attributes:
        archive: The archive reference the run records.
    """

    def __init__(self, message: str, *, archive: str) -> None:
        self.archive = archive
        super().__init__(message)


class ArchiveRecordError(FirecubeError):
    """The call wrote the archive, but recording its run in the product's control plane failed.

    Attributes:
        archive: The location of the written archive.
        product_uri: The URI of the product whose control plane refused the record.
    """

    def __init__(self, message: str, *, archive: str, product_uri: str) -> None:
        self.archive = archive
        self.product_uri = product_uri
        super().__init__(message)


@dataclass(frozen=True)
class ArchiveCreateResult:
    """What `create_archive` wrote.

    Attributes:
        archive_format: `"zip"` or `"tensogram"`.
        target: The location of the written archive.
        file_size_bytes: The size of the written archive in bytes.
        run_id: The maintenance run recorded in the source product, or `None` for a
            plain Zarr store.
        tgm_summary: The tensogram conversion's summary, or `None` for a zip archive.
            Its keys include `groups`, `variables`, `skipped`, `time_range` and `time_ranges`.
    """

    archive_format: str
    target: str
    file_size_bytes: int
    run_id: str | None
    tgm_summary: Mapping[str, Any] | None


@dataclass(frozen=True)
class ArchiveCreatePlan:
    """What `create_archive` would write, as its dry run reads it.

    Attributes:
        archive_format: `"zip"` or `"tensogram"`.
        source: The URI of the store to archive.
        archive_path: The absolute local path the archive would be written to.
        is_product: The store has a control plane, so the run would take its claim and record a run.
        group: The one product group to archive, or `None`.
        variables: The variable names to archive, or `None` for every variable.
        start_date: The ISO 8601 start of the time range, or `None`.
        end_date: The ISO 8601 end of the time range, or `None`.
        compression: The tensogram codec. A zip archive ignores it.
        overwrite: The run would replace an existing archive at `archive_path`.
    """

    archive_format: str
    source: str
    archive_path: str
    is_product: bool
    group: str | None
    variables: tuple[str, ...] | None
    start_date: str | None
    end_date: str | None
    compression: str
    overwrite: bool


@dataclass(frozen=True)
class ArchiveRestoreSource:
    """A local archive checked for `restore_archive`, and the product label it records.

    Attributes:
        archive_path: The absolute local path of the archive file.
        archive_format: `"zip"` or `"tensogram"`.
        product_label: The label the archive records, or `None` when it records none.
        label_stamp: The bytes of the zip archive's `.firecube/product.json` entry, or
            `None` when it holds none. A .tgm archive holds none.
        backup_chain: The checked backup chain of an incremental zip backup, the full
            backup first, or `None` for any other archive.
    """

    archive_path: str
    archive_format: str
    product_label: str | None
    label_stamp: bytes | None
    backup_chain: tuple[ChainBackup, ...] | None


@dataclass(frozen=True)
class ArchiveRestoreResult:
    """What `restore_archive` restored.

    Attributes:
        run_id: The run recorded in the target product.
        summary: The restore's summary: `target`, `groups`, `variables`, `coordinates`
            and `controlplane_restored`.
    """

    run_id: str
    summary: Mapping[str, Any]


@dataclass(frozen=True)
class ArchiveRestorePlan:
    """What `restore_archive` would restore, as its dry run reads it.

    Attributes:
        source: The checked archive and the label it records.
        target: The product URI the restore would write.
        label: The label the restored product takes.
        overwrite: The run would replace an existing store at `target`.
    """

    source: ArchiveRestoreSource
    target: str
    label: str
    overwrite: bool


def _ignore_line(line: str) -> None:
    """Discard a progress or warning line."""


def _error_text(exc: BaseException) -> str:
    """Return the `Message` an exception was raised with, else its text."""
    first = exc.args[0] if exc.args else None
    return first if isinstance(first, Message) else str(exc)


def _admit_maintenance_claim(
    *, manager, product: str, operation: str, holder_run_id: str | None = None
) -> None:
    """Refuse the maintenance claim when the product's held claims refuse it; read only.

    With `holder_run_id`, the product's exclusive maintenance claim must exist, and it is
    the one claim the listing lets through when that run owns it.

    Raises:
        ArchiveClaimError: The product has an active claim, or `holder_run_id` names a
            product without claims.
    """
    # Function-scope import: keeps the control-plane manager out of module import.
    from firecube.core.controlplane.manager import (
        holder_absent_message,
        is_exclusive_maintenance_holder,
        maintenance_refusal_message,
    )

    active_claims = manager.list_claims(product=product)
    blocking_claims = [
        claim
        for claim in active_claims
        if not is_exclusive_maintenance_holder(claim, product=product, holder_run_id=holder_run_id)
    ]
    if blocking_claims:
        details = ", ".join(f"{claim.domain} (owner={claim.owner_id})" for claim in blocking_claims)
        raise ArchiveClaimError(
            maintenance_refusal_message(
                repo=manager.repo,
                product=product,
                operation=operation,
                detail=f"active write claim(s) exist: {details}",
                claims=blocking_claims,
            ),
            product=product,
            operation=operation,
        )
    if holder_run_id is not None and not active_claims:
        raise ArchiveClaimError(
            holder_absent_message(
                product=product, operation=operation, holder_run_id=holder_run_id
            ),
            product=product,
            operation=operation,
        )


def _acquire_maintenance_claim(
    *,
    manager,
    product: str,
    operation: str,
    owner_id: str,
    warn: Callable[[str], None],
    holder_run_id: str | None = None,
) -> tuple[WriteDomain, Any]:
    """Acquire the maintenance claim, then check the exclusive maintenance claim under it.

    The held claims are listed first, as `_admit_maintenance_claim` lists them. With
    `holder_run_id`, the product's exclusive maintenance claim must be owned by it. A
    failed release of the acquired claim goes to `warn`.

    Returns:
        The claim's domain and the handle whose release stops the heartbeat.

    Raises:
        ArchiveClaimError: The product has an active claim, the acquisition conflicts, the
            product is under exclusive maintenance, or `holder_run_id` does not own its
            exclusive maintenance claim; the acquired claim is released first.
    """
    # Function-scope import: keeps the control-plane manager out of module import.
    from firecube.core.controlplane.manager import (
        exclusive_maintenance_holder_refusal,
        maintenance_refusal_message,
    )

    _admit_maintenance_claim(
        manager=manager, product=product, operation=operation, holder_run_id=holder_run_id
    )
    domain = WriteDomain(product=product, category="maintenance", name=operation)
    try:
        # The refusal below names the owner and the commands, so the conflict detail stays off.
        handle = manager.acquire_claim(
            product=product, domain=domain, owner_id=owner_id, describe_conflict=False
        )
    except ClaimConflictError as exc:
        blocking: list[Any] = []
        with contextlib.suppress(Exception):
            blocking = manager.list_claims(product=product)
        raise ArchiveClaimError(
            maintenance_refusal_message(
                repo=manager.repo,
                product=product,
                operation=operation,
                detail=_error_text(exc) or "write claim acquisition failed",
                claims=blocking,
            ),
            product=product,
            operation=operation,
        ) from exc
    # An exclusive claim taken, released or changed between the listing and the acquire
    # refuses here.
    try:
        if holder_run_id is None:
            manager.check_exclusive_maintenance(product=product, owner_id=owner_id)
            return domain, handle
        refusal = exclusive_maintenance_holder_refusal(
            repo=manager.repo, product=product, operation=operation, holder_run_id=holder_run_id
        )
    except ProductUnderMaintenanceError as exc:
        _release_claim(handle, warn=warn)
        raise ArchiveClaimError(
            exc.message or str(exc), product=product, operation=operation
        ) from exc
    except BaseException:
        _release_claim(handle, warn=warn)
        raise
    if refusal is not None:
        _release_claim(handle, warn=warn)
        raise ArchiveClaimError(refusal, product=product, operation=operation)
    return domain, handle


def _release_claim(claim_handle: Any, *, warn: Callable[[str], None]) -> None:
    """Release a held claim, or do nothing without one.

    A failed release goes to `warn` as a warning, so it never replaces a pending error.
    """
    # Release stops the heartbeat and retires only the record generation this handle wrote.
    if claim_handle is not None:
        try:
            claim_handle.release()
        except Exception as exc:
            # Warn, never raise: a raise here replaces the archive or record error.
            warn(
                Message(
                    "Warning: claim {claim_domain} at {claim_path} not released: {error}",
                    claim_domain=claim_handle.info.domain,
                    claim_path=claim_handle.claim_path.to_str(),
                    error=_error_text(exc),
                )
            )


def _record_archive_create(
    *,
    manager,
    product: str,
    run_id: str,
    scope_meta: dict[str, Any],
    error: str | None = None,
) -> None:
    """Record a finished archive create as a maintenance run of the source product.

    Call it after the archive work, so the archive never contains an unfinished
    run. Record failures propagate to the caller.
    """
    manager.record_maintenance_started(
        product=product, run_id=run_id, op=MAINTENANCE_OP_ARCHIVE_CREATE, scope_meta=scope_meta
    )
    if error is None:
        manager.record_maintenance_completed(
            product=product, run_id=run_id, op=MAINTENANCE_OP_ARCHIVE_CREATE, scope_meta=scope_meta
        )
    else:
        manager.record_maintenance_failed(
            product=product,
            run_id=run_id,
            op=MAINTENANCE_OP_ARCHIVE_CREATE,
            scope_meta=scope_meta,
            error=error,
        )


def _record_archive_create_failure(
    *,
    manager,
    product: str,
    run_id: str,
    scope_meta: dict[str, Any],
    error: str,
    warn: Callable[[str], None],
) -> None:
    """Record a failed archive create, warning on a record failure so the archive error survives."""
    try:
        _record_archive_create(
            manager=manager, product=product, run_id=run_id, scope_meta=scope_meta, error=error
        )
    except Exception as exc:
        warn(
            Message("Warning: archive create failure not recorded: {error}", error=_error_text(exc))
        )


def _refuse_existing_archive(archive_path: str, *, archive_format: str, overwrite: bool) -> None:
    """Refuse an existing archive file without `overwrite`, in the text its format's writer uses.

    Mirrors the existence checks of `zarr_to_zip` and `zarr_to_tgm`.

    Raises:
        FileExistsError: A file exists at `archive_path` and `overwrite` is not set.
    """
    if overwrite or not local_path_from_target(archive_path).exists():
        return
    hint = ". Use overwrite=True to replace it." if archive_format == "tensogram" else ""
    raise FileExistsError(f"Target file already exists: {archive_path}{hint}")


def _admit_create_paths(
    product: Product,
    *,
    archive_path: str,
    archive_format: str,
    overwrite: bool,
    keys: Iterable[str] | None,
) -> None:
    """Refuse the archive path and the source as the format's writer does first; read only.

    Mirrors the checks `zarr_to_zip` and `zarr_to_tgm` make before they write, in their order.
    The zip writer's check of each named key is not mirrored.

    Raises:
        ValueError: The archive path is remote, a directory or inside the source, or the
            source has no root `zarr.json` or holds the zip marker entry.
        FileExistsError: The archive file exists and `overwrite` is not set.
        OptionalExtraMissingError: A tensogram archive lacks the tensogram extras.
    """
    session = product.session
    source_uri = product.identity.product_uri
    if archive_format == "zip":
        from firecube.core.archive.zip.marker import MARKER_ENTRY

        if is_remote_target(archive_path):
            raise ValueError(f"Remote target not yet supported for zip output: {archive_path}")
        target_path = local_path_from_target(archive_path)
        if target_path.is_dir():
            raise ValueError(f"Target is a directory, expected a file path: {archive_path}")
        _refuse_existing_archive(archive_path, archive_format=archive_format, overwrite=overwrite)
        if not source_uri.is_remote():
            source_path = local_path_from_target(source_uri.to_str()).resolve()
            if target_path.resolve().is_relative_to(source_path):
                raise ValueError(f"Target must not be inside the source store: {archive_path}")
        if not session.exists(source_uri.join("zarr.json")):
            raise ValueError(f"No Zarr v3 root metadata found at {source_uri.to_str()}")
        if keys is None and session.exists(source_uri.join(MARKER_ENTRY)):
            raise ValueError(f"Keys not in the product or reserved for the archive: {MARKER_ENTRY}")
        return
    from firecube.core.archive.tensogram._compat import require_tensogram

    require_tensogram("zarr_to_tgm")
    if is_remote_target(archive_path):
        raise ValueError(
            f"Remote target not yet supported for tgm output: {archive_path}. "
            "Write to a local path and upload separately."
        )
    _refuse_existing_archive(archive_path, archive_format=archive_format, overwrite=overwrite)
    # The tensogram writer opens the source's root group first; a missing store refuses here.
    session.zarr.open_group(source_uri, mode="r")


def _preflight_create(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool,
    group: str | None,
    variables: list[str] | None,
    start_date: str | None,
    end_date: str | None,
    compression: str,
    holder_run_id: str | None,
    keys: Iterable[str] | None,
    marker: Mapping[str, Any] | None,
) -> ArchiveCreatePlan:
    """Make each refusal of `create_archive` that reads can make, and return its plan.

    Reads only: the call takes no claim and writes nothing. The order is the run's: the
    options, the held claims, then the archive path and the source.

    Raises:
        ValueError: `archive_format` is unknown, or a tensogram archive gets `keys` or `marker`.
        ConfigurationError: A zip archive gets a group, a variable list or a date.
        ArchiveClaimError: `holder_run_id` names a store that is not a product, or the
            product's held claims refuse the claim.
        ArchiveConversionError: The archive path or the source fails the writer's first checks.
    """
    if archive_format not in _ARCHIVE_FORMATS:
        raise ValueError(f"archive_format must be 'zip' or 'tensogram', not {archive_format!r}")
    if archive_format == "zip" and (group or start_date or end_date or variables):
        raise ConfigurationError(
            Message(
                "Zip archive does not support {group}, {start_date}, {end_date}, or {variables}",
                group=Param("group"),
                start_date=Param("start_date"),
                end_date=Param("end_date"),
                variables=Param("variables"),
            )
        )
    if archive_format == "tensogram" and (keys is not None or marker is not None):
        raise ValueError("Tensogram archive does not support keys or marker")
    source_uri = product.identity.product_uri
    is_product = product.session.exists(source_uri.join(CONTROL_DIRNAME, SCHEMA_FILENAME))
    if not is_product and holder_run_id is not None:
        # A store without a control plane is never admitted, so no claim can admit a holder.
        raise ArchiveClaimError(
            Message(
                "Cannot run {operation} for store {product} as holder {holder_run_id}: the "
                "store is not a product, so no exclusive maintenance claim admits the holder.",
                operation="archive_create",
                product=product.label,
                holder_run_id=holder_run_id,
            ),
            product=product.label,
            operation="archive_create",
        )
    if is_product:
        manager = product.session.control_plane()
        try:
            _admit_maintenance_claim(
                manager=manager,
                product=product.label,
                operation="archive_create",
                holder_run_id=holder_run_id,
            )
        finally:
            manager.close()
    try:
        _admit_create_paths(
            product,
            archive_path=archive_path,
            archive_format=archive_format,
            overwrite=overwrite,
            keys=keys,
        )
    except (FileExistsError, ValueError, ImportError) as exc:
        # The run wraps the writer's refusals the same way.
        raise ArchiveConversionError(_error_text(exc), archive=archive) from exc
    return ArchiveCreatePlan(
        archive_format=archive_format,
        source=source_uri.to_str(),
        archive_path=archive_path,
        is_product=is_product,
        group=group,
        variables=None if variables is None else tuple(variables),
        start_date=start_date,
        end_date=end_date,
        compression=compression,
        overwrite=overwrite,
    )


@overload
def create_archive(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool = ...,
    group: str | None = ...,
    variables: list[str] | None = ...,
    start_date: str | None = ...,
    end_date: str | None = ...,
    compression: str = ...,
    allow_nan: bool = ...,
    allow_inf: bool = ...,
    warn: Callable[[str], None] = ...,
    holder_run_id: str | None = ...,
    keys: Iterable[str] | None = ...,
    marker: Mapping[str, Any] | None = ...,
    dry_run: Literal[False] = ...,
) -> ArchiveCreateResult: ...


@overload
def create_archive(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool = ...,
    group: str | None = ...,
    variables: list[str] | None = ...,
    start_date: str | None = ...,
    end_date: str | None = ...,
    compression: str = ...,
    allow_nan: bool = ...,
    allow_inf: bool = ...,
    warn: Callable[[str], None] = ...,
    holder_run_id: str | None = ...,
    keys: Iterable[str] | None = ...,
    marker: Mapping[str, Any] | None = ...,
    dry_run: Literal[True],
) -> ArchiveCreatePlan: ...


@overload
def create_archive(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool = ...,
    group: str | None = ...,
    variables: list[str] | None = ...,
    start_date: str | None = ...,
    end_date: str | None = ...,
    compression: str = ...,
    allow_nan: bool = ...,
    allow_inf: bool = ...,
    warn: Callable[[str], None] = ...,
    holder_run_id: str | None = ...,
    keys: Iterable[str] | None = ...,
    marker: Mapping[str, Any] | None = ...,
    dry_run: bool,
) -> ArchiveCreateResult | ArchiveCreatePlan: ...


def create_archive(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool = False,
    group: str | None = None,
    variables: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    compression: str = "zstd",
    allow_nan: bool = True,
    allow_inf: bool = True,
    warn: Callable[[str], None] = _ignore_line,
    holder_run_id: str | None = None,
    keys: Iterable[str] | None = None,
    marker: Mapping[str, Any] | None = None,
    dry_run: bool = False,
) -> ArchiveCreateResult | ArchiveCreatePlan:
    """Write the product's store to a local .zip or .tgm archive.

    The call archives a product, a store with a control plane, under the product's
    maintenance claim. It records the run after it writes the archive, so the archive
    never holds the run. It only reads a plain Zarr store: no claim, no record.
    The call releases the claim and closes the control plane before it returns or raises.
    The dry run and the run make the same refusals before the claim.

    Args:
        product: The open source store, from `open_product`.
        archive: The archive reference the run records.
        archive_path: The absolute local path the archive is written to.
        archive_format: `"zip"` or `"tensogram"`.
        overwrite: Replace an existing archive at `archive_path`.
        group: The one product group to archive, tensogram only.
        variables: The variable names to archive, tensogram only.
        start_date: The ISO 8601 start of the time range, tensogram only.
        end_date: The ISO 8601 end of the time range, tensogram only.
        compression: The tensogram codec. A zip archive ignores it.
        allow_nan: Allow NaN values in a tensogram archive.
        allow_inf: Allow Inf values in a tensogram archive.
        warn: Receives each bookkeeping warning line, "Warning: " included.
        holder_run_id: The run that holds the product's exclusive maintenance claim. Pass
            it to archive under that claim; the run record names it. A plain store refuses it.
        keys: The product keys a zip archive holds; `None` archives every key.
        marker: The JSON object a zip archive holds at its reserved marker entry.
        dry_run: Make the refusals from reads only and return the plan; take no claim
            and write nothing.

    Returns:
        The written archive, its size, the recorded run id and the tensogram summary;
        with `dry_run`, the plan.

    Raises:
        ValueError: `archive_format` is unknown, or a tensogram archive gets `keys` or
            `marker`; the call reads and writes nothing.
        ConfigurationError: A zip archive gets a group, a variable list or a date; the
            call reads and writes nothing.
        ArchiveClaimError: The product's claims refuse the archive, another owner holds
            its exclusive maintenance claim, or `holder_run_id` does not hold it or names a
            plain store; the call records no run. The dry run refuses held claims too.
        ArchiveConversionError: The archive path or the source fails the writer's first
            checks, before the claim or under it before the writer; the call records no run.
            Or the conversion raises `FileExistsError`, `ValueError`, `ImportError` or
            `CalendarArchiveError`, or the zip writer raises `RuntimeError` or `OSError`;
            for a product, the call first tries to record the run as failed.
        ArchiveRecordError: The call writes the archive, then fails to record the run.
    """
    plan = _preflight_create(
        product,
        archive=archive,
        archive_path=archive_path,
        archive_format=archive_format,
        overwrite=overwrite,
        group=group,
        variables=variables,
        start_date=start_date,
        end_date=end_date,
        compression=compression,
        holder_run_id=holder_run_id,
        keys=keys,
        marker=marker,
    )
    if dry_run:
        return plan
    # Only a caller that names a subset or a marker passes them on.
    zip_selection: dict[str, Any] = {}
    if keys is not None:
        zip_selection["keys"] = list(keys)
    if marker is not None:
        zip_selection["marker"] = marker
    # Call-time imports, here and in the zip branch: callers and tests replace the
    # converters on their modules.
    from firecube.core.archive.tensogram.converter import CalendarArchiveError, zarr_to_tgm

    run_id = f"archive-create-{uuid.uuid4()}"
    session = product.session
    product_name = product.label
    source_uri = product.identity.product_uri
    scope_meta: dict[str, Any] = {
        "run_kind": "archive_create",
        "source_product": source_uri.to_str(),
        "target_archive": archive,
        "archive_format": archive_format,
        "group": group,
        "start_date": start_date,
        "end_date": end_date,
        "variables": variables,
    }
    if holder_run_id is not None:
        scope_meta["holder_run_id"] = holder_run_id
    # A plain Zarr store stays a pure read: no claim, no record, nothing written to it.
    # The call builds and closes its own manager: a handle built outside `open_product`
    # has no block that would close one.
    manager = session.control_plane() if plan.is_product else None
    claim_handle: Any = None
    tgm_result: dict[str, Any] | None = None
    try:
        if manager is not None:
            _claim_domain, claim_handle = _acquire_maintenance_claim(
                manager=manager,
                product=product_name,
                operation="archive_create",
                owner_id=run_id,
                warn=warn,
                holder_run_id=holder_run_id,
            )
        # An archive file written since the preflight refuses here, before the writer and
        # without a run record. The writer's no-clobber publication still guards the race.
        try:
            _refuse_existing_archive(
                archive_path, archive_format=archive_format, overwrite=overwrite
            )
        except FileExistsError as exc:
            raise ArchiveConversionError(_error_text(exc), archive=archive) from exc
        scope_meta["archive_started_at"] = datetime.now(UTC).isoformat()
        # The zip writer's own verification (RuntimeError) and a filesystem that cannot
        # publish the file (OSError from the no-clobber hard link) are conversion failures.
        zip_writer_errors: tuple[type[Exception], ...] = (
            (RuntimeError, OSError) if archive_format == "zip" else ()
        )
        try:
            if archive_format == "zip":
                from firecube.core.archive.zip.archive import zarr_to_zip

                zip_result = zarr_to_zip(
                    session,
                    archive_path,
                    overwrite=overwrite,
                    **zip_selection,
                )
                result_target = zip_result.target
                result_file_size_bytes = zip_result.file_size_bytes
            else:
                tgm_result = zarr_to_tgm(
                    source_uri.to_str(),
                    archive_path,
                    group=group,
                    variables=variables,
                    start_date=start_date,
                    end_date=end_date,
                    compression=compression,
                    overwrite=overwrite,
                    session=session,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                )
                result_target = tgm_result["target"]
                result_file_size_bytes = tgm_result["file_size_bytes"]
        except (
            FileExistsError,
            ValueError,
            ImportError,
            CalendarArchiveError,
            *zip_writer_errors,
        ) as exc:
            if manager is not None:
                _record_archive_create_failure(
                    manager=manager,
                    product=product_name,
                    run_id=run_id,
                    scope_meta=scope_meta,
                    error=str(exc),
                    warn=warn,
                )
            raise ArchiveConversionError(_error_text(exc), archive=archive) from exc
        except Exception as exc:
            if manager is not None:
                _record_archive_create_failure(
                    manager=manager,
                    product=product_name,
                    run_id=run_id,
                    scope_meta=scope_meta,
                    error=str(exc),
                    warn=warn,
                )
            raise

        scope_meta["file_size_bytes"] = result_file_size_bytes
        if manager is not None:
            try:
                _record_archive_create(
                    manager=manager, product=product_name, run_id=run_id, scope_meta=scope_meta
                )
            except Exception as exc:
                raise ArchiveRecordError(
                    f"Archive written to {result_target}, but recording it in the control plane "
                    f"of {source_uri.to_str()} failed: {exc}",
                    archive=result_target,
                    product_uri=source_uri.to_str(),
                ) from exc
    finally:
        if manager is not None:
            try:
                _release_claim(claim_handle, warn=warn)
            finally:
                manager.close()

    return ArchiveCreateResult(
        archive_format=archive_format,
        target=result_target,
        file_size_bytes=result_file_size_bytes,
        run_id=run_id if manager is not None else None,
        tgm_summary=tgm_result,
    )


def read_restore_source(
    *, archive: str, archive_path: str, archive_format: str
) -> ArchiveRestoreSource:
    """Check a local archive for `restore_archive` and read the product label it records.

    Reads only. The call checks a zip archive's marker, and an incremental backup's whole
    backup chain. A zip archive's `.firecube/product.json` entry names its label, else
    its agreeing run records do. A backup chain takes its full backup's label. A .tgm
    archive's control-plane message names its label.

    Args:
        archive: The archive reference the errors name.
        archive_path: The absolute local path of the archive file.
        archive_format: `"zip"` or `"tensogram"`.

    Returns:
        The checked archive and its label, `None` when it records none.

    Raises:
        ValueError: `archive_format` is unknown.
        BackupChainError: A zip marker is unreadable or malformed, or the backup chain of
            an incremental backup is broken or holds a backup with another label.
        ArchiveConversionError: The label stamp or the control-plane message is malformed,
            or the tensogram extras are missing.
        ProductLabelConflictError: A zip archive without a label stamp holds run records
            that name several labels.
    """
    if archive_format not in _ARCHIVE_FORMATS:
        raise ValueError(f"archive_format must be 'zip' or 'tensogram', not {archive_format!r}")
    # Call-time imports: callers and tests replace these functions on their modules.
    from firecube.core.archive.tensogram.restore import tgm_product_label
    from firecube.core.archive.zip.archive import zip_label_stamp, zip_product_label
    from firecube.core.archive.zip.backup_chain import (
        admit_marker,
        backup_chain_label,
        backup_chain_label_stamp,
        resolve_backup_chain,
    )
    from firecube.core.archive.zip.marker import MODE_INCREMENTAL

    # A present marker is checked, and an incremental backup with its whole backup chain,
    # so a refusal comes before the restore writes anything. An invalid marker never reads
    # as a full backup.
    marker = admit_marker(archive_path) if archive_format == "zip" else None
    backup_chain = None
    stamp = None
    try:
        if archive_format == "tensogram":
            label = tgm_product_label(archive_path)
        elif marker is not None and marker["mode"] == MODE_INCREMENTAL:
            backup_chain = tuple(resolve_backup_chain(archive_path))
            label = backup_chain_label(backup_chain)
            stamp = backup_chain_label_stamp(backup_chain)
        else:
            label = zip_product_label(archive_path)
            stamp = zip_label_stamp(archive_path)
    except (ImportError, ValueError) as exc:
        raise ArchiveConversionError(_error_text(exc), archive=archive) from exc
    return ArchiveRestoreSource(
        archive_path=archive_path,
        archive_format=archive_format,
        product_label=label,
        label_stamp=stamp,
        backup_chain=backup_chain,
    )


def restore_label(source: ArchiveRestoreSource, product_name: str | None, *, archive: str) -> str:
    """Return the label a restore of `source` takes: the archive's, else `product_name`.

    Args:
        source: The archive as `read_restore_source` read it.
        product_name: The label the caller gives, or `None`.
        archive: The archive reference the errors name.

    Raises:
        ConfigurationError: `product_name` is empty, or both it and the archive's label are absent.
        ProductLabelMismatchError: `product_name` differs from the label the archive records.
    """
    if product_name == "":
        raise ConfigurationError(Message("{name} must not be empty.", name=Param("product_name")))
    recorded = source.product_label
    if recorded is None:
        if product_name is None:
            raise ConfigurationError(
                Message(
                    "The archive {archive} records no product label. "
                    "Name the restored product with {name}.",
                    archive=archive,
                    name=Param("product_name", Placeholder("<label>")),
                )
            )
        return product_name
    if product_name is not None and product_name != recorded:
        raise ProductLabelMismatchError(
            Message(
                "Product label {given!r} is refused: the archive {archive} records the "
                "label {product!r}.",
                given=product_name,
                archive=archive,
                product=recorded,
            ),
            given=product_name,
            recorded=recorded,
        )
    return recorded


def _preflight_restore(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool,
    source: ArchiveRestoreSource | None,
) -> ArchiveRestorePlan:
    """Make each refusal of `restore_archive` that reads can make, and return its plan.

    Reads only: the call takes no claim and writes nothing. In the run's order, it checks
    the labels, the target's key paths, the held claims, the archive, then the target store.

    Raises:
        ValueError: `archive_format` is unknown, or `source` was read from another path or format.
        ProductLabelMismatchError: The archive or the target records another label than `product`.
        ProductLabelConflictError: The archive or an unstamped target names several labels.
        BackupChainError: A marker or backup chain is refused, or a target key path holds a symlink.
        ArchiveClaimError: The product's held claims refuse the claim.
        FileNotFoundError: The archive file does not exist.
        ArchiveConversionError: `read_restore_source` or the restore's first checks refuse the
            archive, or the target holds a store and `overwrite` is not set.
    """
    if archive_format not in _ARCHIVE_FORMATS:
        raise ValueError(f"archive_format must be 'zip' or 'tensogram', not {archive_format!r}")
    if source is not None and (source.archive_path, source.archive_format) != (
        archive_path,
        archive_format,
    ):
        raise ValueError(
            f"source was read from {source.archive_path!r} as {source.archive_format!r}, "
            f"not from {archive_path!r} as {archive_format!r}"
        )
    # Call-time imports: callers and tests replace these functions on their modules.
    from firecube.core.archive._common import refuse_other_product_label
    from firecube.core.archive.zip.backup_chain import refuse_symlinked_target

    if source is None:
        source = read_restore_source(
            archive=archive, archive_path=archive_path, archive_format=archive_format
        )
    label = restore_label(source, product.label, archive=archive)
    refuse_other_product_label(product.session, label)
    if source.backup_chain is not None:
        refuse_symlinked_target(source.backup_chain, product.session)
    manager = product.session.control_plane()
    try:
        _admit_maintenance_claim(manager=manager, product=label, operation="archive_restore")
    finally:
        manager.close()
    target = product.identity.product_uri.to_str()
    try:
        _admit_restore_archive(source)
        _refuse_existing_target(target, archive_format=archive_format, overwrite=overwrite)
    except (FileExistsError, ImportError, ValueError) as exc:
        # The run wraps the restore's refusals the same way.
        raise ArchiveConversionError(_error_text(exc), archive=archive) from exc
    return ArchiveRestorePlan(source=source, target=target, label=label, overwrite=overwrite)


def _admit_restore_archive(source: ArchiveRestoreSource) -> None:
    """Refuse the archive as the restore does before it touches the target; read only.

    Mirrors the first checks of `zip_to_zarr` and `tgm_to_zarr`, and the empty-archive
    refusal of `restore_archive`. The zip restore's CRC pass over every entry is not
    mirrored. `read_restore_source` already read each backup of a backup chain whole.

    Raises:
        FileNotFoundError: The archive file does not exist.
        ValueError: The zip archive has an unsafe entry name, no root `zarr.json` or invalid
            Zarr metadata; or the .tgm archive is empty, has another archive version, or
            fails the quick structural check.
        OptionalExtraMissingError: A .tgm archive lacks the tensogram extras.
    """
    if source.backup_chain is not None:
        return
    archive_path = source.archive_path
    local_path = local_path_from_target(archive_path)
    if not local_path.is_file():
        raise FileNotFoundError(f"Archive not found: {archive_path}")
    if source.archive_format == "zip":
        import zipfile

        from firecube.core.archive.zip.archive import (
            _CLAIMS_PREFIX,
            _refuse_invalid_metadata,
            _refuse_unsafe_entries,
        )
        from firecube.core.archive.zip.marker import MARKER_ENTRY

        with zipfile.ZipFile(local_path) as zf:
            names = zf.namelist()
            _refuse_unsafe_entries(names, archive_path)
            keys = sorted(
                key
                for key in names
                if not key.endswith("/")
                and not key.startswith(_CLAIMS_PREFIX)
                and key != MARKER_ENTRY
            )
            # A marker that says incremental takes the backup chain branch above.
            if "zarr.json" not in keys:
                raise ValueError(f"No Zarr v3 root metadata found in archive: {archive_path}")
            _refuse_invalid_metadata(zf, keys, archive_path)
        return
    from firecube.core.archive.tensogram._compat import require_tensogram
    from firecube.core.archive.tensogram.restore import _refuse_structural_issues
    from firecube.core.archive.tensogram.schema import ARCHIVE_VERSION, KEY_ARCHIVE_VERSION

    require_tensogram("tgm_to_zarr")
    import tensogram as _tensogram  # pyright: ignore[reportMissingImports]

    tensogram = cast(Any, _tensogram)
    with tensogram.TensogramFile.open(str(local_path)) as tensogram_file:
        if tensogram_file.message_count() == 0:
            raise ValueError(
                f"Archive restore produced no output: {archive_path!r} "
                "may be empty or not a valid tensogram archive."
            )
        first_meta = tensogram_file.file_decode_metadata(0)
        first_extra = (first_meta.extra or {}) if hasattr(first_meta, "extra") else {}
        archive_version = first_extra.get("firecube", {}).get(KEY_ARCHIVE_VERSION)
        if archive_version != ARCHIVE_VERSION:
            raise ValueError(
                f"Unsupported Firecube archive version: {archive_version!r}. "
                f"Expected {ARCHIVE_VERSION!r}."
            )
        _refuse_structural_issues(tensogram, str(local_path))


def _refuse_existing_target(target: str, *, archive_format: str, overwrite: bool) -> None:
    """Refuse an existing Zarr store at `target` without `overwrite`; read only.

    Calls the restore's own target check with `overwrite=False`, which removes nothing.
    A zip restore counts a root `zarr.json`; a .tgm restore counts any entry but `.firecube`.

    Raises:
        FileExistsError: `target` holds a store and `overwrite` is not set.
    """
    if overwrite:
        return
    if archive_format == "zip":
        from firecube.core.archive.zip.archive import _prepare_target as prepare_zip_target

        prepare_zip_target(local_path_from_target(target), overwrite=False, keep_run_id=None)
    else:
        from firecube.core.archive.tensogram.restore import _prepare_target as prepare_tgm_target

        prepare_tgm_target(target, overwrite=False, keep_run_id=None)


@overload
def restore_archive(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool = ...,
    report: Callable[[str], None] = ...,
    warn: Callable[[str], None] = ...,
    source: ArchiveRestoreSource | None = ...,
    dry_run: Literal[False] = ...,
) -> ArchiveRestoreResult: ...


@overload
def restore_archive(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool = ...,
    report: Callable[[str], None] = ...,
    warn: Callable[[str], None] = ...,
    source: ArchiveRestoreSource | None = ...,
    dry_run: Literal[True],
) -> ArchiveRestorePlan: ...


@overload
def restore_archive(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool = ...,
    report: Callable[[str], None] = ...,
    warn: Callable[[str], None] = ...,
    source: ArchiveRestoreSource | None = ...,
    dry_run: bool,
) -> ArchiveRestoreResult | ArchiveRestorePlan: ...


def restore_archive(
    product: Product,
    *,
    archive: str,
    archive_path: str,
    archive_format: str,
    overwrite: bool = False,
    report: Callable[[str], None] = _ignore_line,
    warn: Callable[[str], None] = _ignore_line,
    source: ArchiveRestoreSource | None = None,
    dry_run: bool = False,
) -> ArchiveRestoreResult | ArchiveRestorePlan:
    """Restore a local .zip or .tgm archive into the product.

    The restore holds the product's maintenance claim and records its run as started
    before it touches the target. On failure it tries to record the run as failed, warns
    through `warn` if that fails, and raises the original error either way. An
    interrupt, `KeyboardInterrupt` or `SystemExit`, records no failure: a started run
    stays started. The call always releases the claim and closes the control plane.

    A zip archive whose marker says incremental is restored with its backup chain: the
    full backup first, then each incremental backup in order. The whole backup chain is
    checked before the claim and the run, so a refused backup chain writes nothing to
    the target.

    A zip archive's marker entry, when present, is checked before the claim and the run.
    A marker whose mode is full, and an archive without a marker, take the zip restore.

    The product's label must equal the label the archive records, as
    `read_restore_source` reads it; an archive that records none takes the product's.
    The call refuses an archive or a target with another label before the claim, and
    refuses the target's label again under it. No write replaces the target's label stamp.
    An unstamped target takes the zip archive's stamp before the claim.
    The dry run makes the same refusals before the claim and writes nothing.

    Args:
        product: The open target product, from `open_product`, under the label to restore.
        archive: The archive reference the run records.
        archive_path: The absolute local path of the archive file.
        archive_format: `"zip"` or `"tensogram"`.
        overwrite: Replace an existing store at the product's URI.
        report: Receives `"restored group: <group>"` after each tensogram group.
        warn: Receives each bookkeeping warning line, "Warning: " included.
        source: The archive as `read_restore_source` read it from `archive_path`; the
            call reads it itself when `None`.
        dry_run: Make the refusals from reads only and return the plan; take no claim
            and write nothing.

    Returns:
        The recorded run id and the restore's summary; with `dry_run`, the plan.

    Raises:
        ValueError: `archive_format` is unknown, or `source` was read from another path
            or format; the call reads and writes nothing.
        ProductLabelMismatchError: The archive or the target records another label than
            the product; the call refuses it before the claim, or under it before the run,
            and records no run.
        ProductLabelConflictError: The archive or an unstamped target holds run records
            that name several labels; the call refuses it before the claim.
        ArchiveClaimError: The product's held claims refuse the restore, before the claim
            and with no run record. Or the acquisition conflicts, or another owner holds the
            exclusive maintenance claim; the call tries to record the run as started and
            failed, once each.
        ArchiveConversionError: `read_restore_source` or the restore's first checks refuse
            the archive, or the target holds a store without `overwrite`; before the claim,
            or under it before the run, with no run record. Or the restore raises
            `FileExistsError`, `ImportError` or `ValueError`, or produces no output, and the
            call first tries to record the run as failed.
        FileNotFoundError: The archive file does not exist; the call records no run.
        BackupChainError: A zip marker is unreadable or malformed, the backup chain of an
            incremental backup is broken or holds a backup with another label, or a local
            target holds a symlink on a key's path; the call refuses it before the claim
            and records no run. A backup chain check that fails inside the run raises it
            after the call records the run failed.
    """
    plan = _preflight_restore(
        product,
        archive=archive,
        archive_path=archive_path,
        archive_format=archive_format,
        overwrite=overwrite,
        source=source,
    )
    if dry_run:
        return plan
    is_zip = archive_format == "zip"
    # Call-time imports: callers and tests replace these converters on their modules.
    from firecube.core.archive._common import refuse_other_product_label, seed_product_label
    from firecube.core.archive.tensogram.restore import tgm_to_zarr
    from firecube.core.archive.zip.archive import zip_to_zarr
    from firecube.core.archive.zip.backup_chain import restore_backup_chain

    session = product.session
    source = plan.source
    product_name = plan.label
    backup_chain = source.backup_chain
    if source.product_label is not None and source.label_stamp is not None:
        # The claim below stamps an unstamped target; the archive's stamp goes first, so
        # the restored product keeps the stamp it was archived with.
        seed_product_label(session, source.product_label, source.label_stamp)

    # Archive restore is recorded as a first-class WAL run, so the audit trail
    # shows when a product was restored from an archive, what the source archive
    # was, and whether the restore succeeded or failed. One manager and recorder
    # serve the whole restore: an overwrite keeps the claim and this run's records
    # (`keep_run_id`), so the cached WAL writer stays valid.
    restore_run_id = f"archive-restore-{uuid.uuid4()}"
    target_uri_str = plan.target
    slice_meta: dict[str, Any] = {
        "run_kind": "archive_restore",
        "source_archive": archive,
        "target_product": target_uri_str,
    }
    manager = session.control_plane()
    recorder = SpanRecorder(manager)
    run_started = False
    start_refused = False

    def _record_started() -> None:
        nonlocal run_started, start_refused
        try:
            recorder.register_run_started(
                run_id=restore_run_id,
                product=product_name,
                output_path=target_uri_str,
                output_format="zarr",
                slice_meta=slice_meta,
            )
        except ProductUnderMaintenanceError:
            # The start is recorded and the refusal recorded it failed: never start it again.
            start_refused = True
            raise
        run_started = True
        manager.record_maintenance_started(
            product=product_name,
            run_id=restore_run_id,
            op=MAINTENANCE_OP_ARCHIVE_RESTORE,
            scope_meta=slice_meta,
        )

    def _record_failure(error: str) -> None:
        # Warn, never raise: a raise here replaces the restore error.
        if start_refused:
            # The exclusive-maintenance refusal already recorded the run failed.
            return
        try:
            if not run_started:
                try:
                    _record_started()
                except ProductUnderMaintenanceError:
                    return
            recorder.register_run_failure(
                run_id=restore_run_id,
                product=product_name,
                output_path=target_uri_str,
                output_format="zarr",
                slice_meta=slice_meta,
                error=error,
            )
            manager.record_maintenance_failed(
                product=product_name,
                run_id=restore_run_id,
                op=MAINTENANCE_OP_ARCHIVE_RESTORE,
                scope_meta=slice_meta,
                error=error,
            )
        except Exception as exc:
            warn(
                Message(
                    "Warning: archive restore failure not recorded: {error}",
                    error=_error_text(exc),
                )
            )

    restore_source = archive_path
    claim_handle: Any = None
    try:
        try:
            _claim_domain, claim_handle = _acquire_maintenance_claim(
                manager=manager,
                product=product_name,
                operation="archive_restore",
                owner_id=restore_run_id,
                warn=warn,
            )
        except Exception as exc:
            _record_failure(str(exc))
            raise
        # A stamp or a store written since the preflight refuses here, under the claim and
        # before the run. The converters check the target again before they write.
        refuse_other_product_label(session, product_name)
        try:
            _refuse_existing_target(
                target_uri_str, archive_format=archive_format, overwrite=overwrite
            )
        except FileExistsError as exc:
            raise ArchiveConversionError(_error_text(exc), archive=archive) from exc
        # The run is recorded before the restore touches the target; if that fails, nothing runs.
        try:
            _record_started()
        except ProductUnderMaintenanceError as exc:
            # The refusal already recorded the run failed; it surfaces as the archive's refusal.
            raise ArchiveClaimError(
                exc.message or str(exc), product=product_name, operation="archive_restore"
            ) from exc
        except Exception as exc:
            _record_failure(str(exc))
            raise

        try:
            if backup_chain is not None:
                result = restore_backup_chain(
                    backup_chain,
                    target_uri_str,
                    session=session,
                    overwrite=overwrite,
                    keep_run_id=restore_run_id,
                )
            elif is_zip:
                result = zip_to_zarr(
                    restore_source,
                    target_uri_str,
                    session=session,
                    overwrite=overwrite,
                    keep_run_id=restore_run_id,
                )
            else:
                result = tgm_to_zarr(
                    restore_source,
                    target_uri_str,
                    overwrite=overwrite,
                    session=session,
                    on_group_restored=lambda restored_group: report(
                        f"restored group: {restored_group or '/'}"
                    ),
                    keep_run_id=restore_run_id,
                )
            if not result.get("variables") and not result.get("groups"):
                raise ValueError(
                    f"Archive restore produced no output: {restore_source!r} "
                    f"may be empty or not a valid {'zip' if is_zip else 'tensogram'} archive."
                )
        except (FileExistsError, ImportError, ValueError) as exc:
            _record_failure(str(exc))
            raise ArchiveConversionError(_error_text(exc), archive=archive) from exc
        except Exception as exc:
            _record_failure(str(exc))
            raise

        try:
            synthetic_result = IngestResult(
                outputs=OutputPaths(primary=target_uri_str),
                output_format="zarr",
                metrics={},
            )
            recorder.register_run(
                result=synthetic_result,
                run_id=restore_run_id,
                product=product_name,
                slice_meta=slice_meta,
                force_reingest=False,
            )
            manager.record_maintenance_completed(
                product=product_name,
                run_id=restore_run_id,
                op=MAINTENANCE_OP_ARCHIVE_RESTORE,
                scope_meta=slice_meta,
            )
        except Exception as exc:
            _record_failure(str(exc))
            raise
    finally:
        _release_claim(claim_handle, warn=warn)
        try:
            manager.close()
        except Exception as exc:
            warn(
                Message(
                    "Warning: control plane of {product_uri} not closed: {error}",
                    product_uri=target_uri_str,
                    error=_error_text(exc),
                )
            )

    return ArchiveRestoreResult(run_id=restore_run_id, summary=result)
