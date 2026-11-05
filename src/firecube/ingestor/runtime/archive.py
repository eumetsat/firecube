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
Both hold the product's maintenance claim while they write.
Bookkeeping warnings go to the caller's `warn` callable.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from firecube.core.controlplane.types import (
    CONTROL_DIRNAME,
    MAINTENANCE_OP_ARCHIVE_CREATE,
    MAINTENANCE_OP_ARCHIVE_RESTORE,
    SCHEMA_FILENAME,
    WriteDomain,
)
from firecube.core.errors import (
    ClaimConflictError,
    FirecubeError,
    Message,
    ProductUnderMaintenanceError,
)
from firecube.ingestor.runtime.recording import SpanRecorder
from firecube.ingestor.types.context import (
    IngestResult,
    RuntimeFlags,
    RuntimeIdentity,
    RuntimeIngestContext,
)
from firecube.ingestor.types.result_metrics import OutputPaths

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession

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
class ArchiveRestoreResult:
    """What `restore_archive` restored.

    Attributes:
        run_id: The run recorded in the target product.
        summary: The restore's summary: `target`, `groups`, `variables`, `coordinates`
            and `controlplane_restored`.
    """

    run_id: str
    summary: Mapping[str, Any]


def _ignore_line(line: str) -> None:
    """Discard a progress or warning line."""


def _error_text(exc: BaseException) -> str:
    """Return the `Message` an exception was raised with, else its text."""
    first = exc.args[0] if exc.args else None
    return first if isinstance(first, Message) else str(exc)


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

    With `holder_run_id`, the product's exclusive maintenance claim must exist and be owned
    by it, and is the one claim the listing lets through. A failed release of the acquired
    claim goes to `warn`.

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
    # Release stops the heartbeat; deleting the file alone lets it come back.
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


def create_archive(
    *,
    session: StorageSession,
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
) -> ArchiveCreateResult:
    """Write the session's store to a local .zip or .tgm archive.

    The call archives a product, a store with a control plane, under the product's
    maintenance claim. It records the run after it writes the archive, so the archive
    never holds the run. It only reads a plain Zarr store: no claim, no record.
    The call releases the claim and closes the control plane before it returns or raises.

    Args:
        session: The storage session bound to the source store.
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

    Returns:
        The written archive, its size, the recorded run id and the tensogram summary.

    Raises:
        ValueError: `archive_format` is unknown, a zip archive gets a group, a
            variable list or a date, or a tensogram archive gets `keys` or `marker`;
            the call reads and writes nothing.
        ArchiveClaimError: The product's claims refuse the archive, another owner holds
            its exclusive maintenance claim, or `holder_run_id` does not hold it or names a
            plain store; the call records no run.
        ArchiveConversionError: The conversion raises `FileExistsError`, `ValueError`,
            `ImportError` or `CalendarArchiveError`; for a product, the call first tries to
            record the run as failed.
        ArchiveRecordError: The call writes the archive, then fails to record the run.
    """
    if archive_format not in _ARCHIVE_FORMATS:
        raise ValueError(f"archive_format must be 'zip' or 'tensogram', not {archive_format!r}")
    if archive_format == "zip" and (group or start_date or end_date or variables):
        raise ValueError("Zip archive does not support group, start_date, end_date, or variables")
    if archive_format == "tensogram" and (keys is not None or marker is not None):
        raise ValueError("Tensogram archive does not support keys or marker")
    # Only a caller that names a subset or a marker passes them on.
    zip_selection: dict[str, Any] = {}
    if keys is not None:
        zip_selection["keys"] = list(keys)
    if marker is not None:
        zip_selection["marker"] = marker
    # Call-time imports, here and in the zip branch: callers and tests replace the
    # converters on their modules.
    from firecube.core.tensogram.converter import CalendarArchiveError, zarr_to_tgm

    run_id = f"archive-create-{uuid.uuid4()}"
    product_name = session.product.product_name
    source_uri = session.product.product_uri
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
    manager = (
        session.control_plane()
        if session.exists(source_uri.join(CONTROL_DIRNAME, SCHEMA_FILENAME))
        else None
    )
    if manager is None and holder_run_id is not None:
        # A store without a control plane is never admitted, so no claim can admit a holder.
        raise ArchiveClaimError(
            Message(
                "Cannot run {operation} for store {product} as holder {holder_run_id}: the "
                "store is not a product, so no exclusive maintenance claim admits the holder.",
                operation="archive_create",
                product=product_name,
                holder_run_id=holder_run_id,
            ),
            product=product_name,
            operation="archive_create",
        )
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
        scope_meta["archive_started_at"] = datetime.now(UTC).isoformat()
        try:
            if archive_format == "zip":
                from firecube.core.zarr.zip_archive import zarr_to_zip

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
        except (FileExistsError, ValueError, ImportError, CalendarArchiveError) as exc:
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


def restore_archive(
    *,
    session: StorageSession,
    archive: str,
    archive_path: str,
    archive_format: str,
    target: str,
    overwrite: bool = False,
    report: Callable[[str], None] = _ignore_line,
    warn: Callable[[str], None] = _ignore_line,
) -> ArchiveRestoreResult:
    """Restore a local .zip or .tgm archive into the session's product.

    The restore holds the product's maintenance claim and records its run as started
    before it touches the target. On failure it tries to record the run as failed, warns
    through `warn` if that fails, and raises the original error either way. An
    interrupt, `KeyboardInterrupt` or `SystemExit`, records no failure: a started run
    stays started. The call always releases the claim and closes the control plane.

    Args:
        session: The storage session bound to the target product.
        archive: The archive reference the run records.
        archive_path: The absolute local path of the archive file.
        archive_format: `"zip"` or `"tensogram"`.
        target: The product URI the restore writes and the run records.
        overwrite: Replace an existing store at `target`.
        report: Receives `"restored group: <group>"` after each tensogram group.
        warn: Receives each bookkeeping warning line, "Warning: " included.

    Returns:
        The recorded run id and the restore's summary.

    Raises:
        ValueError: `archive_format` is unknown; the call reads and writes nothing.
        ArchiveClaimError: The product's claims refuse the restore, or another owner holds
            its exclusive maintenance claim; the call tries to record the run as started and
            failed, once each.
        ArchiveConversionError: The restore raises `FileExistsError`, `ImportError` or
            `ValueError`, or produces no output; the call first tries to record the run as failed.
    """
    if archive_format not in _ARCHIVE_FORMATS:
        raise ValueError(f"archive_format must be 'zip' or 'tensogram', not {archive_format!r}")
    is_zip = archive_format == "zip"
    # Call-time imports: callers and tests replace these converters on their modules.
    from firecube.core.tensogram.restore import tgm_to_zarr
    from firecube.core.zarr.zip_archive import zip_to_zarr

    # Archive restore is recorded as a first-class WAL run, so the audit trail
    # shows when a product was restored from an archive, what the source archive
    # was, and whether the restore succeeded or failed. One manager and recorder
    # serve the whole restore: an overwrite keeps the claim and this run's records
    # (`keep_run_id`), so the cached WAL writer stays valid.
    restore_run_id = f"archive-restore-{uuid.uuid4()}"
    product_name = session.product.product_name
    target_uri_str = target
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
            if is_zip:
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
            synthetic_ctx = RuntimeIngestContext(
                source=archive,
                target=target_uri_str,
                output_format="zarr",
                run_id=restore_run_id,
                identity=RuntimeIdentity(run_id=restore_run_id),
                flags=RuntimeFlags(force_reingest=False),
            )
            synthetic_result = IngestResult(
                outputs=OutputPaths(primary=target_uri_str),
                output_format="zarr",
                metrics={},
            )
            recorder.register_run(
                ctx=synthetic_ctx,
                result=synthetic_result,
                run_id=restore_run_id,
                product=product_name,
                slice_meta=slice_meta,
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
