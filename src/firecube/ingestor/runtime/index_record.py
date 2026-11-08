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

"""Rebuild and verify a product's resolved-index record.

`rebuild_index_record` writes the record a plugin declares. `verify_index_record`
compares the stored record with the root attribute and, optionally, the plugin.
Rebuild passes its messages to the caller's callables; the caller renders the verify verdict.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from firecube.core import observability
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.manager import check_legacy_index_record
from firecube.core.controlplane.types import ResolvedIndexRecord, WriteDomain
from firecube.core.errors import (
    LegacyIndexRecordError,
    ManifestError,
    Message,
    ResolvedIndexAttrsConflictError,
    ResolvedIndexClaimTimeoutError,
    ResolvedIndexConflictError,
)
from firecube.core.index_resolve import ExtentUnknownError, ResolvedIndex, resolve_index_spec
from firecube.core.observability.metrics import TelemetryService, emit_index_ensured_full
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.types.context import IngestContext, PluginContext, RuntimeIngestContext

_log = logging.getLogger(__name__)


class IndexRecordError(ConfigurationError):
    """The plugin's index declaration or the stored index state refuses the operation."""


@dataclass(frozen=True)
class IndexRebuildResult:
    """What a rebuild persisted.

    Attributes:
        outcome: `"created"` when the rebuild wrote the record or its root attribute;
            `"matched_existing"` when the stored record already matched; `"dry-run"`
            when a preview wrote nothing.
        identity_hash: The identity hash of the persisted record, or of the declared
            record in a dry run.
        run_id: The run id recorded in the control plane, or `None` in a dry run.
    """

    outcome: Literal["created", "matched_existing", "dry-run"]
    identity_hash: str
    run_id: str | None


@dataclass(frozen=True)
class IndexVerification:
    """The verdict of a resolved-index verification.

    Attributes:
        outcome: One of:
            `"verified"`: the record is consistent; `attrs_hash` is `None` when the
            root attribute is not mirrored yet.
            `"unreadable-record"`: the stored record cannot be read; `detail` says why.
            `"legacy-record"`: only the legacy slot-index record exists; `detail` says where.
            `"attrs-without-record"`: the root attribute exists, the record does not.
            `"no-record"`: neither the record nor the root attribute exists.
            `"index-drift"`: the plugin declares another identity hash than the record.
            `"attrs-drift"`: the root attribute diverges from the record.
        identity_hash: The stored record's identity hash, or `None` without a record.
        attrs_hash: The identity hash mirrored on the root attributes, or `None`.
        declared_hash: The plugin's declared identity hash, or `None` when no plugin was compared.
        detail: The error text for `"unreadable-record"` and `"legacy-record"`, else `None`.
    """

    outcome: Literal[
        "verified",
        "unreadable-record",
        "legacy-record",
        "attrs-without-record",
        "no-record",
        "index-drift",
        "attrs-drift",
    ]
    identity_hash: str | None = None
    attrs_hash: str | None = None
    declared_hash: str | None = None
    detail: str | None = None


def _text_keeping_message(exc: BaseException) -> str:
    """Return the exception's `Message` when it carries one, else `str(exc)`, so the CLI can render it."""
    first = exc.args[0] if exc.args else None
    return first if isinstance(first, Message) else str(exc)


def _plugin_context(target: str, *, run_id: str) -> PluginContext:
    """Build the in-memory plugin context the index commands hand to `index_spec`."""
    ingest_ctx = IngestContext(
        source="",
        target=target,
        in_memory=True,
        output_format="zarr",
        options={},
        run_id=run_id,
    )
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        ingest_ctx,
        run_id=run_id,
        temp_root=None,
        materializer=None,
    )
    return PluginContext(runtime_ctx)


def _declared_index(
    ingestor: Any,
    *,
    plugin_name: str,
    target: str,
    run_id: str,
    action: str,
    configure: Callable[[Any, str], PluginContext] | None = None,
) -> ResolvedIndex:
    """Resolve the plugin's declared index.

    `configure` takes the plugin and the run id and returns the configured context
    that `index_spec` receives; without it the context carries no options. An
    exception from `configure` propagates unchanged.

    Raises:
        IndexRecordError: `index_spec` returns `None`, or it or the resolution raises;
            the original exception is the cause.
    """
    context = (
        configure(ingestor, run_id)
        if configure is not None
        else _plugin_context(target, run_id=run_id)
    )
    try:
        spec = ingestor.index_spec(context)
    except Exception as exc:
        raise IndexRecordError(
            f"Plugin '{plugin_name}' failed to resolve index_spec: {exc}"
        ) from exc
    if spec is None:
        raise IndexRecordError(
            f"Plugin '{plugin_name}' does not opt into parallel ingestion "
            f"(index_spec() returned None). Nothing to {action}."
        )
    try:
        return resolve_index_spec(
            spec,
            time_dim_name=str(getattr(ingestor, "time_dim_name", "timestamp")),
        )
    except Exception as exc:
        raise IndexRecordError(
            f"Plugin '{plugin_name}' failed to resolve index_spec: {exc}"
        ) from exc


def _preview_rebuild(
    chunk_manager: ChunkManager,
    product_name: str,
    record: ResolvedIndexRecord,
    *,
    report: Callable[[str], None],
    warn: Callable[[str], None],
) -> IndexRebuildResult:
    """Make the refusals `ensure_resolved_index` makes, from reads only; report what it would write.

    Raises:
        ProductUnderMaintenanceError: Another owner holds the exclusive maintenance claim.
        ResolvedIndexConflictError: The stored record differs; `warn` receives its message.
        ResolvedIndexAttrsConflictError: The root attribute disagrees with the stored record.
        ResolvedIndexClaimTimeoutError: No record exists and `resolved_index:current` is held.
    """
    chunk_manager.check_exclusive_maintenance(
        product=product_name, owner_id=record.recorded_by_run_id
    )
    stored = chunk_manager.get_resolved_index(product=product_name)
    attrs_hash = chunk_manager.read_resolved_index_attrs_hash(product=product_name)
    declared = record.identity_hash
    if stored is not None and stored.identity_hash != declared:
        message = (
            "plugin declares incompatible resolved index: "
            f"stored={stored.identity_hash[:16]} declared={declared[:16]}"
        )
        warn(message)
        raise ResolvedIndexConflictError(message)
    if stored is None and attrs_hash is not None:
        raise ResolvedIndexAttrsConflictError(
            "zarr root attrs have resolved-index identity hash but no control-plane record"
        )
    if stored is not None and attrs_hash not in (None, declared):
        raise ResolvedIndexAttrsConflictError(
            "zarr root attrs have drifted from authoritative resolved-index record: "
            f"cp_hash={stored.identity_hash[:16]} attrs_hash={str(attrs_hash)[:16]!r}"
        )
    if stored is None:
        # A held claim, even a stale one, refuses the run's acquire; the run then waits for
        # a record its holder never wrote, as the ingest dry run reports too.
        domain = WriteDomain(product=product_name, category="resolved_index", name="current")
        holder = chunk_manager.repo.read_claim_by_domain(
            product=product_name, domain=domain.identifier
        )
        if holder is not None:
            state = "stale" if holder.stale else "live"
            raise ResolvedIndexClaimTimeoutError(
                f"claim {holder.domain} is held by {holder.owner_id} ({state}) and the store "
                "has no resolved-index record, so the rebuild would not converge on one"
            )
        action = "write the index record and mirror it on the root attributes"
    elif attrs_hash is None:
        action = "mirror the stored index record on the root attributes"
    else:
        action = "keep the index record, which matches"
    report(
        f"Dry run: would {action} (identity_hash={declared[:16]}) and record one rebuild run; "
        "nothing is written"
    )
    return IndexRebuildResult(outcome="dry-run", identity_hash=declared, run_id=None)


def rebuild_index_record(
    *,
    ingestor: Any,
    plugin_name: str,
    chunk_manager: ChunkManager,
    target: str,
    product_name: str,
    logger: logging.Logger = _log,
    report: Callable[[str], None] = lambda _message: None,
    warn: Callable[[str], None] = lambda _message: None,
    configure: Callable[[Any, str], PluginContext] | None = None,
    dry_run: bool = False,
) -> IndexRebuildResult:
    """Persist the resolved-index record the plugin declares, and mirror it on the root attributes.

    Every refusal comes before the record is written. Once the control plane holds
    the run, the call tries to record it as complete or failed before returning or
    raising; a failure of that record is logged, not raised. The caller closes
    `chunk_manager`. With `dry_run`, the call makes the refusals from reads only,
    reports what it would write and writes nothing: no claim, record, attribute or run.

    Args:
        ingestor: The plugin instance whose `index_spec` declares the index.
        plugin_name: The plugin's registered name, used in messages and telemetry.
        chunk_manager: The control plane of the target product.
        target: The product URI handed to the plugin's context and recorded as the run's output.
        product_name: The logical product name.
        logger: Receives the index-ensured event log and run-record failures.
        report: Receives the outcome line before the run is recorded complete.
        warn: Receives the conflict message before the run is recorded failed.
        configure: Takes the plugin and the run id, returns the configured context for `index_spec`.

    Returns:
        The outcome, the persisted identity hash and the run id.

    Raises:
        IndexRecordError: The plugin declares no index or a failing one, the declared
            index has an unbounded group, or a legacy slot-index hash on the root
            attributes differs from the declared one.
        ResolvedIndexConflictError: The control plane already holds a different record;
            `warn` receives its message, then the run is recorded as failed.
        ResolvedIndexAttrsConflictError: The root attribute disagrees with the stored record.
        ProductUnderMaintenanceError: Another owner holds the exclusive maintenance claim.
    """
    run_id = f"rebuild-{uuid.uuid4()}"
    run_meta = {"run_id": run_id, "command": "index_rebuild"}
    run_created = False
    completed = False
    try:
        resolved = _declared_index(
            ingestor,
            plugin_name=plugin_name,
            target=target,
            run_id=run_id,
            action="rebuild",
            configure=configure,
        )
        legacy_model = resolved.as_legacy_slot_index_model()
        if legacy_model is not None:
            persisted_legacy_hash = chunk_manager.read_slot_index_attrs_hash(product=product_name)
            declared_legacy_hash = legacy_model.identity_hash
            if persisted_legacy_hash is not None and persisted_legacy_hash != declared_legacy_hash:
                raise IndexRecordError(
                    f"Refusing to rebuild: persisted legacy slot-index identity hash "
                    f"({persisted_legacy_hash[:16]}) differs from plugin's declared hash "
                    f"({declared_legacy_hash[:16]}). Rebuilding would silently re-anchor "
                    f"slot positions and disagree with data already on disk. To override: "
                    f"(1) remove the `firecube_slot_index_model_identity_hash` root attr from the Zarr store, "
                    f"AND (2) delete `.firecube/slot_index/current.json`. Both steps required."
                )
        # An unbounded group serializes as ``size: null`` rather than raising,
        # so ``as_resolved_index_record`` cannot signal the refusal; probe each
        # group's extent explicitly before persisting anything.
        unbounded_groups: list[str] = []
        for group in resolved.groups:
            try:
                resolved.size(group)
            except ExtentUnknownError:
                unbounded_groups.append(group)
        if unbounded_groups:
            groups = ", ".join(repr(group) for group in unbounded_groups)
            raise IndexRecordError(
                f"Plugin '{plugin_name}' failed to create resolved index record for "
                f"unbounded group(s) {groups}: set end_date or slot_count to "
                "materialize the group's coord"
            )
        record = resolved.as_resolved_index_record(run_id=run_id)
        if dry_run:
            return _preview_rebuild(chunk_manager, product_name, record, report=report, warn=warn)
        # ``ensure_resolved_index`` does not create the run; the caller's
        # recording does. A conflict still leaves a run that must be recorded failed.
        try:
            persisted_record, outcome = chunk_manager.ensure_resolved_index(
                product=product_name,
                record=record,
                run_id=run_id,
            )
            run_created = True
        except ResolvedIndexConflictError as exc:
            run_created = True
            warn(_text_keeping_message(exc))
            raise
        telemetry = TelemetryService(
            observability.create_ingestion_telemetry(
                plugin=plugin_name,
                product=product_name,
                output_format="zarr",
                write_mode="rebuild",
                run_id=run_id,
            ),
            plugin_name,
        )
        emit_index_ensured_full(
            chunk_manager,
            telemetry,
            product=product_name,
            run_id=run_id,
            record=persisted_record,
            outcome="rebuilt",
            logger=logger,
        )
        completed = True
        if outcome == "created":
            report(f"Rebuilt index record: identity_hash={persisted_record.identity_hash[:16]}")
        else:
            report(f"Index record unchanged: identity_hash={persisted_record.identity_hash[:16]}")
        return IndexRebuildResult(
            outcome=outcome,
            identity_hash=persisted_record.identity_hash,
            run_id=run_id,
        )
    finally:
        if run_created:
            try:
                if completed:
                    chunk_manager.record_run_terminal(
                        product=product_name,
                        run_id=run_id,
                        output_path=target,
                        output_format="zarr",
                        size=0,
                        meta=run_meta,
                        status="complete",
                    )
                else:
                    chunk_manager.record_run_failed(
                        product=product_name,
                        run_id=run_id,
                        output_path=target,
                        output_format="zarr",
                        size=0,
                        meta=run_meta,
                        error="rebuild aborted",
                    )
            except Exception as exc:
                logger.error("rebuild run termination failed: %s", exc)


def verify_index_record(
    *,
    chunk_manager: ChunkManager,
    product_name: str,
    target: str,
    plugin_name: str | None = None,
    load_plugin: Callable[[], Any] | None = None,
    configure: Callable[[Any, str], PluginContext] | None = None,
) -> IndexVerification:
    """Compare the stored resolved-index record with its root attribute and, optionally, the plugin.

    The call reads only and writes nothing. It loads the plugin only after the record
    checks pass, so a missing or legacy record never needs one.

    Args:
        chunk_manager: The control plane of the target product. The caller closes it.
        product_name: The logical product name.
        target: The product URI handed to the plugin's context.
        plugin_name: The plugin to compare against, or `None` to skip the comparison.
            It also names the plugin in the legacy-record message.
        load_plugin: Returns the plugin instance. Required when `plugin_name` is given.
        configure: Takes the plugin and the run id, returns the configured context for `index_spec`.

    Returns:
        The verdict, with the hashes it compared.

    Raises:
        ValueError: `plugin_name` is given without `load_plugin`.
        IndexRecordError: The plugin declares no index or a failing one; the
            plugin's exception is the cause.
        ManifestError: The root attributes cannot be read.
    """
    if plugin_name is not None and load_plugin is None:
        raise ValueError("load_plugin is required when plugin_name is given")
    try:
        record = chunk_manager.get_resolved_index(product=product_name)
    except ManifestError as exc:
        return IndexVerification(outcome="unreadable-record", detail=str(exc))
    try:
        check_legacy_index_record(
            chunk_manager, product=product_name, plugin_name=plugin_name or product_name
        )
    except LegacyIndexRecordError as exc:
        return IndexVerification(outcome="legacy-record", detail=_text_keeping_message(exc))

    attrs_hash = chunk_manager.read_resolved_index_attrs_hash(product=product_name)
    if record is None:
        if attrs_hash is not None:
            return IndexVerification(outcome="attrs-without-record", attrs_hash=attrs_hash)
        return IndexVerification(outcome="no-record")

    declared_hash: str | None = None
    if plugin_name is not None and load_plugin is not None:
        ingestor = load_plugin()
        declared_hash = _declared_index(
            ingestor,
            plugin_name=plugin_name,
            target=target,
            run_id="verify",
            action="verify",
            configure=configure,
        ).identity_hash
        if declared_hash != record.identity_hash:
            return IndexVerification(
                outcome="index-drift",
                identity_hash=record.identity_hash,
                attrs_hash=attrs_hash,
                declared_hash=declared_hash,
            )

    if attrs_hash is not None and attrs_hash != record.identity_hash:
        outcome: Literal["verified", "attrs-drift"] = "attrs-drift"
    else:
        outcome = "verified"
    return IndexVerification(
        outcome=outcome,
        identity_hash=record.identity_hash,
        attrs_hash=attrs_hash,
        declared_hash=declared_hash,
    )
