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

"""Read-only CLI commands for resolved-index records."""

from __future__ import annotations

import contextlib
import datetime
import inspect
import logging
import uuid
from pathlib import Path
from typing import Any, NoReturn, cast

import click

from firecube.cli._formatter import FirecubeGroup
from firecube.core import observability
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.manager import check_legacy_index_record
from firecube.core.errors import LegacyIndexRecordError, ManifestError, ResolvedIndexConflictError
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.observability.metrics import TelemetryService, emit_index_ensured_full
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.uris import storage_uri_from_target
from firecube.ingestor.api import IngestContext, PluginContext, RuntimeIngestContext
from firecube.ingestor.registry import loader as ingestor_loader

logger = logging.getLogger(__name__)


@click.group(cls=FirecubeGroup)
def index() -> None:
    """Inspect resolved-index control-plane records."""


def _manager(target: str, product_name: str) -> ChunkManager:
    product_uri = storage_uri_from_target(target)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name=product_name),
        driver=StorageDriverConfig.from_storage_config_or_default(None),
    )
    workspace = Path.cwd()
    if product_uri.protocol == "file":
        workspace = Path(product_uri.path).parent
    return ChunkManager(binding=binding, workspace=workspace)


def _load_record(manager: ChunkManager, product_name: str) -> Any | None:
    try:
        return manager.get_resolved_index(product=product_name)
    except ManifestError as exc:
        click.echo(str(exc), err=True)
        raise click.exceptions.Exit(1) from exc


def _exit_no_record() -> NoReturn:
    click.echo("No index record found", err=True)
    raise click.exceptions.Exit(3)


def _plugin_context(target: str, *, run_id: str) -> PluginContext:
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


def _load_plugin(plugin: str, manager: ChunkManager) -> Any:
    plugins = (
        ingestor_loader.AVAILABLE_INGESTORS
        if plugin in ingestor_loader.AVAILABLE_INGESTORS
        else ingestor_loader.discover_ingestors()
    )
    if plugin not in plugins:
        raise click.ClickException(f"Unknown plugin '{plugin}'.")
    ingestor_cls = cast(Any, plugins[plugin])
    # Signature-based dispatch (not bare `except TypeError`) so a plugin-side
    # TypeError from a broken `__init__` propagates to the operator.
    try:
        sig = inspect.signature(ingestor_cls.__init__)
    except (TypeError, ValueError):
        return ingestor_cls()
    if "chunk_manager" in sig.parameters:
        return ingestor_cls(chunk_manager=manager)
    return ingestor_cls()


def _group_rows(index_payload: dict[str, Any]) -> list[tuple[str, str, str]]:
    groups = index_payload.get("groups", {})
    if not isinstance(groups, dict):
        return []
    rows: list[tuple[str, str, str]] = []
    for group_name, group_payload in sorted(groups.items()):
        kind = "unknown"
        size = "unknown"
        if isinstance(group_payload, dict):
            kind = str(group_payload.get("kind", kind))
            size = str(group_payload.get("size", size))
        rows.append((str(group_name), kind, size))
    return rows


def _derived_coordinates_for_group(
    group_name: str, group_payload: dict[str, Any]
) -> list[str] | None:
    kind = group_payload.get("kind")
    if kind != "regular_time":
        return None

    params = group_payload.get("params", {})
    if not isinstance(params, dict):
        raise click.ClickException(
            f"Group {group_name!r}: 'params' is missing or not a mapping in the index record."
        )

    missing = [f for f in ("epoch", "cadence_s") if f not in params]
    if "size" not in group_payload:
        missing.append("size")
    if missing:
        raise click.ClickException(
            f"Group {group_name!r}: cannot compute derived coordinates: "
            f"missing required field(s): {', '.join(sorted(missing))}. "
            "Re-run ingestion to populate the index record."
        )

    epoch_str: str = params["epoch"]
    cadence_s: int = int(params["cadence_s"])
    slot_count: int = int(group_payload["size"])

    try:
        epoch_dt = datetime.datetime.fromisoformat(epoch_str.replace("Z", "+00:00"))
    except (ValueError, TypeError) as exc:
        raise click.ClickException(
            f"Group {group_name!r}: cannot parse epoch {epoch_str!r}: {exc}"
        ) from exc

    delta = datetime.timedelta(seconds=cadence_s)
    return [(epoch_dt + i * delta).isoformat().replace("+00:00", "Z") for i in range(slot_count)]


@index.command("show")
@click.option("--target", required=True, help="Product Zarr URI to inspect.")
@click.option("--product-name", required=True, help="Logical product name.")
@click.option("--json", "as_json", is_flag=True, help="Emit the raw record as JSON.")
@click.option(
    "--derived/--no-derived",
    default=False,
    is_flag=True,
    help=(
        "Compute and display derived coordinates for RegularTimeAxis groups at read time. "
        "No-op for IrregularTimeAxis and IntegerAxis groups. "
        "NEVER persists anything."
    ),
)
def show_cmd(target: str, product_name: str, as_json: bool, derived: bool) -> None:
    """Show the current resolved-index record."""

    manager = _manager(target, product_name)
    try:
        record = _load_record(manager, product_name)
        if record is None:
            _exit_no_record()
        if as_json:
            click.echo(record.to_json_bytes().decode("utf-8"))
            return
        click.echo(f"schema_version: {record.schema_version}")
        click.echo(f"recorded_at: {record.recorded_at}")
        click.echo(f"recorded_by_run_id: {record.recorded_by_run_id}")
        click.echo(f"identity_hash: {record.identity_hash}")
        click.echo("groups:")
        click.echo("  group kind size")
        for group_name, kind, size in _group_rows(record.index):
            click.echo(f"  {group_name} {kind} {size}")
        if derived:
            groups = record.index.get("groups", {})
            if not isinstance(groups, dict):
                return
            any_regular = False
            for group_name, group_payload in sorted(groups.items()):
                if not isinstance(group_payload, dict):
                    continue
                coords = _derived_coordinates_for_group(group_name, group_payload)
                if coords is None:
                    click.echo(
                        f"  note: group {group_name!r} is not a regular_time axis; "
                        "--derived is a no-op for this group",
                        err=True,
                    )
                    continue
                any_regular = True
                click.echo(f"derived_coordinates[{group_name!r}]:")
                for coord in coords:
                    click.echo(f"  {coord}")
            if not any_regular:
                click.echo(
                    "note: no regular_time groups found; --derived produced no output",
                    err=True,
                )
    finally:
        manager.close()


@index.command("verify")
@click.option("--target", required=True, help="Product Zarr URI to inspect.")
@click.option("--product-name", required=True, help="Logical product name.")
@click.option("--plugin", required=False, help="Optional plugin name to compare against.")
def verify_cmd(target: str, product_name: str, plugin: str | None) -> None:
    """Verify the current resolved-index record and mirrored root attr."""

    manager = _manager(target, product_name)
    try:
        record = _load_record(manager, product_name)
        try:
            check_legacy_index_record(
                manager, product=product_name, plugin_name=plugin or product_name
            )
        except LegacyIndexRecordError as exc:
            click.echo(str(exc), err=True)
            raise click.exceptions.Exit(1) from exc

        attrs_hash = manager.read_resolved_index_attrs_hash(product=product_name)
        if record is None:
            if attrs_hash is not None:
                click.echo(
                    "attrs drift — root attr has resolved-index identity hash but no "
                    "on-disk record; run `firecube zarr index rebuild`",
                    err=True,
                )
                raise click.exceptions.Exit(1)
            _exit_no_record()

        if plugin is not None:
            ingestor = _load_plugin(plugin, manager)
            try:
                spec = ingestor.index_spec(_plugin_context(target, run_id="verify"))
                if spec is None:
                    raise click.ClickException(
                        f"Plugin '{plugin}' does not opt into parallel ingestion "
                        f"(index_spec() returned None). Nothing to verify."
                    )
                resolved = resolve_index_spec(
                    spec,
                    time_dim_name=str(getattr(ingestor, "time_dim_name", "timestamp")),
                )
            except click.ClickException:
                raise
            except Exception as exc:
                raise click.ClickException(
                    f"Plugin '{plugin}' failed to resolve index_spec: {exc}"
                ) from exc
            declared_hash = resolved.identity_hash
            if declared_hash != record.identity_hash:
                click.echo(
                    "index drift — plugin resolved-index identity hash "
                    f"{declared_hash[:8]} differs from on-disk record "
                    f"{record.identity_hash[:8]}",
                    err=True,
                )
                raise click.exceptions.Exit(1)

        if attrs_hash is None:
            click.echo(
                "root attr will be re-mirrored on next `ensure` — this is the "
                "expected repair path when only the on-disk record exists, not drift"
            )
        elif attrs_hash != record.identity_hash:
            click.echo(
                "attrs drift — root attr diverges from on-disk record; run "
                "`firecube zarr index rebuild`",
                err=True,
            )
            raise click.exceptions.Exit(1)
        click.echo(f"VERIFIED resolved index {record.identity_hash[:16]}")
    finally:
        manager.close()


@index.command("rebuild")
@click.option("--target", required=True, help="Product Zarr URI to rebuild the index for.")
@click.option("--plugin", required=True, help="Plugin name used to resolve the index spec.")
@click.option("--product-name", required=True, help="Logical product name.")
def rebuild_cmd(target: str, plugin: str, product_name: str) -> None:
    """Rebuild the resolved-index record from a plugin declaration."""

    manager = _manager(target, product_name)
    run_id = f"rebuild-{uuid.uuid4()}"
    run_meta = {"run_id": run_id, "command": "index_rebuild"}
    index_event_run_created = False
    completed = False
    try:
        ingestor = _load_plugin(plugin, manager)
        try:
            spec = ingestor.index_spec(_plugin_context(target, run_id=run_id))
        except Exception as exc:
            raise click.ClickException(
                f"Plugin '{plugin}' failed to resolve index_spec: {exc}"
            ) from exc
        if spec is None:
            raise click.ClickException(
                f"Plugin '{plugin}' does not opt into parallel ingestion "
                f"(index_spec() returned None). Nothing to rebuild."
            )
        try:
            resolved = resolve_index_spec(
                spec,
                time_dim_name=str(getattr(ingestor, "time_dim_name", "timestamp")),
            )
        except Exception as exc:
            raise click.ClickException(
                f"Plugin '{plugin}' failed to resolve index_spec: {exc}"
            ) from exc
        legacy_model = resolved.as_legacy_slot_index_model()
        if legacy_model is not None:
            persisted_legacy_hash = manager.read_slot_index_attrs_hash(product=product_name)
            declared_legacy_hash = legacy_model.identity_hash
            if persisted_legacy_hash is not None and persisted_legacy_hash != declared_legacy_hash:
                raise click.ClickException(
                    f"Refusing to rebuild: persisted legacy slot-index identity hash "
                    f"({persisted_legacy_hash[:16]}) differs from plugin's declared hash "
                    f"({declared_legacy_hash[:16]}). Rebuilding would silently re-anchor "
                    f"slot positions and disagree with data already on disk. To override: "
                    f"(1) remove the `firecube_slot_index_model_identity_hash` root attr from the Zarr store, "
                    f"AND (2) delete `.firecube/slot_index/current.json`. Both steps required."
                )
        record = resolved.as_resolved_index_record(run_id=run_id)
        try:
            persisted_record, outcome = manager.ensure_resolved_index(
                product=product_name,
                record=record,
                run_id=run_id,
            )
            index_event_run_created = True
        except ResolvedIndexConflictError as exc:
            index_event_run_created = True
            click.echo(str(exc), err=True)
            raise click.exceptions.Exit(1) from exc
        telemetry = TelemetryService(
            observability.create_ingestion_telemetry(
                plugin=plugin,
                product=product_name,
                output_format="zarr",
                write_mode="rebuild",
                run_id=run_id,
            ),
            plugin,
        )
        emit_index_ensured_full(
            manager,
            telemetry,
            product=product_name,
            run_id=run_id,
            record=persisted_record,
            outcome="rebuilt",
            logger=logger,
        )
        completed = True
        if outcome == "created":
            click.echo(f"Rebuilt index record: identity_hash={persisted_record.identity_hash[:16]}")
            return
        click.echo(f"Index record unchanged: identity_hash={persisted_record.identity_hash[:16]}")
    finally:
        if index_event_run_created:
            try:
                if completed:
                    manager.record_run_terminal(
                        product=product_name,
                        run_id=run_id,
                        output_path=target,
                        output_format="zarr",
                        size=0,
                        meta=run_meta,
                        status="complete",
                    )
                else:
                    manager.record_run_failed(
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
        with contextlib.suppress(Exception):
            manager.close()
