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
import functools
import inspect
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, cast

import click

from firecube.cli._errors import echo_rendered, render
from firecube.cli._exit_codes import ExitCode
from firecube.cli._formatter import FirecubeGroup
from firecube.cli._product import recorded_identity
from firecube.cli._shared_options import format_option, product_name_option, target_uri_option
from firecube.cli._typed_options import TypedOptionsParam
from firecube.cli._uri_policy import parse_and_validate_uri
from firecube.core.controlplane import ChunkManager
from firecube.core.errors import (
    ManifestError,
    Message,
    Operation,
    ResolvedIndexConflictError,
)
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.time.calendar import is_gregorian_like
from firecube.ingestor.registry import loader as ingestor_loader

if TYPE_CHECKING:
    from collections.abc import Callable

    from firecube.ingestor.api import PluginContext
    from firecube.ingestor.runtime.index_record import IndexVerification

logger = logging.getLogger(__name__)


@click.group(cls=FirecubeGroup)
def index() -> None:
    """Inspect resolved-index control-plane records."""


def _manager(target: str, product_name: str) -> ChunkManager:
    product_uri = parse_and_validate_uri(target, param_hint="--target")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name=product_name),
        driver=StorageDriverConfig.from_storage_config_or_default(None),
    )
    workspace = Path.cwd()
    if product_uri.protocol == "file":
        workspace = Path(product_uri.path).parent
    return ChunkManager(binding=binding, workspace=workspace)


def _recorded_manager(target: str) -> tuple[ChunkManager, str]:
    """Return the manager of the product at `target` and the label its store records.

    Raises:
        click.UsageError: The store records no label.
    """
    uri = parse_and_validate_uri(target, param_hint="--target")
    driver = StorageDriverConfig.from_storage_config_or_default(None)
    label = recorded_identity(uri, format="zarr", driver=driver).product_name
    return _manager(target, label), label


def _load_record(manager: ChunkManager, product_name: str) -> Any | None:
    try:
        return manager.get_resolved_index(product=product_name)
    except ManifestError as exc:
        click.echo(render(exc), err=True)
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED) from exc


def _exit_no_record() -> NoReturn:
    click.echo("No index record found", err=True)
    raise click.exceptions.Exit(ExitCode.CHECK_FAILED)


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


def _configurer(
    target: str, options: tuple[tuple[str, object], ...]
) -> Callable[[Any, str], PluginContext]:
    """Return the callable that configures a loaded plugin with the `--option` values."""

    def configure(ingestor: Any, run_id: str) -> PluginContext:
        from firecube.cli.zarr._common import _configure_ingestor_for_cli

        return _configure_ingestor_for_cli(ingestor, target=target, options=options, run_id=run_id)

    return configure


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


def _decoded_calendar_isoformats(values: Any, *, units: str, calendar: str) -> list[str]:
    """Decode encoded numbers into calendar dates and format each as ISO 8601.

    ``decode_time_array`` always returns an object array of calendar-valued
    (``cftime``-shaped) scalars here, never ``datetime64``: this helper is
    only called for a non-Gregorian-like calendar, and xarray's CF decoder
    returns an object array of calendar-valued scalars for any such
    calendar. Each scalar's own ``isoformat()`` is used, not a Gregorian
    ``datetime64`` conversion.
    """
    import numpy as np

    from firecube.core.zarr.time_decode import decode_time_array

    encoded = np.asarray(values)
    decoded = decode_time_array(encoded, {"units": units, "calendar": calendar})
    return [item.isoformat() for item in np.asarray(decoded).reshape(-1)]


def _derived_coordinates_for_group(
    group_name: str, group_payload: dict[str, Any]
) -> list[str] | None:
    kind = group_payload.get("kind")
    if kind not in ("regular_time", "irregular_time"):
        return None

    params = group_payload.get("params", {})
    if not isinstance(params, dict):
        raise click.ClickException(
            f"Group {group_name!r}: 'params' is missing or not a mapping in the index record."
        )
    calendar = params.get("calendar")
    calendar_is_gregorian = calendar is None or is_gregorian_like(str(calendar))

    if kind == "irregular_time":
        # No-op for a Gregorian-like calendar (the default,
        # proleptic_gregorian): an irregular axis's coordinate is already
        # the plugin-declared explicit timestamp, nothing to derive.
        if calendar_is_gregorian:
            return None
        values = params.get("values")
        units = params.get("units")
        if not isinstance(values, list) or units is None:
            raise click.ClickException(
                f"Group {group_name!r}: cannot compute derived coordinates: missing "
                "required field(s) 'values'/'units' for a calendar irregular_time group. "
                "Re-run ingestion to populate the index record."
            )
        return _decoded_calendar_isoformats(values, units=str(units), calendar=str(calendar))

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

    if not calendar_is_gregorian:
        import numpy as np

        from firecube.core.time.calendar import derive_regular_axis_units

        units = derive_regular_axis_units(epoch_str)
        encoded = np.arange(slot_count, dtype=np.int64) * cadence_s
        return _decoded_calendar_isoformats(encoded, units=units, calendar=str(calendar))

    try:
        epoch_dt = datetime.datetime.fromisoformat(epoch_str.replace("Z", "+00:00"))
    except (ValueError, TypeError) as exc:
        raise click.ClickException(
            f"Group {group_name!r}: cannot parse epoch {epoch_str!r}: {render(exc)}"
        ) from exc

    delta = datetime.timedelta(seconds=cadence_s)
    return [(epoch_dt + i * delta).isoformat().replace("+00:00", "Z") for i in range(slot_count)]


def _derived_coordinates_label_for_group(
    group_name: str, group_payload: dict[str, Any]
) -> str | None:
    coords = _derived_coordinates_for_group(group_name, group_payload)
    if coords is None:
        return None

    params = group_payload.get("params", {})
    mode = params.get("mode") if isinstance(params, dict) else None
    label = f"derived_coordinates[{group_name!r}]"
    if mode == "floor":
        label += " (nominal grid labels — stored values are observed times)"
    return f"{label}:"


@index.command(
    "show",
    epilog="""\b
Examples:
  # show the resolved-index record
  firecube zarr index show --target file:///data/p.zarr

\b
  # also compute derived coordinates of regular time axes
  firecube zarr index show --target file:///data/p.zarr --derived

See also: firecube zarr index verify, firecube zarr index rebuild
""",
)
@target_uri_option(tier="inspect")
@format_option(("table", "json"))
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
def show_cmd(target: str, output_format: str, derived: bool) -> None:
    """Show the current resolved-index record; `-f json` emits the raw record."""

    manager, product_name = _recorded_manager(target)
    try:
        record = _load_record(manager, product_name)
        if record is None:
            _exit_no_record()
        if output_format == "json":
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
                click.echo(_derived_coordinates_label_for_group(group_name, group_payload))
                for coord in coords:
                    click.echo(f"  {coord}")
            if not any_regular:
                click.echo(
                    "note: no regular_time groups found; --derived produced no output",
                    err=True,
                )
    finally:
        manager.close()


@index.command(
    "verify",
    epilog="""\b
Examples:
  # check the record against the root attr
  firecube zarr index verify --target file:///data/p.zarr

\b
  # also compare the record with a plugin's declaration
  firecube zarr index verify --target file:///data/p.zarr --plugin my_plugin

See also: firecube zarr index show, firecube zarr index rebuild
""",
)
@target_uri_option(tier="inspect")
@click.option("--plugin", required=False, help="Optional plugin name to compare against.")
@click.option(
    "--option",
    "option",
    multiple=True,
    type=TypedOptionsParam(),
    help="Plugin option in key=value form. Requires --plugin.",
)
def verify_cmd(target: str, plugin: str | None, option: tuple[tuple[str, object], ...]) -> None:
    """Verify the current resolved-index record and mirrored root attr."""

    from firecube.ingestor.runtime.index_record import IndexRecordError, verify_index_record

    if option and plugin is None:
        raise click.UsageError("--option requires --plugin")
    manager, product_name = _recorded_manager(target)
    try:
        load_plugin = (
            functools.partial(_load_plugin, plugin, manager) if plugin is not None else None
        )
        try:
            verification = verify_index_record(
                chunk_manager=manager,
                product_name=product_name,
                target=target,
                plugin_name=plugin,
                load_plugin=load_plugin,
                configure=_configurer(target, option),
            )
        except IndexRecordError as exc:
            # A plugin's own ClickException reaches the operator unchanged.
            cause = exc.__cause__
            if isinstance(cause, click.ClickException):
                raise cause  # noqa: B904 - re-raise the plugin's exception as it was raised
            raise
        _render_verification(
            verification,
            rebuild=Operation(
                "rebuild_index", target=target, plugin=plugin, product_name=product_name
            ),
        )
    finally:
        manager.close()


def _render_verification(verification: IndexVerification, *, rebuild: Operation) -> None:
    """Echo a verification verdict; exit 1 on drift, an unusable record or no record.

    `rebuild` is the `zarr index rebuild` call, with this invocation's values, that the
    attrs advice prints.
    """
    outcome = verification.outcome
    if outcome in ("unreadable-record", "legacy-record"):
        detail = verification.detail
        click.echo(str(detail) if detail is None else render(detail), err=True)
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED)
    if outcome == "attrs-without-record":
        advice = Message(
            "attrs drift — root attr has resolved-index identity hash but no "
            "on-disk record; `{rebuild}` refuses this state. "
            "Restore `.firecube/index/current.json`, or remove the root attrs "
            "`firecube_resolved_index_identity_hash` and `firecube_resolved_index` "
            "and run `{rebuild}` with the plugin that wrote the data.",
            rebuild=rebuild,
        )
        click.echo(render(advice), err=True)
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED)
    if outcome == "no-record":
        _exit_no_record()
    identity_hash = str(verification.identity_hash)
    if outcome == "index-drift":
        click.echo(
            "index drift — plugin resolved-index identity hash "
            f"{str(verification.declared_hash)[:8]} differs from on-disk record "
            f"{identity_hash[:8]}",
            err=True,
        )
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED)
    if outcome == "attrs-drift":
        advice = Message(
            "attrs drift — root attr diverges from on-disk record; "
            "`{rebuild}` refuses this state. "
            "Remove the root attr `firecube_resolved_index_identity_hash`, "
            "then run `{rebuild}`: it writes the attr again "
            "from the record.",
            rebuild=rebuild,
        )
        click.echo(render(advice), err=True)
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED)
    if verification.attrs_hash is None:
        click.echo(
            "root attr will be re-mirrored on next `ensure` — this is the "
            "expected repair path when only the on-disk record exists, not drift"
        )
    click.echo(f"VERIFIED resolved index {identity_hash[:16]}")


@index.command(
    "rebuild",
    epilog="""\b
Examples:
  # rebuild the record from a plugin's declaration
  firecube zarr index rebuild --target file:///data/p.zarr --plugin my_plugin --product-name p

\b
  # pass a plugin option
  firecube zarr index rebuild --target file:///data/p.zarr --plugin my_plugin --product-name p \\
      --option key=value

See also: firecube zarr index verify, firecube zarr index show
""",
)
@target_uri_option(tier="write")
@click.option("--plugin", required=True, help="Plugin name used to resolve the index spec.")
@product_name_option(required=True)
@click.option(
    "--option",
    "option",
    multiple=True,
    type=TypedOptionsParam(),
    help="Plugin option in key=value form.",
)
def rebuild_cmd(
    target: str, plugin: str, product_name: str, option: tuple[tuple[str, object], ...]
) -> None:
    """Rebuild the resolved-index record from a plugin declaration."""

    from firecube.ingestor.runtime.index_record import rebuild_index_record

    manager = _manager(target, product_name)
    try:
        ingestor = _load_plugin(plugin, manager)
        try:
            rebuild_index_record(
                ingestor=ingestor,
                plugin_name=plugin,
                chunk_manager=manager,
                target=target,
                product_name=product_name,
                logger=logger,
                report=echo_rendered,
                warn=functools.partial(echo_rendered, err=True),
                configure=_configurer(target, option),
            )
        except ResolvedIndexConflictError as exc:
            # The library has already echoed the conflict through ``warn``.
            raise click.exceptions.Exit(ExitCode.CHECK_FAILED) from exc
    finally:
        with contextlib.suppress(Exception):
            manager.close()
