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

"""``firecube zarr slots``: chunk-aligned parallel ingestion plans."""

from __future__ import annotations

import contextlib
import json
import logging
import os
from collections.abc import Generator, Iterable, Iterator
from typing import TYPE_CHECKING, Any, cast

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import render
from firecube.cli._input_filters import input_filters_option, resolve_input_filters
from firecube.cli._input_sources import admit_sources, input_data_option, stac_search_option
from firecube.cli._product import resolve_product_identity
from firecube.cli._shared_options import (
    format_option,
    product_name_option,
    storage_driver_option,
    storage_type_option,
)
from firecube.cli._typed_options import TypedOptionsParam
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_and_validate_uri,
)
from firecube.cli.zarr._common import _configure_ingestor_for_cli
from firecube.core.controlplane import ChunkManager
from firecube.core.errors import Message, Operation, Placeholder
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.ingestor.registry import loader as ingestor_loader
from firecube.ingestor.registry.loader import discover_ingestors
from firecube.ingestor.runtime import slot_plan
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor

if TYPE_CHECKING:
    from firecube.core.config import StorageConfig
    from firecube.ingestor.runtime.slot_plan import (
        GroupSlotPlan,
        SlotPlanBlockedError,
        SlotPlanCeilingError,
    )

#: Plan JSON schema version. Independent of control-plane SCHEMA_VERSION.
PLAN_SCHEMA_VERSION = "v1"

#: Ranges in the whole plan, all groups, above which one stderr hint precedes the plan.
PLAN_RANGE_HINT = 100_000


def _warn_stderr(message: str) -> None:
    """Write an operator warning to stderr without contaminating Click stdout capture."""
    os.write(2, f"{render(message)}\n".encode())


class _WarnFilter(logging.Filter):
    """Block WARNING+ records for a target logger and its descendants.

    Thread-safe replacement for `logger.disabled = True`, which mutates a
    shared attribute and races between CLI threads. This filter is per-instance
    and only silences WARNING and above; DEBUG/INFO on the target logger stay
    visible if the operator has enabled them.
    """

    def __init__(self, name: str) -> None:
        super().__init__()
        self._target = name
        self._target_dot = name + "."

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != self._target and not record.name.startswith(self._target_dot):
            return True
        return record.levelno < logging.WARNING


@contextlib.contextmanager
def _suppress_logger_warnings(logger_name: str) -> Generator[None]:
    """Temporarily suppress WARNING+ records that would corrupt JSON CLI output.

    Attaches ``_WarnFilter`` to the target logger and to every handler along
    the target's ancestor chain (up to and including root) because Python's
    ``logging`` does not consult ancestor-logger filters when a descendant
    record propagates: only handler-level filters are called on the way up.
    Attaching at handlers is what actually blocks descendant WARNING records
    before they reach the stderr sink that would corrupt JSON output.
    """
    target = logging.getLogger(logger_name)
    flt = _WarnFilter(logger_name)
    target.addFilter(flt)
    handlers_touched: list[logging.Handler] = []
    current: logging.Logger | None = target
    while current is not None:
        for handler in list(current.handlers):
            handler.addFilter(flt)
            handlers_touched.append(handler)
        if not current.propagate:
            break
        current = current.parent
    try:
        yield
    finally:
        for handler in handlers_touched:
            handler.removeFilter(flt)
        target.removeFilter(flt)


def _discover_ingestors_quietly() -> dict[str, type[Any]]:
    """Discover plugins without allowing registry warnings to contaminate JSON output."""
    with _suppress_logger_warnings("firecube.registry"):
        return discover_ingestors()


@click.command(
    "slots",
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""\b
Examples:
  # Emit JSON plan for Argo/Kubeflow (resume-aware by default)
  firecube zarr slots <plugin> --target file:///tmp/x.zarr --product-name <name> \\
      --storage-type local --storage-driver fsspec --write-mode direct

  # Human-readable table output for inspection
  firecube zarr slots <plugin> --target file:///tmp/x.zarr --product-name <name> \\
      --storage-type local --storage-driver fsspec --write-mode direct -f table

  # Override slot partition size
  firecube zarr slots <plugin> --target file:///tmp/x.zarr --product-name <name> \\
      --storage-type local --storage-driver fsspec --write-mode direct --slot-size 200
""",
)
@click.argument("plugin")
@click.option(
    "--target",
    required=True,
    help="Target product URI required (file:///abs/path or s3://bucket/key).",
)
@product_name_option(required=True)
@storage_type_option(
    required=False, extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3)."
)
@storage_driver_option(required=False)
@click.option(
    "-w",
    "--write-mode",
    "write_mode",
    required=True,
    type=click.Choice(["staged", "direct"], case_sensitive=False),
    help="Write mode.",
)
@input_data_option(
    short=False,
    help="Source path, s3:// prefix or stac:// collection for AUTO discovery (optional, repeatable).",
)
@input_filters_option
@stac_search_option
@click.option(
    "--slot-size",
    "slot_size",
    type=int,
    default=None,
    help=(
        "Slot partition size. Default per group: the least common multiple of the time units "
        "of its time-indexed arrays that are not engine-owned, the time coordinate included "
        "(declared chunks or shards, else one slot), and of the stored chunk or shard of each "
        "pod-written array the target already has; the whole axis when there is none."
    ),
)
@click.option(
    "--no-resume",
    "no_resume",
    is_flag=True,
    help="Disable resume-aware narrowing; emit full [0, total_slots) ranges.",
)
@click.option(
    "--yes-i-really-mean-it",
    "yes_i_really_mean_it",
    is_flag=True,
    help=(
        f"Emit a plan with more ranges than the ceiling "
        f"({slot_plan.PLAN_RANGE_CEILING:,} ranges in the whole plan)."
    ),
)
@format_option(default="json")
@click.option(
    "--option",
    "option",
    multiple=True,
    type=TypedOptionsParam(),
    help="Plugin/engine option in key=value form.",
)
@click.pass_context
def slots(
    ctx: click.Context,
    plugin: str,
    target: str,
    product_name: str,
    storage_type: str,
    storage_driver: str,
    storage_anonymous: bool | None,
    write_mode: str,
    input_data: tuple[str, ...],
    input_filters: list[str] | None,
    stac_search: dict[str, Any] | None,
    slot_size: int | None,
    no_resume: bool,
    yes_i_really_mean_it: bool,
    output_format: str,
    option: tuple[tuple[str, object], ...] = (),
) -> None:
    """Emit chunk-aligned slot ranges for orchestrated parallel ingestion.

    Read-only: does NOT mutate target storage or tracking state.
    JSON output is Argo withItems / Kubeflow ParallelFor compatible.

    Existing pod-written arrays in the target constrain the slot size by their stored chunk or shard.
    The command writes each range as it generates it, so memory does not grow with the range count.
    Above 100,000 ranges in the plan it prints one hint on stderr before the plan.
    Above 1,000,000 ranges it refuses the plan; pass --yes-i-really-mean-it to emit it anyway.
    """
    input_filters = resolve_input_filters(ctx, plugin, input_filters)

    target_uri = parse_and_validate_uri(target, param_hint="--target")
    storage_type = apply_smart_default(target_uri, storage_type)
    sources = admit_sources(input_data, stac_search, command="firecube zarr slots")

    storage_config = get_storage_config(
        ctx,
        overrides={
            "storage_type": storage_type,
            "storage_driver": storage_driver,
            "anonymous": storage_anonymous,
        },
        cache=False,
    )
    resolved_storage_driver = storage_config.storage_driver
    identity = resolve_product_identity(
        target_uri,
        format="zarr",
        product_name=product_name,
    )
    binding = StorageBinding(
        identity=identity,
        driver=StorageDriverConfig.from_storage_config(storage_config),
    )
    chunk_manager = ChunkManager(binding=binding)

    plugins = (
        ingestor_loader.AVAILABLE_INGESTORS
        if plugin in ingestor_loader.AVAILABLE_INGESTORS
        else _discover_ingestors_quietly()
    )
    if plugin not in plugins:
        raise click.ClickException(f"Unknown plugin '{plugin}'.")
    ingestor_cls = cast(Any, plugins[plugin])

    ingestor = ingestor_cls(chunk_manager=chunk_manager) if sources else ingestor_cls()

    if not isinstance(ingestor, DirectZarrIngestor):
        raise click.ClickException(
            f"Plugin '{plugin}' does not support slot-range planning. "
            "This command is available for plugins that write Zarr stores in parallel."
        )
    with _suppress_logger_warnings("firecube.registry"):
        plugin_ctx = _configure_ingestor_for_cli(
            ingestor,
            target=target,
            options=option,
            input_filters=input_filters,
            stac_search=stac_search,
            sources=sources,
            run_id="zarr-slots",
        )

    def coverage_storage_config() -> StorageConfig:
        # Built as before the move: no `anonymous` override, unlike `storage_config`.
        return get_storage_config(
            ctx,
            overrides={
                "storage_type": storage_type,
                "storage_driver": resolved_storage_driver,
            },
            cache=False,
        )

    try:
        plans = slot_plan.plan_slots(
            ingestor=ingestor,
            plugin_ctx=plugin_ctx,
            plugin_name=plugin,
            chunk_manager=chunk_manager,
            identity=identity,
            target=target,
            storage_config=storage_config,
            coverage_storage_config=coverage_storage_config,
            input_data=sources or None,
            slot_size=slot_size,
            resume=not no_resume,
            allow_above_ceiling=yes_i_really_mean_it,
            alignment_warnings=output_format != "json",
            warn=_warn_stderr,
        )
    except slot_plan.SlotPlanBlockedError as exc:
        raise click.ClickException(render(_blocked_refusal(exc, target))) from exc
    except slot_plan.SlotPlanCeilingError as exc:
        raise click.ClickException(render(_ceiling_refusal(exc))) from exc
    except slot_plan.SlotPlanError as exc:
        raise click.ClickException(render(exc)) from exc

    range_count = sum(plan.range_count for plan in plans)
    if range_count > PLAN_RANGE_HINT:
        _warn_stderr(_range_hint(plans, range_count))

    header = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "product_name": product_name,
        "target": target,
        "storage_type": storage_type,
        "storage_driver": resolved_storage_driver,
        "write_mode": write_mode,
        "strategy": "indexed-region",
        "groups": [_group_summary(plan) for plan in plans],
    }
    _emit_slots_output(header, plans, output_format)


def _group_summary(plan: GroupSlotPlan) -> dict[str, Any]:
    """Return the group's entry of the plan's `groups` list."""
    owner = plan.static_owner
    return {
        "name": plan.name,
        "total_slots": plan.total,
        "slot_size": plan.slot_size,
        "covered_ranges": [[s, e] for s, e in plan.covered],
        "remaining_ranges": [[s, e] for s, e in plan.remaining],
        "blocked_ranges": [[s, e] for s, e in plan.blocked],
        "static_owner": None if owner is None else {"slot_start": owner[0], "slot_end": owner[1]},
    }


def _blocked_refusal(exc: SlotPlanBlockedError, target: str) -> Message:
    """Build the refusal for a plan with blocked ranges, naming the recovery commands.

    The `delete-span` preview carries `target` as its product name.
    """
    details = "; ".join(
        f"group '{name}' has blocked ranges {[[s, e] for s, e in intervals]}"
        for name, intervals in exc.blocked.items()
    )
    return Message(
        "firecube zarr slots: cannot emit executable slots — chunk-alignment coverage would silently drop work. "
        "Blocked: {details}. "
        "Remediation: preview with "
        "`{preview}`, "
        "then commit with `--yes-i-really-mean-it` and re-ingest using `firecube ingest ... --option force_reingest=true`. "
        "See docs/operations/parallel-zarr-writes.md#recover-a-blocked-plan.",
        details=details,
        preview=Operation(
            "delete_span",
            product_name=target,
            run_id=Placeholder("<id>"),
            dry_run=True,
            force=True,
        ),
    )


def _group_counts(plans: Iterable[GroupSlotPlan]) -> str:
    """Describe each group's range count and slot size, for operator messages."""
    return "; ".join(
        f"group '{plan.name}': {plan.range_count} ranges at slot size {plan.slot_size}"
        for plan in plans
    )


def _ceiling_refusal(exc: SlotPlanCeilingError) -> str:
    """Build the refusal for a plan above the range ceiling, naming an executable size if any."""
    plans, range_count, ceiling = exc.plans, exc.range_count, exc.ceiling
    suggested = exc.suggested_slot_size
    if suggested is None:
        remedy = (
            f"No --slot-size gives an executable plan of at most {ceiling} ranges; "
            "pass --yes-i-really-mean-it to emit this plan anyway."
        )
    else:
        remedy = (
            f"Pass --slot-size {suggested} for an executable plan of at most {ceiling} ranges, "
            "or --yes-i-really-mean-it to emit this plan anyway."
        )
    return (
        f"firecube zarr slots: the plan has {range_count} ranges, more than the limit of "
        f"{ceiling} ({_group_counts(plans)}). {remedy}"
    )


def _range_hint(plans: list[GroupSlotPlan], range_count: int) -> str:
    """Build the one-line stderr hint for a plan above the range hint threshold."""
    return (
        f"firecube zarr slots: the plan has {range_count} ranges ({_group_counts(plans)}). "
        "For fewer ranges pass --slot-size N with N a multiple of each group's slot size, "
        "or run one Indexed Job over JOB_COMPLETION_INDEX with FIRECUBE_SLOT_SIZE."
    )


#: Pieces joined into one write: bounds memory while keeping the number of writes small.
_WRITE_BATCH = 1024


def _echo_batched(pieces: Iterable[str]) -> None:
    """Write the pieces to stdout in order, joined in batches of `_WRITE_BATCH`."""
    batch: list[str] = []
    for piece in pieces:
        batch.append(piece)
        if len(batch) >= _WRITE_BATCH:
            click.echo("".join(batch), nl=False)
            batch.clear()
    if batch:
        click.echo("".join(batch), nl=False)


def _json_pieces(header: dict[str, Any], plans: list[GroupSlotPlan]) -> Iterator[str]:
    """Yield the plan as JSON text, byte-identical to `json.dumps(plan, indent=2)` plus a newline.

    The header goes through `json.dumps` with an empty `ranges` list; each range is formatted
    at the indentation `json.dumps` gives a member of that list. Only the group name needs
    escaping, so it goes through `json.dumps` once per group.
    """
    head, _, _ = json.dumps({**header, "ranges": []}, indent=2).rpartition("[]")
    yield head
    opener = "["
    for plan in plans:
        group = json.dumps(plan.name)
        for start, end in plan.ranges():
            yield (
                f"{opener}\n"
                "    {\n"
                f'      "group": {group},\n'
                f'      "slot_start": {start},\n'
                f'      "slot_end": {end},\n'
                '      "env": {\n'
                f'        "FIRECUBE_SLOT_START": "{start}",\n'
                f'        "FIRECUBE_SLOT_END": "{end}",\n'
                f'        "FIRECUBE_SLOT_GROUP": {group}\n'
                "      },\n"
                '      "cli_args": [\n'
                '        "--slot-start",\n'
                f'        "{start}",\n'
                '        "--slot-end",\n'
                f'        "{end}",\n'
                '        "--slot-group",\n'
                f"        {group}\n"
                "      ]\n"
                "    }"
            )
            opener = ","
    yield "[]\n}\n" if opener == "[" else "\n  ]\n}\n"


def _emit_slots_output(
    header: dict[str, Any], plans: list[GroupSlotPlan], output_format: str
) -> None:
    """Write the plan to stdout, generating each range as it is written.

    Raises:
        click.ClickException: `output_format` is not json, table or csv; nothing is written.
    """
    if output_format == "json":
        _echo_batched(_json_pieces(header, plans))
        return
    separators = {"table": "\t", "csv": ","}
    if output_format not in separators:
        raise click.ClickException(f"Unsupported output format: {output_format}")
    sep = separators[output_format]
    click.echo(f"group{sep}slot_start{sep}slot_end")
    _echo_batched(
        f"{plan.name}{sep}{start}{sep}{end}\n" for plan in plans for start, end in plan.ranges()
    )
