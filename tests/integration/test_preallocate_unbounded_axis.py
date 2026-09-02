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

"""Preallocate must skip unbounded axes with an audible warning, not crash.

An unbounded ``RegularTimeAxis`` (no ``end_date`` and no ``slot_count``)
has no fixed extent, so ``resolved_index.size(group)`` raises
``ExtentUnknownError``. The preallocate CLI must:

* materialize every bounded group's coord array as before,
* skip each unbounded group with a per-group stderr warning that names
  the group and points the operator at ``end_date`` / ``slot_count``,
* hard-fail with ``ConfigurationError`` when *every* group is unbounded
  (there is nothing to preallocate — silently succeeding would look like
  a completed run and mask the misconfiguration),
* preserve ``--dry-run`` behavior — dry-run already tolerates unbounded
  axes via ``_resolve_preallocate_windows``'s guard and must not now
  emit the new warning or fail.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration


_MIXED_PLUGIN_NAME = "preallocate_unbounded_axis_mixed_test_plugin"
_ALL_UNBOUNDED_PLUGIN_NAME = "preallocate_unbounded_axis_all_test_plugin"
_EPOCH_ISO = "2024-01-01T00:00:00Z"
_CADENCE_S = 600
_BOUNDED_SLOT_COUNT = 100
_COORD_NAME = "time"
_BOUNDED_GROUP = "bounded"
_UNBOUNDED_GROUP = "unbounded"
_UNBOUNDED_GROUP_A = "unbounded_a"
_UNBOUNDED_GROUP_B = "unbounded_b"


class _MixedBoundedUnboundedIngestor(DirectZarrIngestor):
    """One bounded (``slot_count=100``) + one unbounded ``RegularTimeAxis``.

    The bounded group carries a ``(time,)`` coord spec so preallocate has
    something to materialize; the unbounded group carries no coord spec
    (there is nothing to allocate — its extent is unknown) and is expected
    to be skipped audibly.
    """

    PRODUCT_NAME: ClassVar[str] = _MIXED_PLUGIN_NAME
    time_dim_name: ClassVar[str] = _COORD_NAME

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name=f"{_MIXED_PLUGIN_NAME}_v1",
            groups={
                _BOUNDED_GROUP: RegularTimeAxis(
                    coordinate=_COORD_NAME,
                    epoch=_EPOCH_ISO,
                    cadence_s=_CADENCE_S,
                    mode="exact",
                    slot_count=_BOUNDED_SLOT_COUNT,
                ),
                _UNBOUNDED_GROUP: RegularTimeAxis(
                    coordinate=_COORD_NAME,
                    epoch=_EPOCH_ISO,
                    cadence_s=_CADENCE_S,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, (str, dt.datetime)):
            return None
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group=_BOUNDED_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name=_COORD_NAME,
                        shape=(_BOUNDED_SLOT_COUNT,),
                        dtype="datetime64[ns]",
                        chunks=None,
                        fill_value=np.datetime64("NaT", "ns"),
                        expected_time_count=_BOUNDED_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD_NAME,),
                    ),
                ],
            ),
            ZarrGroupSpec(
                group=_UNBOUNDED_GROUP,
                arrays=[],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


class _AllUnboundedIngestor(DirectZarrIngestor):
    """Every group's ``RegularTimeAxis`` is unbounded.

    Preallocate must hard-fail with ``ConfigurationError`` — there is
    nothing to allocate for any group, and silently succeeding would mask
    a plugin/config bug (no epoch bound, no slot count) that will surface
    much later as a broken ingest.
    """

    PRODUCT_NAME: ClassVar[str] = _ALL_UNBOUNDED_PLUGIN_NAME
    time_dim_name: ClassVar[str] = _COORD_NAME

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name=f"{_ALL_UNBOUNDED_PLUGIN_NAME}_v1",
            groups={
                _UNBOUNDED_GROUP_A: RegularTimeAxis(
                    coordinate=_COORD_NAME,
                    epoch=_EPOCH_ISO,
                    cadence_s=_CADENCE_S,
                ),
                _UNBOUNDED_GROUP_B: RegularTimeAxis(
                    coordinate=_COORD_NAME,
                    epoch=_EPOCH_ISO,
                    cadence_s=_CADENCE_S,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, (str, dt.datetime)):
            return None
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(group=_UNBOUNDED_GROUP_A, arrays=[]),
            ZarrGroupSpec(group=_UNBOUNDED_GROUP_B, arrays=[]),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.fixture(autouse=True)
def _register_stub_plugins() -> Iterator[None]:
    """Install both stub ingestors under stable plugin names per test."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)

    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    _MixedBoundedUnboundedIngestor.name = _MIXED_PLUGIN_NAME  # pyright: ignore[reportAttributeAccessIssue]
    _AllUnboundedIngestor.name = _ALL_UNBOUNDED_PLUGIN_NAME  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_MIXED_PLUGIN_NAME] = _MixedBoundedUnboundedIngestor
    _loader.AVAILABLE_INGESTORS[_ALL_UNBOUNDED_PLUGIN_NAME] = _AllUnboundedIngestor
    try:
        yield
    finally:
        _loader._LOADED = original_loaded
        _loader.AVAILABLE_INGESTORS.clear()
        _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(plugin: str, target: Path, *, dry_run: bool = False) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        f"file://{target}",
        "--product-name",
        plugin,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    if dry_run:
        args.append("--dry-run")
    return args


def test_preallocate_mixed_bounded_and_unbounded_axis_skips_unbounded_with_warning(
    tmp_path: Path,
) -> None:
    """Mixed groups: bounded coord materializes; unbounded is skipped audibly.

    The stderr warning must name the unbounded group and point the
    operator at the two remediations (``end_date`` / ``slot_count``). The
    bounded group's coord array must exist at its declared shape. The
    unbounded group must NOT gain a coord array (its extent is unknown,
    so there is nothing to allocate).
    """
    target = tmp_path / "mixed.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(_MIXED_PLUGIN_NAME, target))

    assert result.exit_code == 0, (
        f"mixed bounded+unbounded preallocate must succeed (bounded group allocated, "
        f"unbounded skipped), got exit_code={result.exit_code!r}, "
        f"output={result.output!r}, stderr={result.stderr!r}, "
        f"exception={result.exception!r}"
    )

    expected_warning = (
        f"warning: skipping unbounded group {_UNBOUNDED_GROUP!r}: "
        "set end_date or slot_count to materialize this group's coord"
    )
    assert expected_warning in result.stderr, (
        f"per-group stderr warning must name the unbounded group and remediation; "
        f"expected substring={expected_warning!r}, stderr={result.stderr!r}"
    )

    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    bounded_coord = cast(Any, root[f"{_BOUNDED_GROUP}/{_COORD_NAME}"])
    assert bounded_coord.shape == (_BOUNDED_SLOT_COUNT,), (
        f"bounded group's coord must materialize at slot_count; got shape={bounded_coord.shape!r}"
    )

    unbounded_dir = target / _UNBOUNDED_GROUP / _COORD_NAME
    assert not unbounded_dir.exists(), (
        f"unbounded group must not gain a coord array (extent unknown); found {unbounded_dir!r}"
    )


def test_preallocate_all_unbounded_axes_hard_fails_with_configuration_error(
    tmp_path: Path,
) -> None:
    """All groups unbounded ⇒ preallocate must hard-fail, not silently succeed.

    Silent success on an all-unbounded config would masquerade as a
    completed preallocate run and mask the misconfiguration until a much
    later ingest failure. The failure must be a ``ConfigurationError``
    (routed to the ``_KNOWN_USER_ERROR_TYPE_NAMES`` set at the CLI
    boundary) whose message names the actionable remediation.
    """
    target = tmp_path / "all-unbounded.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(_ALL_UNBOUNDED_PLUGIN_NAME, target))

    assert result.exit_code != 0, (
        f"all-unbounded preallocate must exit non-zero (silent success masks "
        f"the misconfiguration), got exit_code={result.exit_code!r}, "
        f"output={result.output!r}, stderr={result.stderr!r}"
    )
    # @wrap_user_facing_errors converts ConfigurationError to a clean CLI error
    # message (SystemExit(1)) — no raw traceback should appear in stderr.
    assert "Traceback" not in result.stderr, (
        f"failure must not surface as a raw traceback; stderr={result.stderr!r}"
    )
    assert "requires at least one bounded axis" in result.stderr, (
        f"error message must name the ``at least one bounded axis`` invariant "
        f"so operators know the remediation; got stderr={result.stderr!r}"
    )


def test_preallocate_dry_run_on_unbounded_axis_still_succeeds(tmp_path: Path) -> None:
    """Dry-run on an unbounded axis must remain a zero-exit no-op mutation.

    The fix touches only the live-write branch (past the
    ``if dry_run: ... return`` guard); dry-run behavior is preserved.
    Dry-run must not gain the new stderr warning and must not now fail
    on the mixed plugin — that would signal an accidental edit to the
    dry-run path.
    """
    target = tmp_path / "dry-run.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(_MIXED_PLUGIN_NAME, target, dry_run=True))

    assert result.exit_code == 0, (
        f"dry-run on unbounded axis must still succeed (the live-path fix "
        f"must not touch the dry-run early-return path), got "
        f"exit_code={result.exit_code!r}, output={result.output!r}, "
        f"stderr={result.stderr!r}, exception={result.exception!r}"
    )
    assert not target.exists(), f"dry-run must not create the target store; found {target!r}"


def test_preallocate_dry_run_on_all_unbounded_axes_still_succeeds(tmp_path: Path) -> None:
    """Dry-run on an all-unbounded plugin must NOT hard-fail.

    Dry-run's job is to preview the resolved index and windows, not to
    gate configuration. The ``ConfigurationError`` on zero bounded axes
    is a live-write invariant only; a dry-run against a misconfigured
    plugin must exit 0, emit a (possibly empty) preview, and leave the
    store untouched so operators can inspect the plan before fixing.
    """
    target = tmp_path / "dry-run-all-unbounded.zarr"

    result = CliRunner().invoke(
        cli, _preallocate_args(_ALL_UNBOUNDED_PLUGIN_NAME, target, dry_run=True)
    )

    assert result.exit_code == 0, (
        f"dry-run on an all-unbounded plugin must succeed (preview only, no "
        f"config enforcement), got exit_code={result.exit_code!r}, "
        f"output={result.output!r}, stderr={result.stderr!r}, "
        f"exception={result.exception!r}"
    )
    assert "requires at least one bounded axis" not in result.stderr, (
        f"dry-run must not emit the live-path hard-fail message; stderr={result.stderr!r}"
    )
    assert "warning: skipping unbounded group" not in result.stderr, (
        f"dry-run must not emit the live-path per-group warning; stderr={result.stderr!r}"
    )
    assert not target.exists(), f"dry-run must not create the target store; found {target!r}"
