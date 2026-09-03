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

"""Mixed bounded/unbounded specs skip full IndexSpec record persistence.

When a plugin declares an ``IndexSpec`` with any unbounded group,
``firecube zarr preallocate`` MUST NOT write a bounded-only
``.firecube/index/current.json``. Persisting the rebuilt bounded-only
snapshot would silently claim the unbounded groups have never been
declared, breaking future resolved-index reads. Bounded groups still
materialize their coord arrays and receive their per-group identity
stamp (verified at ingest startup via
``BaseIngestor._verify_per_group_identity_at_store``); the fully-bounded
path remains unchanged and still persists the full record.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import direct_zarr_capable_test_plugin as _bounded_plugin_module
import mixed_bounded_unbounded_test_plugin as _mixed_plugin_module
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import FIRECUBE_GROUP_IDENTITY_HASH_ATTR
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    ResolvedIndexRecord,
)
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_MIXED_PLUGIN = "mixed_bounded_unbounded_test"
_MIXED_PRODUCT = "mixed_bounded_unbounded_test"
_MIXED_BOUNDED_GROUP = "data"
_MIXED_UNBOUNDED_GROUP = "aux"
_MIXED_COORD = "timestamp"
_MIXED_BOUNDED_SLOT_COUNT = 100

_BOUNDED_PLUGIN = "direct_zarr_capable_test_plugin"
_BOUNDED_PRODUCT = "direct_zarr_capable_test_product"


@pytest.fixture(autouse=True)
def _reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(_mixed_plugin_module)
    importlib.reload(_bounded_plugin_module)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(plugin: str, product: str, target: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        f"file://{target}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]


def _current_json_path(target: Path) -> Path:
    return target / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME


def test_mixed_spec_preallocate_does_not_write_index_current_json(tmp_path: Path) -> None:
    """Mixed bounded+unbounded spec must NOT create ``.firecube/index/current.json``.

    Persisting a bounded-only snapshot for a mixed spec would be a partial
    view that misleads later resolved-index reads. The skip-persistence rule skips
    ``ensure_resolved_index`` entirely when any group is unbounded.
    """
    target = tmp_path / "mixed.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(_MIXED_PLUGIN, _MIXED_PRODUCT, target))

    assert result.exit_code == 0, (
        f"mixed-spec preallocate must succeed (bounded group allocated), got "
        f"exit_code={result.exit_code!r}, output={result.output!r}, "
        f"stderr={result.stderr!r}, exception={result.exception!r}"
    )

    current_json = _current_json_path(target)
    assert not current_json.exists(), (
        f"mixed-spec preallocate must NOT persist a resolved-index record; "
        f"found {current_json!r} (contents={current_json.read_text()!r})"
    )
    assert "resolved index: skipped" in result.output, (
        f"CLI must announce the skip so operators do not mistake the absent "
        f"record for a persistence bug; got output={result.output!r}"
    )


def test_mixed_spec_preallocate_still_materializes_bounded_group_coord(tmp_path: Path) -> None:
    """Bounded groups in a mixed spec still materialize their coord arrays.

    The persistence skip is scoped to full-record persistence only. Per-group
    coord materialization (and its ``firecube_group_identity_hash``
    stamp) MUST still happen so the per-group verification path at
    ingest startup has something to compare against.
    """
    target = tmp_path / "mixed.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(_MIXED_PLUGIN, _MIXED_PRODUCT, target))
    assert result.exit_code == 0, result.output

    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    bounded_coord = cast(Any, root[f"{_MIXED_BOUNDED_GROUP}/{_MIXED_COORD}"])
    assert bounded_coord.shape == (_MIXED_BOUNDED_SLOT_COUNT,), (
        f"bounded group's coord must materialize at slot_count even when the "
        f"full record is skipped; got shape={bounded_coord.shape!r}"
    )

    attrs = dict(bounded_coord.attrs)
    assert FIRECUBE_GROUP_IDENTITY_HASH_ATTR in attrs, (
        f"bounded group's coord must carry the per-group identity stamp so "
        f"ingest-startup verification can match it against the live spec; "
        f"attrs={attrs!r}"
    )
    stamped = attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR]
    assert isinstance(stamped, str) and len(stamped) == 64, (
        f"per-group identity stamp must be a 64-char sha256 hex digest; got {stamped!r}"
    )


def test_fully_bounded_spec_preallocate_still_writes_index_current_json(tmp_path: Path) -> None:
    """Regression: fully-bounded specs still persist the full resolved-index record.

    The persistence skip is gated on ``skipped_unbounded`` being truthy. A fully-
    bounded spec must take the ``else`` branch, call
    ``ensure_resolved_index``, and produce a well-formed
    ``.firecube/index/current.json`` — otherwise ingest startup on the
    bounded product would find no record and behave as a fresh cube.
    """
    target = tmp_path / "bounded.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(_BOUNDED_PLUGIN, _BOUNDED_PRODUCT, target))
    assert result.exit_code == 0, (
        f"fully-bounded preallocate must succeed and persist the record; got "
        f"exit_code={result.exit_code!r}, output={result.output!r}, "
        f"stderr={result.stderr!r}, exception={result.exception!r}"
    )

    current_json = _current_json_path(target)
    assert current_json.exists(), (
        f"fully-bounded preallocate must persist a resolved-index record at "
        f"{current_json!r}; the persistence skip must NOT touch the fully-bounded path"
    )

    record = ResolvedIndexRecord.from_json_bytes(current_json.read_bytes())
    assert record.index.get("name") == "direct_zarr_capable_fixture_v2", (
        f"persisted record must carry the plugin's IndexSpec name; got "
        f"record.index={record.index!r}"
    )
    groups = record.index.get("groups", {})
    assert set(groups.keys()) == {"data"}, (
        f"persisted record must list every bounded group; got groups={groups!r}"
    )
    assert "resolved index: skipped" not in result.output, (
        f"fully-bounded path must NOT emit the mixed-skip notice; got output={result.output!r}"
    )


class _RelayState:
    """Mutable bound for the in-test plugin: ``None`` keeps ``aux`` unbounded."""

    aux_slot_count: int | None = None


class _RelayIngestor(_mixed_plugin_module.MixedBoundedUnboundedTestIngestor):
    """Same spec name across runs; ``aux`` becomes bounded when the state is set."""

    PRODUCT_NAME = "mixed_later_bounded_test"

    def index_spec(self, ctx: Any) -> Any:
        from firecube.core.api import IndexSpec, RegularTimeAxis

        return IndexSpec(
            name="mixed_later_bounded_v1",
            groups={
                _MIXED_BOUNDED_GROUP: RegularTimeAxis(
                    coordinate=_MIXED_COORD,
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                    mode="exact",
                    slot_count=_MIXED_BOUNDED_SLOT_COUNT,
                ),
                _MIXED_UNBOUNDED_GROUP: RegularTimeAxis(
                    coordinate=_MIXED_COORD,
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                    mode="exact",
                    slot_count=_RelayState.aux_slot_count,
                ),
            },
        )


def test_later_bounded_spec_rerun_persists_record_without_conflict(tmp_path: Path) -> None:
    """Bounding the previously-unbounded group later must not conflict.

    The mixed run persists no record, so once the plugin declares a bound for
    the remaining group there is no stale bounded-only snapshot to disagree
    with: the fully-bounded rerun persists the full record and an idempotent
    re-run matches it.
    """
    plugin = "mixed_later_bounded_test"
    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    _RelayIngestor.name = plugin  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[plugin] = _RelayIngestor
    _RelayState.aux_slot_count = None
    target = tmp_path / "later_bounded.zarr"
    runner = CliRunner()

    mixed = runner.invoke(cli, _preallocate_args(plugin, plugin, target))
    assert mixed.exit_code == 0, mixed.output
    assert not _current_json_path(target).exists(), (
        "mixed run must not persist a record; got a current.json"
    )

    _RelayState.aux_slot_count = 50
    try:
        bounded = runner.invoke(cli, _preallocate_args(plugin, plugin, target))
        assert bounded.exit_code == 0, (
            f"fully-bounded rerun must not conflict with the mixed run:\n{bounded.output}"
            f"\n{bounded.exception!r}"
        )
        record = ResolvedIndexRecord.from_json_bytes(_current_json_path(target).read_bytes())
        groups = record.index.get("groups", {})
        assert set(groups.keys()) == {_MIXED_BOUNDED_GROUP, _MIXED_UNBOUNDED_GROUP}, (
            f"full record must list both groups once bounded; got {groups!r}"
        )

        rerun = runner.invoke(cli, _preallocate_args(plugin, plugin, target))
        assert rerun.exit_code == 0, (
            f"idempotent rerun against the persisted full record must succeed:\n{rerun.output}"
            f"\n{rerun.exception!r}"
        )
        assert "matched_existing" in rerun.output, rerun.output
    finally:
        _RelayState.aux_slot_count = None
