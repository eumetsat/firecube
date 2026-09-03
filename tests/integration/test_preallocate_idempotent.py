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

"""Regression coverage for engine-managed floor-axis coord materialization.

These tests lock the four operational guarantees that make ``firecube zarr
preallocate`` safe to retry, resume, and extend against a live coord array:

* **Idempotent same-window** re-run makes zero writes and leaves the coord
  chunk file byte-identical.
* **Disjoint window extension** ``[0..N) → [N..2N)`` succeeds twice and
  preserves the ``firecube_coord_managed`` marker through both runs.
* **Conflicting re-run** with drifting ``inspect_item`` output raises
  :class:`SchemaDriftError` and preserves the first run's values byte-for-byte
  (no partial overwrite).
* **Interrupted materialization** (marker stamped but partial ``NaT`` coord
  entries in the window) reconciles per slot: matching stored entries are
  no-ops, ``NaT`` entries are filled, and divergent stored values still raise.

The inline stub plugin mirrors the pattern established by
``test_off_grid_floor_ingest_e2e.py``; a module-level ``_StubState`` steers
the plugin between runs without touching the shared
``regular_axis_test_plugin`` fixture.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
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


_STUB_PLUGIN_NAME = "preallocate_idempotent_test_plugin"
_EPOCH_ISO = "2024-01-01T00:00:00Z"
_EPOCH_NS = np.datetime64("2024-01-01T00:00:00", "ns")
_CADENCE_S = 600
_SLOT_COUNT = 12
_COORD_NAME = "time"
_GROUP = "data"
_VALUES_ARRAY = "values"
_VALUES_X_DIM = 4
_DEFAULT_OFFSET_S = 2


class _StubState:
    """Per-test knobs for the module-level stub ingestor."""

    slot_indices: ClassVar[list[int]] = list(range(_SLOT_COUNT))
    offset_s: ClassVar[int] = _DEFAULT_OFFSET_S

    @classmethod
    def reset(cls) -> None:
        cls.slot_indices = list(range(_SLOT_COUNT))
        cls.offset_s = _DEFAULT_OFFSET_S


def _off_grid_time_for_slot(slot: int, *, offset_s: int = _DEFAULT_OFFSET_S) -> np.datetime64:
    return _EPOCH_NS + np.timedelta64(slot * _CADENCE_S + offset_s, "s").astype("timedelta64[ns]")


class PreallocateIdempotentIngestor(DirectZarrIngestor):
    """Stub plugin whose ``inspect_item`` output is steered by ``_StubState``.

    The plugin declares a ``RegularTimeAxis(mode="floor", slot_count=12)``
    and returns items whose coordinate is ``epoch + slot*cadence +
    _StubState.offset_s`` seconds. ``discover_source_files`` yields the
    integer slot indices in ``_StubState.slot_indices``; the plugin
    delegates coord materialization to the engine (no ``kind="timestamp"``
    intents), so the observed values arrive at the coord array only
    through ``preallocate``'s engine-managed materialization path.
    """

    PRODUCT_NAME: ClassVar[str] = _STUB_PLUGIN_NAME
    time_dim_name: ClassVar[str] = _COORD_NAME

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name=f"{_STUB_PLUGIN_NAME}_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD_NAME,
                    epoch=_EPOCH_ISO,
                    cadence_s=_CADENCE_S,
                    mode="floor",
                    slot_count=_SLOT_COUNT,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        slot = int(item)
        seconds_since_epoch = slot * _CADENCE_S + _StubState.offset_s
        coord = dt.datetime(2024, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=seconds_since_epoch)
        return ItemInfo(coordinate=coord)

    def discover_source_files(self, ctx: PluginContext) -> list[Any]:
        return list(_StubState.slot_indices)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name=_VALUES_ARRAY,
                        shape=(_SLOT_COUNT, _VALUES_X_DIM),
                        dtype="float32",
                        chunks=(1, _VALUES_X_DIM),
                        fill_value=0.0,
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD_NAME, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.fixture(autouse=True)
def _register_stub_plugin() -> Iterator[None]:
    """Install ``PreallocateIdempotentIngestor`` under a stable name per test."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)

    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    PreallocateIdempotentIngestor.name = _STUB_PLUGIN_NAME  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_STUB_PLUGIN_NAME] = PreallocateIdempotentIngestor
    _StubState.reset()
    try:
        yield
    finally:
        _StubState.reset()
        _loader._LOADED = original_loaded
        _loader.AVAILABLE_INGESTORS.clear()
        _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(
    target: Path,
    source: Path,
    *,
    slot_start: int | None = None,
    slot_end: int | None = None,
) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        _STUB_PLUGIN_NAME,
        "--target",
        f"file://{target}",
        "--product-name",
        _STUB_PLUGIN_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--input-data",
        str(source),
        "--option",
        "no_progress=true",
    ]
    if slot_start is not None:
        args.extend(["--slot-start", str(slot_start)])
    if slot_end is not None:
        args.extend(["--slot-end", str(slot_end)])
    return args


def _make_source_dir(tmp_path: Path) -> Path:
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    return source_dir


def _open_coord_array(target: Path) -> Any:
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    return cast(Any, root[f"{_GROUP}/{_COORD_NAME}"])


def _open_coord_array_rw(target: Path) -> Any:
    root = zarr.open_group(store=str(target), mode="a", zarr_format=3)
    return cast(Any, root[f"{_GROUP}/{_COORD_NAME}"])


def _coord_arrays_equal(a: np.ndarray, b: np.ndarray) -> bool:
    """Byte-level equality for datetime64 arrays including ``NaT`` positions.

    ``np.array_equal`` returns ``False`` whenever either input contains a
    ``NaT`` entry because ``NaT != NaT`` under IEEE-style comparison; the
    NaT-holes contract in this suite demands positional equality across
    identical NaT layouts, so tests compare the raw byte representation
    (equivalent to same shape + same dtype + same underlying int64 slots).
    """
    if a.shape != b.shape or a.dtype != b.dtype:
        return False
    return a.tobytes() == b.tobytes()


def _sha256_coord_chunks(target: Path) -> str:
    """Hash all chunk-data files under the coord array (``c/…``).

    Skips ``zarr.json`` metadata so re-stamping identical attrs on a
    verify-only re-run does not perturb the digest; the plan requires the
    coord *chunk* to be byte-identical, not the whole array directory.
    """
    coord_chunk_root = target / _GROUP / _COORD_NAME / "c"
    h = hashlib.sha256()
    if not coord_chunk_root.exists():
        raise AssertionError(f"expected coord chunk root {coord_chunk_root} to exist")
    files = sorted(p for p in coord_chunk_root.rglob("*") if p.is_file())
    if not files:
        raise AssertionError(f"no coord chunk files found under {coord_chunk_root}")
    for chunk_file in files:
        h.update(chunk_file.relative_to(coord_chunk_root).as_posix().encode("utf-8"))
        h.update(b"\0")
        h.update(chunk_file.read_bytes())
    return h.hexdigest()


def _run_preallocate(
    runner: CliRunner,
    target: Path,
    source: Path,
    *,
    slot_start: int | None = None,
    slot_end: int | None = None,
) -> Any:
    return runner.invoke(
        cli, _preallocate_args(target, source, slot_start=slot_start, slot_end=slot_end)
    )


def test_same_window_idempotent(tmp_path: Path) -> None:
    """Second preallocate run with matching inputs makes zero coord writes.

    First run materializes observed floor values for slots ``[0..6)`` and
    stamps ``firecube_coord_managed``. Second run repeats with the exact
    same window and inputs; the ``_write_observed_regular_coord_values``
    branch reports ``matched == len(observed_values)`` and skips every
    write. The SHA-256 of the coord chunk files is therefore byte-identical
    between runs — a stronger guarantee than "no exception raised" because
    it also detects unnecessary re-writes.
    """
    target = tmp_path / "idempotent.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()
    _StubState.slot_indices = list(range(6))

    first = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert first.exit_code == 0, first.output

    coord_before = _open_coord_array(target)
    values_before = np.asarray(coord_before[:]).copy()
    assert bool(coord_before.attrs.get(ATTR_COORD_MANAGED, False)), (
        f"first run must stamp {ATTR_COORD_MANAGED}: attrs={dict(coord_before.attrs)!r}"
    )
    assert not bool(coord_before.attrs.get(ATTR_PREALLOCATED, False))
    sha_before = _sha256_coord_chunks(target)

    second = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert second.exit_code == 0, second.output
    assert "no-op" in second.output, (
        "second run must emit the observed-coord no-op summary:\n" + second.output
    )

    sha_after = _sha256_coord_chunks(target)
    assert sha_before == sha_after, (
        "coord chunk SHA-256 must be byte-identical between idempotent runs; "
        f"before={sha_before!r} after={sha_after!r}"
    )

    coord_after = _open_coord_array(target)
    values_after = np.asarray(coord_after[:])
    assert _coord_arrays_equal(values_before, values_after)
    for slot in range(6):
        assert values_after[slot] == _off_grid_time_for_slot(slot), (
            f"slot {slot} value {values_after[slot]!r} must match observed floor value"
        )
    for slot in range(6, _SLOT_COUNT):
        assert bool(np.isnat(values_after[slot])), (
            f"slot {slot} outside window must remain NaT: {values_after[slot]!r}"
        )


def test_disjoint_windows_extend(tmp_path: Path) -> None:
    """First run materializes ``[0..6)``, second run extends to ``[6..12)``.

    The stub returns items for every slot in ``[0..12)``; the CLI
    ``--slot-start/--slot-end`` filter drops items outside the current
    window inside ``_discover_regular_observed_coord_values``. After both
    runs the coord array carries observed values across the full range,
    ``firecube_coord_managed`` is still stamped, and ``firecube_preallocated``
    is never introduced.
    """
    target = tmp_path / "disjoint.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()
    _StubState.slot_indices = list(range(_SLOT_COUNT))

    first = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert first.exit_code == 0, first.output
    coord_after_first = _open_coord_array(target)
    values_after_first = np.asarray(coord_after_first[:])
    assert bool(coord_after_first.attrs.get(ATTR_COORD_MANAGED, False))
    assert not bool(coord_after_first.attrs.get(ATTR_PREALLOCATED, False))
    for slot in range(6):
        assert values_after_first[slot] == _off_grid_time_for_slot(slot)
    for slot in range(6, _SLOT_COUNT):
        assert bool(np.isnat(values_after_first[slot]))

    second = _run_preallocate(runner, target, source, slot_start=6, slot_end=_SLOT_COUNT)
    assert second.exit_code == 0, second.output

    coord_after_second = _open_coord_array(target)
    values_after_second = np.asarray(coord_after_second[:])
    assert bool(coord_after_second.attrs.get(ATTR_COORD_MANAGED, False)), (
        "coord_managed marker must survive the disjoint window extension"
    )
    assert not bool(coord_after_second.attrs.get(ATTR_PREALLOCATED, False))
    for slot in range(_SLOT_COUNT):
        assert values_after_second[slot] == _off_grid_time_for_slot(slot), (
            f"slot {slot} value {values_after_second[slot]!r} must match observed value"
        )


def test_conflicting_rerun_raises(tmp_path: Path) -> None:
    """Second run with drifting ``inspect_item`` values raises ``SchemaDriftError``.

    Run 1 stamps the coord array with observed values at ``+2s`` offset.
    Run 2 flips ``_StubState.offset_s`` to ``+137s`` (a coprime stride to
    every plausible grid) so every discovered slot conflicts with the
    stored value. ``_write_observed_regular_coord_values`` raises on the
    first drifting slot; no writes happen before the raise, so the first
    run's values are preserved verbatim.
    """
    target = tmp_path / "conflict.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()
    _StubState.slot_indices = list(range(6))
    _StubState.offset_s = _DEFAULT_OFFSET_S

    first = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert first.exit_code == 0, first.output

    coord_before = _open_coord_array(target)
    values_before = np.asarray(coord_before[:]).copy()

    _StubState.offset_s = 137
    second = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert second.exit_code != 0, (
        "second preallocate with drifting inspect_item output must fail:\n" + second.output
    )
    # Drift is wrapped at the CLI boundary: message rendered, no traceback.
    message = second.output + second.stderr
    assert "Traceback" not in message, message
    assert "slot 0" in message, f"error must name the drifting slot: {message!r}"
    stored_iso = str(values_before[0])
    assert stored_iso in message, (
        f"error must include original stored value ISO {stored_iso!r}: {message!r}"
    )
    conflicting_desired = np.array(
        _off_grid_time_for_slot(0, offset_s=137), dtype="datetime64[ns]"
    )[()]
    conflicting_iso = str(conflicting_desired)
    assert conflicting_iso in message, (
        f"error must include drifting discovered value ISO {conflicting_iso!r}: {message!r}"
    )

    coord_after = _open_coord_array(target)
    values_after = np.asarray(coord_after[:])
    assert _coord_arrays_equal(values_after, values_before), (
        "conflicting re-run must NOT overwrite any slot; "
        f"before={values_before!r}\nafter={values_after!r}"
    )
    assert bool(coord_after.attrs.get(ATTR_COORD_MANAGED, False))


def test_interrupted_reconciles_nat_slots(tmp_path: Path) -> None:
    """A stamped marker plus partial ``NaT`` window entries is recoverable.

    The test seeds a plausible mid-materialization crash: run ``preallocate``
    once with full coverage, then manually reset slots ``[3..6)`` to
    ``NaT`` while leaving ``firecube_coord_managed=True``. Re-running with
    the *same* ``--input-data`` fills only those empty slots while preserving
    matching stored values.
    """
    target = tmp_path / "interrupted.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()
    _StubState.slot_indices = list(range(6))
    _StubState.offset_s = _DEFAULT_OFFSET_S

    first = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert first.exit_code == 0, first.output

    coord_rw = _open_coord_array_rw(target)
    nat = np.array(np.datetime64("NaT", "ns"), dtype="datetime64[ns]")[()]
    for slot in (3, 4, 5):
        coord_rw[slot] = nat
    assert bool(coord_rw.attrs.get(ATTR_COORD_MANAGED, False)), (
        f"seed step must retain {ATTR_COORD_MANAGED}: attrs={dict(coord_rw.attrs)!r}"
    )

    coord_read = _open_coord_array(target)
    seeded_values = np.asarray(coord_read[:])
    assert not bool(np.isnat(seeded_values[0]))
    assert bool(np.isnat(seeded_values[3]))

    second = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert second.exit_code == 0, second.output

    coord_final = _open_coord_array(target)
    final_values = np.asarray(coord_final[:])
    for slot in range(6):
        assert final_values[slot] == _off_grid_time_for_slot(slot)
    assert _coord_arrays_equal(final_values[:3], seeded_values[:3])
    assert bool(np.all(np.isnat(final_values[6:])))
