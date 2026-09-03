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

"""Recovery gates for ``firecube zarr preallocate`` observed-coord materialization.

Complements the idempotency scenarios locked in
``test_preallocate_idempotent.py`` with the failure modes that
appear only during real crash-and-resume incidents:

* **Crash after marker before writes**: the marker is stamped and the
  window is all ``NaT``. A blind observer cannot distinguish this from a
  fresh-window start; the resume must therefore fill and finish, not
  abort.
* **Marker missing, coord fully written**: the coord array was written
  by a pre-marker release. The resume must NOT silently overwrite the
  existing data; it must either upgrade the store to the marker regime
  when the discovered values are consistent, or fail loudly when they
  drift.
* **Coord chunk corrupted**: a chunk file was replaced with garbage.
  Preallocate must refuse to continue instead of auto-healing.

The stub plugin machinery mirrors ``test_preallocate_idempotent.py`` but
uses a distinct plugin name so both suites can run in the same session
without registry collisions.
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


_STUB_PLUGIN_NAME = "preallocate_recovery_test_plugin"
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


class PreallocateRecoveryIngestor(DirectZarrIngestor):
    """Recovery-suite stub plugin steered by module-level ``_StubState``.

    Mirrors the idempotency-suite plugin in ``test_preallocate_idempotent``
    but under a distinct plugin name so both suites can register their
    stubs simultaneously without stepping on each other.
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
    """Install ``PreallocateRecoveryIngestor`` under a stable name per test."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)

    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    PreallocateRecoveryIngestor.name = _STUB_PLUGIN_NAME  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_STUB_PLUGIN_NAME] = PreallocateRecoveryIngestor
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


def _coord_chunk_files(target: Path) -> list[Path]:
    """Return every chunk-data file under the coord array (``c/…``)."""
    coord_chunk_root = target / _GROUP / _COORD_NAME / "c"
    if not coord_chunk_root.exists():
        raise AssertionError(f"expected coord chunk root {coord_chunk_root} to exist")
    return sorted(p for p in coord_chunk_root.rglob("*") if p.is_file())


def _coord_chunk_bytes(target: Path) -> dict[str, bytes]:
    """Return path→bytes for every chunk under the coord array."""
    coord_chunk_root = target / _GROUP / _COORD_NAME / "c"
    return {
        p.relative_to(coord_chunk_root).as_posix(): p.read_bytes()
        for p in _coord_chunk_files(target)
    }


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


def test_crash_after_marker_before_writes(tmp_path: Path) -> None:
    """Marker stamped + all-NaT window resumes and finishes successfully.

    Simulates the narrow crash window in ``_materialize_regular_coord_array``
    between attribute-stamp and observed-value writes: the marker is
    present but every slot in the window is still ``NaT``. Stored data
    cannot distinguish this from a genuine start-of-window run, and it
    does not need to: per-slot reconciliation fills every NaT slot with
    the incoming value, so the resume completes the window either way.

    The seed uses the normal preallocate path then wipes the covered
    slots back to NaT (``firecube_coord_managed`` retained) so the on-disk
    state is byte-equivalent to what the crash would leave behind.
    """
    target = tmp_path / "crash-after-marker.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()
    _StubState.slot_indices = list(range(6))

    seed = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert seed.exit_code == 0, seed.output

    coord_rw = _open_coord_array_rw(target)
    assert bool(coord_rw.attrs.get(ATTR_COORD_MANAGED, False)), (
        f"seed run must stamp {ATTR_COORD_MANAGED}: attrs={dict(coord_rw.attrs)!r}"
    )
    nat = np.array(np.datetime64("NaT", "ns"), dtype="datetime64[ns]")[()]
    for slot in range(6):
        coord_rw[slot] = nat

    coord_before = _open_coord_array(target)
    values_before = np.asarray(coord_before[:])
    assert bool(coord_before.attrs.get(ATTR_COORD_MANAGED, False)), (
        "marker must survive the manual NaT reset (this is the whole point of the seed)"
    )
    for slot in range(6):
        assert bool(np.isnat(values_before[slot])), (
            f"seed must leave slot {slot} in NaT state: {values_before[slot]!r}"
        )

    resume = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert resume.exit_code == 0, (
        "marker-stamped + all-NaT window must resume cleanly (fresh-start equivalent):\n"
        + resume.output
    )

    coord_after = _open_coord_array(target)
    values_after = np.asarray(coord_after[:])
    assert bool(coord_after.attrs.get(ATTR_COORD_MANAGED, False)), "resume must keep marker stamped"
    assert not bool(coord_after.attrs.get(ATTR_PREALLOCATED, False))
    for slot in range(6):
        expected = _off_grid_time_for_slot(slot)
        assert values_after[slot] == expected, (
            f"slot {slot} value {values_after[slot]!r} must equal observed value {expected!r}"
        )
    for slot in range(6, _SLOT_COUNT):
        assert bool(np.isnat(values_after[slot])), (
            f"slot {slot} outside window must remain NaT: {values_after[slot]!r}"
        )


def test_marker_missing_coord_full(tmp_path: Path) -> None:
    """Marker missing + coord fully written is a legacy shell and refuses."""
    target = tmp_path / "marker-missing.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()
    _StubState.slot_indices = list(range(6))

    seed = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert seed.exit_code == 0, seed.output

    coord_rw = _open_coord_array_rw(target)
    assert bool(coord_rw.attrs.get(ATTR_COORD_MANAGED, False)), (
        f"seed run must stamp {ATTR_COORD_MANAGED}: attrs={dict(coord_rw.attrs)!r}"
    )
    del coord_rw.attrs[ATTR_COORD_MANAGED]

    coord_before = _open_coord_array(target)
    assert not bool(coord_before.attrs.get(ATTR_COORD_MANAGED, False)), (
        "seed must leave the store without a marker to simulate the legacy case"
    )
    assert not bool(coord_before.attrs.get(ATTR_PREALLOCATED, False))
    values_before = np.asarray(coord_before[:]).copy()
    for slot in range(6):
        assert values_before[slot] == _off_grid_time_for_slot(slot), (
            f"seed must leave slot {slot} filled with the observed value: {values_before[slot]!r}"
        )
    bytes_before = _coord_chunk_bytes(target)

    resume = _run_preallocate(runner, target, source, slot_start=0, slot_end=6)
    assert resume.exit_code != 0, (
        "marker-less observed shell must refuse as legacy classification:\n" + resume.output
    )
    # The refusal is wrapped at the CLI boundary: message rendered, no traceback.
    message = resume.output + resume.stderr
    assert "Traceback" not in message, message
    assert "legacy" in message
    assert "firecube chunks" in message

    bytes_after = _coord_chunk_bytes(target)
    assert bytes_before == bytes_after, (
        "legacy refusal must NOT overwrite any coord chunk bytes; "
        f"before-keys={sorted(bytes_before)} after-keys={sorted(bytes_after)}"
    )

    coord_after = _open_coord_array(target)
    values_after = np.asarray(coord_after[:])
    assert not bool(coord_after.attrs.get(ATTR_COORD_MANAGED, False)), (
        "legacy refusal must not retroactively stamp firecube_coord_managed"
    )
    assert not bool(coord_after.attrs.get(ATTR_PREALLOCATED, False))
    assert values_before.tobytes() == values_after.tobytes(), (
        "resume must leave every coord value byte-identical; "
        f"before={values_before!r} after={values_after!r}"
    )


def test_coord_chunk_corrupted(tmp_path: Path) -> None:
    """A corrupted coord chunk file makes preallocate fail loudly, not auto-heal.

    Seeds a healthy target then replaces every coord chunk file with
    random bytes. The resume path must refuse to overwrite the corrupted
    bytes silently: either the codec pipeline raises on decode or the
    (nonsense) decoded value trips the drift check in
    ``_write_observed_regular_coord_values``. Either outcome carries a
    non-zero exit and an actionable exception; the operator remediates
    the corruption (e.g. restore from backup) before re-running.
    """
    target = tmp_path / "corrupted-chunk.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()
    _StubState.slot_indices = list(range(_SLOT_COUNT))

    seed = _run_preallocate(runner, target, source, slot_start=0, slot_end=_SLOT_COUNT)
    assert seed.exit_code == 0, seed.output

    chunk_files = _coord_chunk_files(target)
    assert chunk_files, "seed run must produce at least one coord chunk file"
    corruption = b"\x00\xff\x00\xff not a valid chunk payload " * 4
    for chunk_file in chunk_files:
        chunk_file.write_bytes(corruption)

    resume = _run_preallocate(runner, target, source, slot_start=0, slot_end=_SLOT_COUNT)
    assert resume.exit_code != 0, (
        "resume against a corrupted coord chunk must fail loudly, not auto-heal:\n" + resume.output
    )
    exc = resume.exception
    assert exc is not None, (
        f"resume must surface an exception, not just a non-zero exit; output={resume.output!r}"
    )
    combined = f"{type(exc).__name__}: {exc}\n{resume.output}"
    coord_path = f"{_GROUP}/{_COORD_NAME}"
    assert (
        coord_path in combined
        or "chunk" in combined.lower()
        or "decode" in combined.lower()
        or "decompression" in combined.lower()
    ), (
        "error must name the coord path, a chunk/decode/decompression failure so the operator "
        f"can locate the corruption: {combined!r}"
    )
