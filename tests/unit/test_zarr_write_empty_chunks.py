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

from __future__ import annotations

import ast
import contextlib
import threading
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import zarr
from click.testing import CliRunner
from zarr.buffer import default_buffer_prototype
from zarr.codecs import BloscCodec, GzipCodec
from zarr.core.sync import sync
from zarr.storage import LocalStore, MemoryStore

from firecube.cli.main import cli
from firecube.core.zarr.guard.chunk_geometry import unit_bounds
from firecube.core.zarr.guard.guarded_write import (
    WriteFootprint,
    guarded_publish,
    guarded_write,
    hold_array,
    unit_store_key,
)
from firecube.core.zarr.guard.unit_buffer import (
    UnitWriteBuffer,
    describe_array,
    has_lossless_codecs,
)
from firecube.core.zarr.guard.unit_ownership import stored_write_unit
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.runtime.zarr.write_context import ZarrWriteContext
from firecube.ingestor.templates.config import ZarrTemplateConfig

pytestmark = pytest.mark.unit


def test_default_is_false() -> None:
    assert ZarrTemplateConfig().zarr_write_empty_chunks is False


def test_option_flows_through_typed_config() -> None:
    assert (
        ZarrTemplateConfig.from_options({"zarr_write_empty_chunks": "true"}).zarr_write_empty_chunks
        is True
    )


def test_show_options_lists_the_field() -> None:
    result = CliRunner().invoke(
        cli, ["ingest", "direct_zarr_capable_test_plugin", "--show-options"]
    )

    assert result.exit_code == 0, result.output
    assert "--option zarr_write_empty_chunks" in result.output


def test_direct_zarr_dispatch_leaves_process_config_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, bool]] = []

    @contextlib.contextmanager
    def spy_config_set(config: dict[str, bool]):
        calls.append(config)
        yield

    monkeypatch.setattr(zarr.config, "set", spy_config_set)

    result = IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
        group_to_intents={},
        zarr_write_empty_chunks=True,
    )

    assert result["zarr_write_empty_chunks_effective"] is True
    assert [call for call in calls if "array.write_empty_chunks" in call] == []


def test_generic_write_context_applies_scoped_config(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, bool]] = []
    entered = False
    exited = False

    @contextlib.contextmanager
    def spy_config_set(config: dict[str, bool]):
        nonlocal entered, exited
        if "array.write_empty_chunks" in config:
            calls.append(config)
            entered = True
        try:
            yield
        finally:
            if "array.write_empty_chunks" in config:
                exited = True

    monkeypatch.setattr(zarr.config, "set", spy_config_set)

    with ZarrWriteContext(write_empty_chunks=True):
        assert entered is True

    assert {"array.write_empty_chunks": True} in calls
    assert exited is True


def test_array_write_empty_chunks_never_mutated_bare() -> None:
    root = Path("src/firecube")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                parents[child] = parent
        for node in ast.walk(tree):
            if not _is_array_write_empty_chunks_set_call(node):
                continue
            if not _has_with_ancestor(node, parents):
                offenders.append(f"{path}:{getattr(node, 'lineno', '?')}")
    assert offenders == []


def _is_array_write_empty_chunks_set_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    if not isinstance(node.func, ast.Attribute) or node.func.attr != "set":
        return False
    for arg in node.args:
        if isinstance(arg, ast.Dict):
            for key in arg.keys:
                if isinstance(key, ast.Constant) and key.value == "array.write_empty_chunks":
                    return True
    return False


def _has_with_ancestor(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    current: ast.AST | None = node
    while current is not None:
        if isinstance(current, ast.With):
            return True
        current = parents.get(current)
    return False


# guarded_write: write_empty_chunks per array handle, and the units a write leaves absent.

CHUNKED = {"chunks": (2,), "shards": None}
SHARDED = {"chunks": (2,), "shards": (4,)}


def _vector(store: Any, layout: dict[str, Any], *, ones: slice | None = None) -> Any:
    """An 8-cell float array with fill 0, optionally set to 1 over ``ones``, reopened."""
    created = zarr.create_array(
        store, name="data/v", shape=(8,), dtype="f4", fill_value=0.0, zarr_format=3, **layout
    )
    if ones is not None:
        created.with_config({"write_empty_chunks": False})[ones] = 1.0
    return zarr.open_array(store, path="data/v", mode="r+")


def _stored(arr: Any, unit: tuple[int, ...]) -> bool:
    return bool(sync(arr.store.exists(unit_store_key(arr, unit))))


def _fill(arr: Any, cells: slice, *, write_empty_chunks: bool | None = False) -> Any:
    return guarded_write(
        arr,
        WriteFootprint("data/v", ((cells.start, cells.stop),)),
        cells,
        np.zeros(cells.stop - cells.start, dtype="f4"),
        store="test",
        write_empty_chunks=write_empty_chunks,
        track_units=True,
    )


def _fill_together(keep: Any, drop: Any) -> dict[str, Any]:
    """All-fill writes to cells 0:2 of both arrays from two threads released at once."""
    barrier = threading.Barrier(2, timeout=10)
    results: dict[str, Any] = {}
    errors: list[BaseException] = []

    def write(name: str, arr: Any, setting: bool) -> None:
        try:
            barrier.wait()
            results[name] = _fill(arr, slice(0, 2), write_empty_chunks=setting)
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [
        threading.Thread(target=write, args=("keep", keep, True)),
        threading.Thread(target=write, args=("drop", drop, False)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert errors == []
    return results


def test_two_threads_with_different_settings_each_get_their_own() -> None:
    """Each write uses its own setting: not its handle's, not the process-global one."""
    with zarr.config.set({"array.write_empty_chunks": True}):
        global_before = zarr.config.get("array.write_empty_chunks")
        for _ in range(25):
            with zarr.config.set({"array.write_empty_chunks": False}):
                keep = _vector(MemoryStore(), CHUNKED)
            drop = _vector(MemoryStore(), CHUNKED)
            assert (keep.config.write_empty_chunks, drop.config.write_empty_chunks) == (False, True)

            results = _fill_together(keep, drop)

            assert _stored(keep, (0,)) is True
            assert _stored(drop, (0,)) is False
            assert (results["keep"].tracked, results["keep"].absent) == (False, ())
            assert (results["drop"].tracked, results["drop"].absent) == (True, ((0,),))
            assert (keep.config.write_empty_chunks, drop.config.write_empty_chunks) == (False, True)
        assert zarr.config.get("array.write_empty_chunks") is global_before


@pytest.mark.parametrize("store_kind", ["memory", "local"])
@pytest.mark.parametrize(
    ("layout", "ones", "cells", "absent"),
    [
        pytest.param(CHUNKED, slice(0, 8), slice(0, 2), ((0,),), id="chunk-whole-to-fill"),
        pytest.param(CHUNKED, slice(0, 8), slice(0, 1), (), id="chunk-part-to-fill"),
        pytest.param(CHUNKED, None, slice(0, 2), ((0,),), id="chunk-never-stored"),
        pytest.param(SHARDED, slice(0, 8), slice(0, 4), ((0,),), id="shard-whole-to-fill"),
        pytest.param(SHARDED, slice(0, 8), slice(0, 2), (), id="shard-one-inner-of-two"),
        pytest.param(SHARDED, slice(0, 2), slice(0, 2), ((0,),), id="shard-last-inner"),
        pytest.param(SHARDED, slice(0, 2), slice(0, 1), (), id="shard-part-of-last-inner"),
        pytest.param(SHARDED, None, slice(0, 4), ((0,),), id="shard-never-stored"),
    ],
)
def test_guarded_write_reports_absent_units(
    tmp_path: Path,
    store_kind: str,
    layout: dict[str, Any],
    ones: slice | None,
    cells: slice,
    absent: tuple[tuple[int, ...], ...],
) -> None:
    store = MemoryStore() if store_kind == "memory" else LocalStore(tmp_path)
    arr = _vector(store, layout, ones=ones)

    result = _fill(arr, cells)

    assert result.tracked is True
    assert result.units == ((0,),)
    assert result.absent == absent
    assert _stored(arr, (0,)) is (absent == ())
    expected = np.zeros(8, dtype="f4")
    if ones is not None:
        expected[ones] = 1.0
    expected[cells] = 0.0
    np.testing.assert_array_equal(arr[...], expected)


@pytest.mark.parametrize("layout", [CHUNKED, SHARDED], ids=["chunk", "shard"])
def test_guarded_write_keeping_empty_chunks_stores_every_touched_unit(
    layout: dict[str, Any],
) -> None:
    arr = _vector(MemoryStore(), layout, ones=slice(0, 8))

    result = _fill(arr, slice(0, 4), write_empty_chunks=True)

    assert (result.tracked, result.absent) == (False, ())
    assert all(_stored(arr, unit) for unit in result.units)


class _ExistsCountingStore(MemoryStore):
    """A MemoryStore that counts its `exists` calls."""

    exists_calls = 0

    async def exists(self, key: str) -> bool:
        self.exists_calls += 1
        return await super().exists(key)


def test_tracked_write_probes_no_unit_it_set_or_deleted() -> None:
    """Every touched unit is set or deleted by the write itself, so none needs an `exists` probe."""
    store = _ExistsCountingStore()
    arr = _vector(store, CHUNKED, ones=slice(0, 4))
    payload = np.array([0, 0, 1, 1, 0, 0, 1, 1], dtype="f4")
    before = store.exists_calls

    result = guarded_write(
        arr,
        WriteFootprint("data/v", ((0, 8),)),
        slice(0, 8),
        payload,
        store="test",
        write_empty_chunks=False,
        track_units=True,
    )

    probes = store.exists_calls - before
    assert (result.tracked, result.absent) == (True, ((0,), (2,)))
    np.testing.assert_array_equal(arr[...], payload)
    assert probes == 0, (
        f"tracked write made {probes} exists calls for {len(result.units)} units it set or deleted"
    )


@pytest.mark.parametrize(
    "selection",
    [slice(0, 3), slice(None), 2, -1, np.int64(5), (Ellipsis,), Ellipsis],
    ids=["slice", "all", "int", "negative", "numpy-int", "ellipsis-tuple", "ellipsis"],
)
def test_guarded_write_refuses_a_selection_outside_its_footprint(selection: Any) -> None:
    arr = _vector(MemoryStore(), CHUNKED, ones=slice(0, 8))

    with pytest.raises(ValueError, match="outside the footprint"):
        guarded_write(arr, WriteFootprint("data/v", ((0, 2),)), selection, 0.0, store="test")

    np.testing.assert_array_equal(arr[...], np.ones(8, dtype="f4"))


@pytest.mark.parametrize(
    ("selection", "named"),
    [
        (np.array([6]), "ndarray"),
        (np.array([True, False, False, False, False, False, False, False]), "ndarray"),
        ([6], "list"),
        ([True, False, False, False, False, False, False, False], "list"),
        ((np.array([6]),), "ndarray"),
        ((slice(0, 2), [6]), "list"),
        (None, "NoneType"),
        (True, "bool"),
        (np.bool_(True), "bool"),
        ((Ellipsis, Ellipsis), "Ellipsis"),
    ],
    ids=[
        "int-array",
        "bool-mask",
        "list",
        "bool-list",
        "int-array-in-tuple",
        "list-in-tuple",
        "newaxis",
        "bool-scalar",
        "numpy-bool-scalar",
        "two-ellipses",
    ],
)
def test_guarded_write_refuses_other_than_basic_selections_before_locking(
    selection: Any, named: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    arr = _vector(MemoryStore(), CHUNKED, ones=slice(0, 8))

    def no_lock(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a unit lock was taken for an unsupported selection")

    monkeypatch.setattr("firecube.core.zarr.guard.guarded_write._UNIT_LOCKS.hold", no_lock)

    with pytest.raises(ValueError, match="int, slice and Ellipsis selections only") as raised:
        guarded_write(arr, WriteFootprint("data/v", ((0, 2),)), selection, 0.0, store="test")

    assert named in str(raised.value)
    np.testing.assert_array_equal(arr[...], np.ones(8, dtype="f4"))


@pytest.mark.parametrize(
    "selection",
    [slice(0, 2), 1, np.int64(1), (slice(0, 2),), (Ellipsis,), Ellipsis, (1, Ellipsis)],
    ids=["slice", "int", "numpy-int", "tuple", "ellipsis-tuple", "ellipsis", "int-ellipsis"],
)
def test_guarded_write_still_writes_basic_selections(selection: Any) -> None:
    arr = _vector(MemoryStore(), CHUNKED)
    expected = np.zeros(8, dtype="f4")
    expected[selection] = 1.0

    guarded_write(arr, WriteFootprint("data/v", ((0, 8),)), selection, 1.0, store="test")

    np.testing.assert_array_equal(arr[...], expected)


def test_guarded_write_refuses_a_footprint_of_another_array() -> None:
    arr = _vector(MemoryStore(), CHUNKED)

    with pytest.raises(ValueError, match="footprint names 'data/w'"):
        guarded_write(arr, WriteFootprint("data/w", ((0, 2),)), slice(0, 2), 1.0, store="test")


def test_write_footprint_spells_the_array_path_one_way() -> None:
    assert WriteFootprint("/data//v/", ((0, 2),)) == WriteFootprint("data/v", ((0, 2),))


def test_guarded_write_inside_a_hold_of_its_array_reuses_the_hold() -> None:
    arr = _vector(MemoryStore(), CHUNKED)
    done = threading.Event()

    def transaction() -> None:
        with hold_array("test", "data/v") as hold, hold_array("test", "/data/v"):
            guarded_write(
                arr, WriteFootprint("data/v", ((0, 8),)), Ellipsis, 1.0, store="test", hold=hold
            )
            guarded_write(arr, WriteFootprint("data/v", ((0, 2),)), slice(0, 2), 2.0, store="test")
        done.set()

    worker = threading.Thread(target=transaction, daemon=True)
    worker.start()
    worker.join(timeout=10)

    assert done.is_set(), "a write inside hold_array waited on its own hold"
    np.testing.assert_array_equal(arr[...], [2, 2, 1, 1, 1, 1, 1, 1])


def test_guarded_write_of_a_scalar_array_inside_its_hold_reuses_the_hold() -> None:
    """A 0-d array's only unit key is its whole-array key: taking it again would never return."""
    store = MemoryStore()
    zarr.create_array(store, name="data/s", shape=(), dtype="f4", fill_value=0.0, zarr_format=3)
    scalar = zarr.open_array(store, path="data/s", mode="r+")
    done = threading.Event()

    def transaction() -> None:
        with hold_array("test", "data/s"):
            guarded_write(scalar, WriteFootprint("data/s", ()), (), 3.0, store="test")
        done.set()

    worker = threading.Thread(target=transaction, daemon=True)
    worker.start()
    worker.join(timeout=10)

    assert done.is_set(), "a write inside hold_array waited on its own hold"
    np.testing.assert_array_equal(scalar[()], 3.0)


def test_guarded_write_refuses_a_hold_of_another_array() -> None:
    arr = _vector(MemoryStore(), CHUNKED)

    with hold_array("test", "data/w") as other, pytest.raises(ValueError, match="not held"):
        guarded_write(
            arr, WriteFootprint("data/v", ((0, 2),)), slice(0, 2), 1.0, store="test", hold=other
        )


@pytest.mark.parametrize(
    ("layout", "footprint", "selection", "absent"),
    [
        pytest.param(
            CHUNKED, (0, 13), slice(0, 13, 12), ((1,), (2,), (3,), (4,), (5,)), id="chunk-stepped"
        ),
        pytest.param(SHARDED, (0, 13), slice(0, 13, 12), ((1,), (2,)), id="shard-stepped"),
        pytest.param(CHUNKED, (0, 8), slice(0, 2), ((1,), (2,), (3,)), id="chunk-wider-footprint"),
        pytest.param(SHARDED, (0, 16), slice(0, 4), ((1,), (2,), (3,)), id="shard-wider-footprint"),
    ],
)
def test_keeping_empty_chunks_still_reports_units_the_write_did_not_reach(
    layout: dict[str, Any],
    footprint: tuple[int, int],
    selection: slice,
    absent: tuple[tuple[int, ...], ...],
) -> None:
    """Kept empty chunks rule out deletion, not absence: unselected units of the box stay absent."""
    zarr.create_array(
        store := MemoryStore(),
        name="data/v",
        shape=(16,),
        dtype="f4",
        fill_value=0.0,
        zarr_format=3,
        **layout,
    )
    arr = zarr.open_array(store, path="data/v", mode="r+")
    cells = len(range(*selection.indices(16)))

    result = guarded_write(
        arr,
        WriteFootprint("data/v", (footprint,)),
        selection,
        np.ones(cells, dtype="f4"),
        store="test",
        write_empty_chunks=True,
        track_units=True,
    )

    assert result.tracked is True
    assert result.absent == absent
    assert result.absent == tuple(unit for unit in result.units if not _stored(arr, unit))


UNIT0, EDGE = ((0, 4),), ((4, 7),)


def _publish(arr: Any, box: Any, payload: bytes | None, **kwargs: Any) -> Any:
    """Import `guarded_publish` per call, so a missing function fails each test alone."""
    from firecube.core.zarr.guard.guarded_write import guarded_publish

    return guarded_publish(arr, WriteFootprint("data/v", box), payload, store="t", **kwargs)


def _seven(store: Any, value: float | None = None, box: Any = ((0, 7),)) -> Any:
    layout = {"shape": (7,), "dtype": "f4", "fill_value": 0.0, "zarr_format": 3, **SHARDED}
    zarr.create_array(store, name="data/v", **layout)
    arr = zarr.open_array(store, path="data/v", mode="r+")
    if value is not None:
        footprint = WriteFootprint("data/v", box)
        guarded_write(arr, footprint, slice(*box[0]), value, store="t", write_empty_chunks=True)
    return arr


def _object(arr: Any, unit: tuple[int, ...]) -> bytes:
    return sync(arr.store.get(unit_store_key(arr, unit), default_buffer_prototype())).to_bytes()


@pytest.mark.parametrize(("unit", "box"), [((0,), UNIT0), ((1,), EDGE)], ids=["sharded", "edge"])
def test_published_bytes_equal_the_object_guarded_write_stores(unit: Any, box: Any) -> None:
    written = _seven(MemoryStore(), 3.0, box)
    arr = _seven(MemoryStore())

    _publish(arr, box, _object(written, unit), write_empty_chunks=False)

    assert _object(arr, unit) == _object(written, unit)


@pytest.mark.parametrize("stored", [True, False], ids=["existing-object", "never-stored"])
def test_publishing_none_removes_the_object_and_reports_the_unit_absent(stored: bool) -> None:
    arr = _seven(MemoryStore(), 1.0 if stored else None)
    result = _publish(arr, UNIT0, None, write_empty_chunks=False, track_units=True)
    assert (result.absent, result.tracked) == (((0,),), True)
    assert (_stored(arr, (0,)), _stored(arr, (1,))) == (False, stored)


@pytest.mark.parametrize(("keep", "tracked"), [(False, True), (True, False)])
def test_publish_tracks_a_set_unit_as_present_without_probing(keep: bool, tracked: bool) -> None:
    store = _ExistsCountingStore()
    payload = _object(_seven(MemoryStore(), 3.0, UNIT0), (0,))
    arr = _seven(store)
    result = _publish(arr, UNIT0, payload, write_empty_chunks=keep, track_units=True)
    assert store.exists_calls == 0
    assert (result.tracked, result.absent, _stored(arr, (0,))) == (tracked, (), True)


@pytest.mark.parametrize(
    ("box", "grown"),
    [(((0, 2),), False), (((0, 8),), False), (((4, 8),), False), (EDGE, True)],
    ids=["part-of-unit", "two-units", "unclipped-edge", "array-grew-after-read"],
)
def test_publish_refuses_a_footprint_that_is_not_one_clipped_unit(box: Any, grown: bool) -> None:
    store = MemoryStore()
    arr = _seven(store, 1.0)
    reopen = lambda: zarr.open_array(store, path="data/v", mode="r+")  # noqa: E731
    if grown:
        reopen().resize((8,))
    before = dict(store._store_dict)

    with pytest.raises(
        ValueError, match=r"footprint box .* (is not unit|touches \d+ stored units|does not fit)"
    ):
        _publish(arr, box, None, **({"reopen": reopen} if grown else {}))

    assert store._store_dict == before


def test_publish_runs_on_result_while_the_unit_lock_is_held() -> None:
    arr = _seven(MemoryStore())
    events: list[str] = []
    attempting = threading.Event()

    def rival_write() -> None:
        attempting.set()
        guarded_write(arr, WriteFootprint("data/v", UNIT0), slice(0, 4), 1.0, store="t")
        events.append("rival write done")

    rival = threading.Thread(target=rival_write, daemon=True)

    def on_result(_result: Any) -> None:
        rival.start()
        assert attempting.wait(timeout=10)
        rival.join(timeout=0.3)
        assert rival.is_alive(), "a write to the same unit ran inside on_result"
        events.append("on_result returned")

    _publish(arr, UNIT0, None, on_result=on_result)
    rival.join(timeout=10)

    assert events == ["on_result returned", "rival write done"]


@pytest.mark.parametrize(("keep", "stored"), [(False, False), (True, True)])
def test_publishing_all_fill_bytes_stores_them_only_when_empty_chunks_are_kept(
    keep: bool, stored: bool
) -> None:
    payload = _object(_seven(MemoryStore(), 0.0, UNIT0), (0,))
    arr = _seven(MemoryStore(), 1.0)
    _publish(arr, UNIT0, payload, write_empty_chunks=keep)
    assert _stored(arr, (0,)) is stored


# UnitWriteBuffer, fed by a sink a second feeder would build over the same two writers.

NB = np.uint32(0x7FC00001).view("f4")  # a NaN whose bits are not numpy's canonical NaN
U0, U1 = "data/v/c/0/0/0", "data/v/c/0/1/0"
# fmt: off
BASE: dict[str, Any] = {"shape": (1, 8, 4), "chunks": (1, 2, 2), "shards": (1, 4, 4), "fill": 0.0,
                        "keep": False, "init": None, "pieces": [], "ops": {}, "absent": None, "events": None}


class _LogStore(MemoryStore):
    """A MemoryStore that logs its sets and deletes as (operation, key)."""
    def __init__(self) -> None:
        super().__init__()
        self.log: list[tuple[str, str]] = []
    async def set(self, key: str, value: Any, byte_range: Any = None) -> None:
        self.log.append(("set", key))
        await super().set(key, value, byte_range)
    async def delete(self, key: str) -> None:
        self.log.append(("delete", key))
        await super().delete(key)


class _Sink:
    """Whole units go through guarded_publish, boxes through guarded_write."""
    def __init__(self, arrays: dict[str, Any], keep: bool = False) -> None:
        self.arrays, self.keep, self.absent, self.events = arrays, keep, [], []
        self.kw = {"store": "t", "write_empty_chunks": keep, "track_units": True,
                   "on_result": lambda r: self.absent.extend(r.absent)}
    def describe(self, key: str) -> Any:
        return describe_array(self.arrays[key], self.keep)
    def publish(self, key: str, unit: Any, payload: bytes | None) -> None:
        arr = self.arrays[key]
        box = unit_bounds(unit, stored_write_unit(arr)[0], arr.shape)
        self.events.append(("publish", key, unit))
        guarded_publish(arr, WriteFootprint(f"data/{key}", box), payload, **self.kw)
    def write_box(self, key: str, box: Any, data: Any) -> None:
        sel = tuple(slice(*b) for b in box)
        self.events.append(("box", key, box))
        guarded_write(self.arrays[key], WriteFootprint(f"data/{key}", box), sel, data, **self.kw)


def _cube(store: Any, name: str = "v", **r: Any) -> Any:
    r = {**BASE, **r}
    arr = zarr.create_array(store, name=f"data/{name}", shape=r["shape"], dtype="f4", zarr_format=3,
                            chunks=r["chunks"], shards=r["shards"], fill_value=r["fill"])
    if r["init"] is not None:
        whole = WriteFootprint(f"data/{name}", tuple((0, n) for n in r["shape"]))
        guarded_write(arr, whole, ..., np.full(r["shape"], r["init"], "f4"), store="t", write_empty_chunks=True)
    return arr
def _cells(box: Any, value: Any) -> Any:
    shape = tuple(b - a for a, b in box)
    return (np.arange(np.prod(shape)) + 1).reshape(shape).astype("f4") if value == "iota" else np.full(shape, value, "f4")
def _piece(rows: tuple[int, int], value: Any, cols: tuple[int, int] = (0, 4)) -> Any:
    return ((0, 1), rows, cols), value
def _box(rows: tuple[int, int], cols: tuple[int, int] = (0, 4)) -> Any:
    return "box", "v", ((0, 1), rows, cols)
def _objects(arr: Any) -> dict[str, bytes]:
    return {k: v.to_bytes() for k, v in arr.store._store_dict.items() if not k.endswith("zarr.json")}


_R5 = {"shape": (1, 2, 4, 6, 4), "chunks": (1, 1, 2, 2, 2), "shards": (1, 1, 2, 4, 2)}
_CLIP = {"shape": (1, 3, 3), "shards": None, "fill": NB, "pieces": [_piece((0, 3), np.nan, (0, 2)), _piece((0, 3), 1.0, (2, 3))]}
_NAN = {"fill": np.nan, "init": 5.0}
_ALL0 = [_piece((0, 4), 0.0)]


@pytest.mark.parametrize(
    "row",
    [
        pytest.param({"pieces": [_piece((0, 2), 1.0), _piece((2, 4), 2.0)], "ops": {U0: ["set"]}}, id="complete-sharded-unit-from-axis-1-pieces"),
        pytest.param({"shape": (1, 6, 4), "pieces": [_piece((4, 5), 1.0), _piece((5, 6), 2.0)], "ops": {U1: ["set"]}}, id="clipped-edge-unit"),
        pytest.param({"init": 5.0, "pieces": _ALL0, "ops": {U0: ["delete"]}, "absent": [(0, 0, 0)]}, id="all-fill-unit-deletes-existing-object"),
        pytest.param({"pieces": _ALL0, "ops": {U0: []}, "absent": [(0, 0, 0)]}, id="all-fill-unit-without-object-writes-nothing"),
        pytest.param({"keep": True, "pieces": _ALL0, "ops": {U0: ["set"]}, "absent": []}, id="all-fill-unit-is-stored-when-empty-chunks-are-kept"),
        pytest.param({"fill": None, "init": 5.0, "pieces": _ALL0, "ops": {U0: ["delete"]}, "absent": [(0, 0, 0)]}, id="fill-none-follows-zarr-default"),
        pytest.param({"init": 5.0, "pieces": [_piece((0, 2), 1.0), _piece((3, 4), 2.0)]}, id="unit-missing-a-piece-goes-out-at-close"),
        pytest.param({"pieces": [_piece((0, 3), 1.0), _piece((2, 4), 2.0), _piece((0, 4), 3.0)],
                      "events": [("put", 0), ("put", 1), _box((0, 3)), _box((2, 4)), ("put", 2), _box((0, 4))]},
                     id="overlap-flushes-at-once-and-taints-the-unit"),
        pytest.param({"shape": (1, 4, 8), "init": 5.0, "pieces": [_piece((0, 2), 1.0, (2, 6)), _piece((2, 3), 2.0, (2, 6))],
                      "events": [("put", 0), ("put", 1), _box((0, 2), (2, 4)), _box((0, 2), (4, 6)), _box((2, 3), (2, 4)), _box((2, 3), (4, 6))]},
                     id="band-spanning-two-units-on-axis-2-missing-a-piece-flushes-in-arrival-order"),
        pytest.param({**_R5, "pieces": [(((0, 1), (0, 1), (0, 2), (1, 6), (0, 2)), "iota"), (((0, 1), (0, 2), (1, 4), (2, 5), (0, 2)), "iota")]}, id="rank-5-overlapping-pieces-spanning-units-on-axis-3"),
        pytest.param({**_NAN, "pieces": [_piece((0, 1), NB, (0, 2))]}, id="noncanonical-nan-piece-against-nan-fill-partial-flush"),
        pytest.param({**_NAN, "pieces": [_piece((0, 1), NB), _piece((1, 4), 1.0)]}, id="noncanonical-nan-piece-against-nan-fill-complete-publish"),
        pytest.param(_CLIP, id="noncanonical-declared-fill-clipped-edge-unsharded"),
        pytest.param({**_CLIP, "shards": (1, 4, 4)}, id="noncanonical-declared-fill-clipped-edge-sharded"),
    ],
)
def test_buffered_pieces_store_what_plain_guarded_writes_store(row: dict[str, Any]) -> None:
    r = {**BASE, **row}
    store, ref = _LogStore(), _cube(MemoryStore(), **r)
    arr = _cube(store, **r)
    store.log.clear()
    sink = _Sink({"v": arr}, r["keep"])
    buffer = UnitWriteBuffer(sink)
    for i, (box, value) in enumerate(r["pieces"]):
        sink.events.append(("put", i))
        cells, plain = _cells(box, value), WriteFootprint("data/v", box)
        guarded_write(ref, plain, tuple(slice(*b) for b in box), cells, store="t", write_empty_chunks=r["keep"])
        buffer.put("v", box, cells, None)
    buffer.close()

    assert _objects(arr) == _objects(ref)
    assert np.asarray(arr[...]).tobytes() == np.asarray(ref[...]).tobytes()
    for key, ops in r["ops"].items():
        assert [op for op, logged in store.log if logged == key] == ops
    if r["absent"] is not None:
        assert sink.absent == r["absent"]
    if r["events"] is not None:
        assert sink.events == r["events"]


def test_token_is_reported_once_after_its_units_reached_the_destination_and_never_when_discarded() -> None:
    sink = _Sink({"v": _cube(MemoryStore()), "w": _cube(MemoryStore(), "w")})
    buffer = UnitWriteBuffer(sink, on_landed=lambda token: sink.events.append(("landed", token)))
    for key, rows, token in [("v", (2, 6), "spans"), ("v", (0, 2), "first"), ("v", (6, 8), "last"),
                             ("w", (0, 2), "flushed"), ("w", (0, 2), "closed")]:
        box = _piece(rows, 1.0)[0]
        buffer.put(key, box, _cells(box, 1.0), token)
        if token == "flushed":
            buffer.flush("w")
    buffer.close()
    discarded: list[object] = []
    box = _piece((0, 2), 1.0)[0]
    UnitWriteBuffer(_Sink({"v": _cube(MemoryStore())}), on_landed=discarded.append).put(
        "v", box, _cells(box, 1.0), "never"
    )

    wbox = ("box", "w", ((0, 1), (0, 2), (0, 4)))
    assert (sink.events, discarded) == ([
        ("publish", "v", (0, 0, 0)), ("landed", "first"), ("publish", "v", (0, 1, 0)),
        ("landed", "spans"), ("landed", "last"), wbox, ("landed", "flushed"), wbox, ("landed", "closed"),
    ], [])


def test_flush_of_one_array_leaves_the_open_units_of_other_arrays_open() -> None:
    a, b = _cube(MemoryStore(), "a"), _cube(MemoryStore(), "b")
    buffer, box = UnitWriteBuffer(_Sink({"a": a, "b": b})), _piece((0, 2), 1.0)[0]
    for key in "ab":
        buffer.put(key, box, _cells(box, 1.0), None)

    buffer.flush("a")
    assert (bool(_objects(a)), _objects(b)) == (True, {})
    buffer.close()

    assert bool(_objects(b)) is True


@pytest.mark.parametrize("codec", [BloscCodec(), GzipCodec()])
def test_codec_outside_the_lossless_list_is_refused_before_a_piece_is_held(codec: Any) -> None:
    kwargs: dict[str, Any] = {"shape": (4,), "dtype": "f4", "chunks": (2,), "zarr_format": 3}
    plain = zarr.create_array(MemoryStore(), name="data/v", **kwargs)
    arr = zarr.create_array(MemoryStore(), name="data/v", compressors=codec, **kwargs)
    assert (has_lossless_codecs(plain.metadata), has_lossless_codecs(arr.metadata)) == (True, False)

    with pytest.raises(ValueError, match="lossless"):
        UnitWriteBuffer(_Sink({"v": arr})).put("v", ((0, 2),), np.ones(2, "f4"), None)


def test_piece_of_another_shape_than_its_box_is_refused_and_nothing_is_held() -> None:
    store, landed = _LogStore(), []
    arr = _cube(store)
    store.log.clear()
    buffer = UnitWriteBuffer(_Sink({"v": arr}), on_landed=landed.append)

    with pytest.raises(ValueError, match="shape"):
        buffer.put("v", ((0, 1), (0, 2), (0, 4)), np.ones((1, 3, 4), "f4"), "token")
    buffer.close()

    assert (store.log, landed) == ([], [])


def test_publish_inside_a_hold_of_its_array_completes_and_stores_the_unit() -> None:
    payload = _object(_seven(MemoryStore(), 3.0, UNIT0), (0,))
    arr = _seven(MemoryStore())
    done = threading.Event()

    def transaction() -> None:
        with hold_array("t", "data/v"):
            _publish(arr, UNIT0, payload, write_empty_chunks=False)
        done.set()

    threading.Thread(target=transaction, daemon=True).start()

    assert done.wait(timeout=10), "a publish inside hold_array waited on its own hold"
    assert _object(arr, (0,)) == payload
