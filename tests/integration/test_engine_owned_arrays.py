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


import sys
from datetime import UTC, datetime, timedelta
from types import FrameType, SimpleNamespace
from typing import Any, ClassVar

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.zarr.guard.engine_owned import (
    materialize_engine_owned_array,
    require_engine_owned_slots,
)
from firecube.core.zarr.guard.metadata_authority import MetadataAuthority, MetadataScope
from firecube.ingestor.registry import loader
from firecube.sdk import (
    DirectZarrIngestor,
    IndexedWrite,
    IndexSpec,
    IntegerAxis,
    ItemInfo,
    RegularTimeAxis,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def budget_plugin(monkeypatch):
    state = SimpleNamespace(
        items=[0, 1, 2, 3],
        owned=False,
        ownership=True,
        axis="integer",
        unit=None,
        shards=None,
        pod_offset=0,
        bias=0,
        static=False,
        missing=False,
        emit_owned=True,
        owned_only=False,
    )
    epoch = datetime(2024, 1, 1, tzinfo=UTC)

    def coordinate(slot):
        if state.axis == "integer":
            return slot
        return epoch + timedelta(seconds=60 * slot)

    class BudgetPlugin(DirectZarrIngestor):
        PRODUCT_NAME: ClassVar[str] = "budget"
        time_dim_name: ClassVar[str] = "time"

        def discover_source_files(self, ctx):
            return state.items

        def index_spec(self, ctx):
            if state.axis == "integer":
                axis = IntegerAxis(slot_count=4)
            else:
                axis = RegularTimeAxis(
                    coordinate="time",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=60,
                    slot_count=4,
                    mode="exact",
                )
            return IndexSpec(name="budget_v1", groups={"data": axis})

        def inspect_item(self, item, ctx):
            values = {"owned": 100.0 + item} if state.owned else None
            if state.missing:
                values = {}
            return ItemInfo(coordinate=coordinate(item), values=values)

        def zarr_schema(self, ctx):
            def array(name, shape, dtype="float32", **kwargs):
                return ZarrArraySpec(name=name, shape=shape, dtype=dtype, **kwargs)

            arrays = []
            for name, shape in [("vec", ()), ("img", (2, 2))]:
                chunks = None if state.unit is None else (state.unit, *shape)
                shards = None if state.shards is None else (state.shards, *shape)
                arrays.append(
                    array(name, (4, *shape), chunks=chunks, shards=shards, fill_value=np.nan)
                )
            if state.owned:
                arrays.append(
                    array("owned", (4,), "float64", fill_value=np.nan, engine_owned=state.ownership)
                )
            if state.static:
                arrays.append(array("lat", (2,), "float64", time_indexed=False))
            if state.axis != "integer":
                arrays.append(
                    array("time", (4,), "datetime64[ns]", fill_value=np.datetime64("NaT", "ns"))
                )
            return [ZarrGroupSpec(group="data", arrays=arrays)]

        def build_write_intents(self, batch, ctx):
            def slot(name, i, data):
                return IndexedWrite.slot(
                    group="data", array=name, coordinate=coordinate(i), data=data
                )

            intents = [
                slot(name, i, np.full(shape, i + 1.0 + state.bias, dtype="float32"))
                for i in batch.items
                for name, shape in [("vec", ()), ("img", (2, 2))]
                if not state.owned_only
            ]
            if state.owned and state.emit_owned:
                intents += [
                    slot("owned", i, np.asarray(100.0 + i + state.pod_offset)) for i in batch.items
                ]
            if state.static:
                intents += [
                    WriteIntent.static(group="data", array="lat", data=np.full(2, float(i)))
                    for i in batch.items
                ]
            return intents

    BudgetPlugin.name = "budget"
    monkeypatch.setattr(loader, "_LOADED", True)
    monkeypatch.setattr(loader, "AVAILABLE_INGESTORS", {"budget": BudgetPlugin})
    return state


def cli_run(target, command="ingest", mode="direct", *extra):
    common = ["budget", "--product-name", "cube.zarr", "--option", "no_progress=true"]
    args = [*command.split(), *common, "--target", target.as_uri(), "--write-mode", mode, *extra]
    return CliRunner().invoke(cli, args)


def payload_snapshot(target):
    return {
        p.relative_to(target).as_posix(): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in target.rglob("*")
        if p.is_file() and ".firecube" not in p.relative_to(target).parts
    }


def root(target) -> Any:
    return zarr.open_group(str(target), mode="r", use_consolidated=False)


@pytest.mark.parametrize("axis", ["integer", "grid"])
@pytest.mark.parametrize("mode", ["direct", "staged"])
def test_preallocate_then_pods_preserve_engine_arrays_and_layout(
    tmp_path, budget_plugin, axis, mode
):
    budget_plugin.owned, budget_plugin.axis = True, axis
    budget_plugin.items = [0, 1, 3]
    target = tmp_path / "cube.zarr"
    result = cli_run(target, "zarr preallocate", "direct", "--input-data", tmp_path.as_uri())
    assert result.exit_code == 0, result.output
    r = root(target)
    np.testing.assert_array_equal(r["data/owned"][:], [100, 101, np.nan, 103])
    assert r["data/owned"].chunks == (4,)
    assert r["data/owned"].attrs["firecube_engine_owned_complete"] is True
    assert r["data/vec"].chunks == (1,)
    assert r["data/img"].chunks == (1, 2, 2)
    watched = target / "data" / "owned"
    before = payload_snapshot(watched)
    result = cli_run(target, "zarr preallocate", "direct", "--input-data", tmp_path.as_uri())
    assert result.exit_code == 0, result.output
    assert payload_snapshot(watched) == before
    for lo, hi in [(0, 2), (2, 4)]:
        result = cli_run(target, "ingest", mode, "--slot-start", str(lo), "--slot-end", str(hi))
        assert result.exit_code == 0, result.output
    np.testing.assert_array_equal(root(target)["data/vec"][:], [1, 2, np.nan, 4])
    np.testing.assert_array_equal(root(target)["data/img"][:, 0, 0], [1, 2, np.nan, 4])
    assert payload_snapshot(watched) == before


@pytest.mark.parametrize(
    "case,message",
    [
        pytest.param("no-input", "--input-data", id="no-input"),
        pytest.param("missing-value", "no value", id="missing-value"),
        pytest.param("unmaterialized", "preallocate", id="unmaterialized"),
        pytest.param("mismatch", "diverged", id="mismatch"),
        pytest.param("ownership-dropped", "ownership", id="ownership-dropped"),
        pytest.param("late-item", "holds no value", id="late-item"),
        pytest.param("incomplete", "preallocate", id="incomplete"),
    ],
)
def test_engine_owned_refusals_leave_payload_untouched(tmp_path, budget_plugin, case, message):
    budget_plugin.owned = True
    target = tmp_path / "cube.zarr"
    if case in ("no-input", "missing-value", "unmaterialized"):
        budget_plugin.missing = case == "missing-value"
        extra = [] if case == "no-input" else ["--input-data", tmp_path.as_uri()]
        command = "ingest" if case == "unmaterialized" else "zarr preallocate"
        result = cli_run(target, command, "direct", *extra)
        assert result.exit_code != 0
        assert message in result.output
        assert not (target / "data").exists()
        return
    if case == "late-item":
        budget_plugin.items = [0, 1, 3]
    result = cli_run(target, "zarr preallocate", "direct", "--input-data", tmp_path.as_uri())
    assert result.exit_code == 0, result.output
    if case == "mismatch":
        budget_plugin.pod_offset = 1
    elif case == "ownership-dropped":
        budget_plugin.ownership = False
    elif case == "late-item":
        budget_plugin.items = [2]
    else:
        arr = zarr.open_group(str(target), mode="a", use_consolidated=False)["data/owned"]
        del arr.attrs["firecube_engine_owned_complete"]
        budget_plugin.emit_owned = False
    before = payload_snapshot(target)
    result = cli_run(target)
    assert result.exit_code != 0
    assert message in result.output
    assert payload_snapshot(target) == before


def test_batch_for_a_slot_preallocate_did_not_materialize_is_refused(tmp_path, budget_plugin):
    budget_plugin.owned = True
    budget_plugin.items = [0, 1, 3]
    target = tmp_path / "cube.zarr"
    result = cli_run(target, "zarr preallocate", "direct", "--input-data", tmp_path.as_uri())
    assert result.exit_code == 0, result.output
    # Slot 2 holds NaN and the pod sends NaN: only the slot check can refuse.
    budget_plugin.items = [2]
    budget_plugin.owned_only = True
    budget_plugin.pod_offset = float("nan")
    before = payload_snapshot(target)
    result = cli_run(target)
    assert result.exit_code != 0
    assert "preallocate" in result.output + str(result.exception)
    assert payload_snapshot(target) == before


@pytest.mark.parametrize("sharded", [False, True], ids=["chunk", "shard"])
def test_pod_refuses_partial_stored_unit_then_whole_owner_writes(tmp_path, budget_plugin, sharded):
    budget_plugin.unit = 1 if sharded else 4
    budget_plugin.shards = 4 if sharded else None
    target = tmp_path / "cube.zarr"
    assert cli_run(target, "zarr preallocate").exit_code == 0
    budget_plugin.unit = None
    before = payload_snapshot(target)
    refused = cli_run(target, "ingest", "direct", "--slot-start", "1", "--slot-end", "2")
    assert refused.exit_code != 0
    assert "[0, 4)" in refused.output + str(refused.exception)
    assert payload_snapshot(target) == before
    result = cli_run(target, "ingest", "direct", "--slot-start", "0", "--slot-end", "4")
    assert result.exit_code == 0, result.output
    np.testing.assert_array_equal(root(target)["data/vec"][:], [1, 2, 3, 4])


@pytest.mark.parametrize(
    "items,ok", [([0, 1], False), ([0, 1, 2, 3], True)], ids=["partial", "full"]
)
def test_staged_promotion_requires_every_cell_of_touched_unit(tmp_path, budget_plugin, items, ok):
    budget_plugin.unit = 4
    target = tmp_path / "cube.zarr"
    assert cli_run(target, "zarr preallocate").exit_code == 0
    assert cli_run(target).exit_code == 0
    before = payload_snapshot(target)
    budget_plugin.items, budget_plugin.bias = items, 10
    result = cli_run(target, "ingest", "staged", "--option", "force_reingest=true")
    assert (result.exit_code == 0) is ok, result.output
    if not ok:
        assert "promot" in result.output + str(result.exception)
        assert payload_snapshot(target) == before
    np.testing.assert_array_equal(root(target)["data/vec"][:], np.arange(1, 5) + 10 * ok)


STORED_RANGE = 1_000_000


def _owned_target(tmp_path, length):
    """Return a store whose engine-owned array lists slots [0, STORED_RANGE) as one range."""
    target = tmp_path / "cube.zarr"
    attrs = {
        "firecube_engine_owned": True,
        "firecube_engine_owned_complete": True,
        "firecube_engine_owned_slots": [[0, STORED_RANGE]],
    }
    group = zarr.open_group(str(target), mode="w", zarr_format=3).create_group("data")
    group.create_array(
        "owned", shape=(length,), dtype="float64", fill_value=np.nan, attributes=attrs
    )
    return target


def _line_events(call):
    """Return ``(result, number of Python line events during call())``."""
    lines = 0

    def count_lines(frame: FrameType, event: str, arg: Any) -> Any:
        nonlocal lines
        if event == "line":
            lines += 1
        return count_lines

    previous = sys.gettrace()
    sys.settrace(count_lines)
    try:
        result = call()
    finally:
        sys.settrace(previous)
    return result, lines


def test_require_engine_owned_slots_work_does_not_grow_with_stored_range(tmp_path):
    target = _owned_target(tmp_path, 8)
    wanted = [("data/owned", slot) for slot in (0, 5, STORED_RANGE - 1)]
    refused, lines = _line_events(lambda: require_engine_owned_slots(root(target), wanted))
    assert refused is None
    assert lines < 3_000, (
        f"require_engine_owned_slots executed {lines} lines for a stored range of {STORED_RANGE} slots"
    )


def test_materialize_stamp_work_does_not_grow_with_stored_range(tmp_path):
    target = _owned_target(tmp_path, STORED_RANGE + 3)
    group = zarr.open_group(str(target), mode="a", use_consolidated=False)
    authority = MetadataAuthority.process_local(MetadataScope(str(target.resolve())))
    values = {STORED_RANGE + i: 1.0 + i for i in range(3)}
    _, lines = _line_events(
        lambda: materialize_engine_owned_array(
            arr=group["data/owned"],
            root=group,
            authority=authority,
            array_path="data/owned",
            values_by_slot=values,
            report=lambda message: None,
        )
    )
    slots = root(target)["data/owned"].attrs["firecube_engine_owned_slots"]
    assert slots == [[0, STORED_RANGE + 3]]
    assert lines < 15_000, (
        f"materialize_engine_owned_array executed {lines} lines for a stored range of {STORED_RANGE} slots"
    )
