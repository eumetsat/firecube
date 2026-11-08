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

"""Engine-driven partial-chunk data + attrs preservation on resume-append.

Two engine-driven variants that exercise the full ``host.run(...)`` path (not
``write_dataset_to_zarr`` directly) to lock the following behaviour end-to-end
on plain resume-appends (NO ``force_reingest``):

* **Data preservation.** With ``chunk_shape={"timestamp": 2}`` and
  run 1 writing days ``[0, 1, 2]``, chunk 0 is full and chunk 1 is half-full
  (only ``day_2``). Run 2 appends ``day_3`` to extend chunk 1 to
  ``[day_2, day_3]``. The regression symptom was that the seeding step
  could clobber ``day_2`` during workspace preparation; after the fix,
  ``temperature[2]`` MUST survive promotion with its original run-1 value.

* **Attrs preservation (engine-driven).** Same chunk setup with
  ``pipeline_batch_size=1`` on run 2. Run-1 dataset attrs
  ``{"title": "first", "history": "run1", "layout_tags": ("weather", "eu")}``
  MUST win over run-2 attrs ``{"title": "second", "history": "run2",
  "layout_tags": ("weather", "eu")}`` (first-write-wins). Because the drift
  diff normalises tuples/lists, ``layout_tags`` compares equal after the
  JSON round-trip and MUST NOT show up in any WARN record's ``changed`` field.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner
from pytest_mock import MockerFixture

from firecube.cli.main import cli
from firecube.core.api import IndexedWrite, IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    ExecutionParams,
    GenericZarrIngestor,
    PluginContext,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.registry import loader as _loader
from firecube.ingestor.runtime.zarr import append as append_module
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.integration

_GROUP = "G"
_PRODUCT = "partial_chunk_preservation.zarr"

# Distinct sentinel values keep the assertion errors self-describing when a
# slot has been clobbered by the seeding step.
_TEMPERATURE = {d: 10.0 + d for d in range(10)}


def _dataset(
    days: list[int],
    *,
    attrs: dict[str, Any] | None = None,
) -> xr.Dataset:
    """Build a small ``temperature`` dataset for the given day indices."""
    timestamps = np.datetime64("2024-01-01", "ns") + np.asarray(days).astype("timedelta64[D]")
    values = np.array([_TEMPERATURE[d] for d in days], dtype=np.float32).reshape(len(days), 1)
    ds = xr.Dataset(
        {"temperature": (("timestamp", "x"), values)},
        coords={"timestamp": timestamps, "x": np.arange(1)},
    )
    ds["timestamp"].encoding.update(dtype="int64", units="nanoseconds since 1970-01-01")
    if attrs is not None:
        ds.attrs.update(attrs)
    return ds


class _PartialChunkZarr(GenericZarrIngestor):
    """Test-local plugin driven by CLI options for reproducible engine runs."""

    PRODUCT_NAME = "partial_chunk_preservation"
    name = "partial_chunk_preservation"
    time_dim_name = "timestamp"

    def discover_source_files(self, ctx: PluginContext) -> list[int]:
        return list(ctx.option("x_days") or [])

    def get_batch_groups(self, items, ctx: PluginContext) -> list[str]:
        return [ctx.option("x_group") or _GROUP]

    def build_dataset(self, group: str, items: list[int], ctx: PluginContext) -> xr.Dataset:
        _ = group
        return _dataset(list(items), attrs=ctx.option("x_attrs", None))


def _engine_run(
    tmp_path: Path,
    days: list[int],
    *,
    attrs: dict[str, Any] | None = None,
    pipeline_batch_size: int = 8,
    resume_existing: bool = False,
    group: str | None = None,
    write_mode: str = "staged",
) -> None:
    """Drive one ingest through ``host.run(ctx)``; staged write mode unless told otherwise."""
    host = _PartialChunkZarr()
    options: dict[str, Any] = {
        "x_group": group,
        "x_days": days,
        "x_attrs": attrs,
        "pipeline_workers": 1,
        "pipeline_batch_size": pipeline_batch_size,
        "no_progress": True,
        "cleanup_workspace": True,
        "zarr_chunk_shape": {"timestamp": 2, "x": 1},
    }
    if resume_existing:
        options["resume_existing"] = True
    ctx = make_test_context(
        tmp_path,
        product=_PRODUCT,
        options=options,
        execution=ExecutionParams(write_mode=write_mode),
    )
    host.run(ctx)


def _attr_drift_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.message == "Group attributes differ from stored; keeping first-write values"
    ]


def test_resume_append_preserves_partial_chunk(tmp_path: Path) -> None:
    """Partial-chunk data survives a resume-append (no ``force_reingest``).

    ``chunk_shape={"timestamp": 2}`` — physical chunks are ``[0, 1]`` and
    ``[2, 3]``. Run 1 writes days ``[0, 1, 2]``, filling chunk 0 and leaving
    chunk 1 half-full with ``day_2`` only. Run 2 appends ``day_3``: the seed
    step MUST NOT clobber the existing ``day_2`` slot in chunk 1. After
    promotion, ``temperature[2]`` is preserved as its run-1 value.

    Regression symptom (pre-fix): ``temperature[2]`` came back NaN because
    the seeding path replaced the partial chunk with fill values before
    batch A wrote ``day_3`` into it.
    """
    _engine_run(tmp_path, days=[0, 1, 2])
    _engine_run(tmp_path, days=[3], resume_existing=True)

    target = tmp_path / _PRODUCT
    with xr.open_zarr(str(target), group=_GROUP, consolidated=False) as ds:
        assert ds.sizes["timestamp"] == 4, (
            f"expected timestamp size 4 after run 2 append, got {ds.sizes['timestamp']}"
        )
        temp = np.asarray(ds["temperature"].values).reshape(-1)
        assert not np.isnan(temp[2]), (
            "temperature[2] (partial-chunk slot from run 1) was clobbered: got NaN"
        )
        assert temp[2] == pytest.approx(_TEMPERATURE[2]), (
            f"temperature[2] should preserve run-1 value {_TEMPERATURE[2]}, got {temp[2]}"
        )
        # Sanity: other slots also match their expected sentinel values.
        assert temp[0] == pytest.approx(_TEMPERATURE[0])
        assert temp[1] == pytest.approx(_TEMPERATURE[1])
        assert temp[3] == pytest.approx(_TEMPERATURE[3])


def test_multi_batch_resume_append_preserves_first_write_attrs_engine_driven(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """First-write-wins group attrs on engine-driven resume-append.

    Same chunk setup as the resume-append case with ``pipeline_batch_size=1``
    on run 2 to force the engine's per-batch write path. Run-1 attrs win;
    the drift diff normalises tuples/lists so ``layout_tags`` compares equal
    after the JSON round-trip and MUST NOT show up in any WARN record's
    ``changed``.
    """
    run1_attrs = {
        "title": "first",
        "history": "run1",
        "layout_tags": ("weather", "eu"),
    }
    run2_attrs = {
        "title": "second",
        "history": "run2",
        "layout_tags": ("weather", "eu"),
    }

    _engine_run(tmp_path, days=[0, 1, 2], attrs=run1_attrs)

    with caplog.at_level(logging.WARNING):
        _engine_run(
            tmp_path,
            days=[3],
            attrs=run2_attrs,
            pipeline_batch_size=1,
            resume_existing=True,
        )

    warnings = _attr_drift_warnings(caplog)
    title_count = sum(1 for r in warnings if "title" in getattr(r, "changed", ()))
    history_count = sum(1 for r in warnings if "history" in getattr(r, "changed", ()))
    tags_count = sum(1 for r in warnings if "layout_tags" in getattr(r, "changed", ()))
    assert title_count == 1, (
        f"expected exactly 1 attr-drift WARN mentioning 'title'; got {title_count} "
        f"(records: {[getattr(r, 'changed', ()) for r in warnings]})"
    )
    assert history_count == 1, (
        f"expected exactly 1 attr-drift WARN mentioning 'history'; got {history_count} "
        f"(records: {[getattr(r, 'changed', ()) for r in warnings]})"
    )
    assert tags_count == 0, (
        f"expected 0 attr-drift WARN mentioning 'layout_tags' (tuple/list "
        f"normalisation); got {tags_count} "
        f"(records: {[getattr(r, 'changed', ()) for r in warnings]})"
    )

    target = tmp_path / _PRODUCT
    with xr.open_zarr(str(target), group=_GROUP, consolidated=False) as ds:
        assert ds.attrs["title"] == "first", (
            f"first-write-wins: title should be 'first', got {ds.attrs.get('title')!r}"
        )
        assert ds.attrs["history"] == "run1", (
            f"first-write-wins: history should be 'run1', got {ds.attrs.get('history')!r}"
        )
        # JSON round-trip lists the tuple; tuple/list normalisation still lets the compare match.
        assert ds.attrs["layout_tags"] == ["weather", "eu"], (
            f"layout_tags should round-trip to list ['weather','eu'], got "
            f"{ds.attrs.get('layout_tags')!r}"
        )


@pytest.mark.parametrize("write_mode", ["direct", "staged"])
def test_root_slash_first_write_keeps_control_plane(tmp_path: Path, write_mode: str) -> None:
    """A cube at the root group "/" keeps ``.firecube/`` through its first write.

    The arrays land at the store root, the schema and the run records the
    control plane wrote before the first batch are still there afterwards, and
    a second run appends to the same root cube.
    """
    target = tmp_path / _PRODUCT
    _engine_run(tmp_path, days=[0, 1, 2], group="/", write_mode=write_mode)

    root = zarr.open_group(str(target), mode="r", use_consolidated=False)
    assert "temperature" in root, f"arrays are not at the root: {sorted(root.keys())}"
    assert (target / ".firecube" / "schema.json").is_file(), (
        "the first write at '/' deleted the control plane"
    )
    events = [
        json.loads(line)
        for segment in sorted((target / ".firecube" / "runs").glob("*/events-*.jsonl"))
        for line in segment.read_text().splitlines()
        if line
    ]
    run_ids = {event["run_id"] for event in events}
    assert len(run_ids) == 1, f"expected the events of one run, got {sorted(run_ids)}"
    assert {event["event_type"] for event in events} >= {"run_started", "run_completed"}, (
        f"the first run's started and completed records are not persisted: {events}"
    )
    assert {p.parent.name for p in (target / ".firecube" / "runs").glob("*/events-*.jsonl")} == (
        run_ids
    ), "an event segment is not under the directory of its run id"

    _engine_run(tmp_path, days=[3], group="/", write_mode=write_mode, resume_existing=True)

    root = zarr.open_group(str(target), mode="r", use_consolidated=False)
    assert np.asarray(cast(Any, root["temperature"])[:]).reshape(-1).tolist() == [
        _TEMPERATURE[d] for d in range(4)
    ]


def test_root_slash_failed_first_batch_repaired(tmp_path: Path, mocker: MockerFixture) -> None:
    """A first batch that fails at "/" is repaired without deleting the store.

    The repair removes the half-written arrays only: ``.firecube/`` stays, and
    a resumed run writes every slot.
    """
    target = tmp_path / _PRODUCT
    real_write = append_module.write_dataset_to_zarr
    calls: list[int] = []

    def _write_then_fail(ds: xr.Dataset, **kwargs: Any) -> None:
        real_write(ds, **kwargs)
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("injected failure after the first write")

    mocker.patch.object(append_module, "write_dataset_to_zarr", _write_then_fail)

    with pytest.raises(Exception, match="injected failure"):
        _engine_run(tmp_path, days=[0, 1], group="/", write_mode="direct")

    root = zarr.open_group(str(target), mode="r", use_consolidated=False)
    assert "temperature" not in root, (
        f"the repair left the failed batch's arrays at the root: {sorted(root.keys())}"
    )
    assert (target / ".firecube" / "schema.json").is_file(), (
        "the repair of a failed first batch at '/' deleted the control plane"
    )

    _engine_run(tmp_path, days=[0, 1], group="/", write_mode="direct", resume_existing=True)

    root = zarr.open_group(str(target), mode="r", use_consolidated=False)
    assert np.asarray(cast(Any, root["temperature"])[:]).reshape(-1).tolist() == [
        _TEMPERATURE[0],
        _TEMPERATURE[1],
    ]


# --- DirectZarr staged promotion of units rewritten to the fill value -----------------
#
# Zarr stores no object for a unit whose every cell equals the fill value, so a
# staged run that rewrites a whole unit to fill leaves nothing in the workspace
# for it. Promotion must delete the target's old object rather than keep it.

_FILL_SLOTS = 8
_FILL_EPOCH = np.datetime64("2026-01-01T00:00:00", "ns")
_FILL_STEP = np.timedelta64(300, "s").astype("timedelta64[ns]")
# What the inline plugin writes this run: {"group", "shards", "values": {slot: value}}.
_FILL_RUN: dict[str, Any] = {}


class _StagedFillDirectZarr(DirectZarrIngestor):
    """One ``values`` array (time chunk 2, fill 0.0), one region write per slot."""

    PRODUCT_NAME: ClassVar[str] = "staged_fill_direct"

    def discover_source_files(self, ctx: PluginContext) -> list[int]:
        return sorted(_FILL_RUN["values"])

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        axis = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2026-01-01T00:00:00Z",
            cadence_s=300,
            mode="exact",
            slot_count=_FILL_SLOTS,
        )
        return IndexSpec(name="staged_fill_v1", groups={_FILL_RUN["group"]: axis})

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo:
        return ItemInfo(coordinate=_FILL_EPOCH + int(item) * _FILL_STEP)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        values = ZarrArraySpec(
            name="values",
            shape=(_FILL_SLOTS, 2, 2),
            dtype="float32",
            chunks=_FILL_RUN.get("chunks", (2, 2, 2)),
            shards=_FILL_RUN["shards"],
            fill_value=0.0,
            expected_time_count=_FILL_SLOTS,
            time_indexed=True,
            dimension_names=("timestamp", "y", "x"),
        )
        return [ZarrGroupSpec(group=_FILL_RUN["group"], arrays=[values])]

    def build_write_intents(self, batch: Any, ctx: PluginContext) -> list[Any]:
        return [
            IndexedWrite.region(
                group=_FILL_RUN["group"],
                array="values",
                coordinate=_FILL_EPOCH + int(slot) * _FILL_STEP,
                data=np.full((2, 2), _FILL_RUN["values"][int(slot)], dtype=np.float32),
                y_slice=slice(0, 2),
            )
            for slot in batch.items
        ]


@pytest.fixture
def _staged_fill_plugin() -> Iterator[None]:
    """Register the inline plugin for the CLI; restore the registry afterwards."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS["staged_fill_direct"] = _StagedFillDirectZarr
    _StagedFillDirectZarr.name = "staged_fill_direct"
    _loader._LOADED = True
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _fill_ingest(
    target: Path,
    driver: str,
    write_mode: str,
    values: dict[int, float],
    options: tuple[str, ...] = (),
) -> None:
    _FILL_RUN["values"] = values
    args = [
        "ingest",
        "staged_fill_direct",
        "--target",
        f"file://{target}",
        "--product-name",
        "staged_fill_direct",
        "--storage-type",
        "local",
        "--storage-driver",
        driver,
        "--write-mode",
        write_mode,
        "--option",
        "no_progress=true",
    ]
    for option in options:
        args += ["--option", option]
    if write_mode == "staged":
        args += ["--option", "force_reingest=true"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, f"{write_mode} ingest failed:\n{result.output}"


def _objects(root: Path) -> dict[str, tuple[int, int, int]]:
    return {
        path.relative_to(root).as_posix(): (
            path.stat().st_ino,
            path.stat().st_mtime_ns,
            path.stat().st_size,
        )
        for path in sorted(root.rglob("*"))
        if path.is_file() and ".firecube" not in path.parts
    }


@pytest.mark.parametrize(
    ("group", "shards", "driver", "subdir", "staged", "expected", "removed", "untouched"),
    [
        pytest.param(
            "data",
            None,
            "fsspec",
            "",
            dict.fromkeys(range(8), 0.0),
            [0.0] * 8,
            ["c/0/0/0", "c/1/0/0", "c/2/0/0", "c/3/0/0"],
            [],
            id="chunk-allfill",
        ),
        pytest.param(
            "data",
            None,
            "fsspec",
            "",
            # units [0,1] rewritten, [2,3] -> fill, [4,5] partly fill (kept), [6,7] untouched
            {0: 20.0, 1: 21.0, 2: 0.0, 3: 0.0, 4: 0.0, 5: 7.0},
            [20.0, 21.0, 0.0, 0.0, 0.0, 7.0, 16.0, 17.0],
            ["c/1/0/0"],
            ["c/3/0/0"],
            id="chunk-mixed",
        ),
        pytest.param(
            "data",
            (4, 2, 2),
            "fsspec",
            "",
            {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0},  # shard 0 -> fill, shard 1 untouched
            [0.0, 0.0, 0.0, 0.0, 14.0, 15.0, 16.0, 17.0],
            ["c/0/0/0"],
            ["c/1/0/0"],
            id="shard-mixed",
        ),
        pytest.param(
            "data",
            None,
            "fsspec",
            "bucket/prefix",
            {2: 0.0, 3: 0.0},
            [10.0, 11.0, 0.0, 0.0, 14.0, 15.0, 16.0, 17.0],
            ["c/1/0/0"],
            ["c/0/0/0", "c/2/0/0", "c/3/0/0"],
            id="chunk-prefixed-product",
        ),
        pytest.param(
            "data",
            (4, 2, 2),
            "obstore",
            "bucket/prefix",
            {4: 0.0, 5: 0.0, 6: 0.0, 7: 0.0},
            [10.0, 11.0, 12.0, 13.0, 0.0, 0.0, 0.0, 0.0],
            ["c/1/0/0"],
            ["c/0/0/0"],
            id="shard-obstore-prefixed-product",
        ),
    ],
)
@pytest.mark.usefixtures("_staged_fill_plugin")
def test_direct_zarr_staged_promotion_deletes_units_rewritten_to_fill(
    tmp_path: Path,
    group: str,
    shards: tuple[int, ...] | None,
    driver: str,
    subdir: str,
    staged: dict[int, float],
    expected: list[float],
    removed: list[str],
    untouched: list[str],
) -> None:
    """A staged rewrite to the fill value reaches the target; nothing else moves.

    Run 1 writes 10..17 directly; run 2 rewrites some slots in staged mode
    with ``force_reingest``. Every unit run 2 turned all fill is gone from the
    target (zarr reads the fill value there), and every target object of a
    unit run 2 did not write keeps its inode, mtime and size, as do the
    untouched time-coordinate chunks.
    """
    _FILL_RUN.update(group=group, shards=shards)
    target = tmp_path / subdir / "t.zarr"
    _fill_ingest(target, driver, "direct", {s: 10.0 + s for s in range(_FILL_SLOTS)})
    before = _objects(target)
    values_prefix = f"{group}/values/"
    for key in removed + untouched:
        assert values_prefix + key in before, f"setup: {values_prefix + key} missing"

    _fill_ingest(target, driver, "staged", staged)

    after = _objects(target)
    got = zarr.open_array(str(target), path=f"{group}/values", mode="r")[:, 0, 0]
    assert np.asarray(got).tolist() == expected
    assert sorted(set(before) - set(after)) == sorted(values_prefix + k for k in removed)
    unwritten_slots = set(range(_FILL_SLOTS)) - set(staged)
    keep = [values_prefix + k for k in untouched] + [
        f"{group}/timestamp/c/{slot}" for slot in sorted(unwritten_slots)
    ]
    for key in keep:
        assert after.get(key) == before[key], f"{key} was rewritten by promotion"


@pytest.mark.parametrize("keep_empty", [False, True], ids=["drop-empty", "keep-empty"])
@pytest.mark.usefixtures("_staged_fill_plugin")
def test_staged_unit_write_buffer_promotes_same_target_as_unbuffered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, keep_empty: bool
) -> None:
    """The unit write buffer changes no byte a staged run promotes.

    One-slot units, so every region write is a whole unit the buffer publishes.
    Run 2 turns units 0 and 1 all fill: the target drops those objects unless
    the handle keeps empty chunks, with the buffer on exactly as with it off.
    """
    monkeypatch.setitem(_FILL_RUN, "chunks", (1, 2, 2))
    _FILL_RUN.update(group="data", shards=None)
    run1 = {s: 10.0 + s for s in range(_FILL_SLOTS)}
    run2 = {0: 0.0, 1: 0.0, 2: 22.0}
    trees: dict[str, dict[str, bytes]] = {}
    for name, buffer in (("off", "false"), ("on", "true")):
        options = (f"zarr_unit_write_buffer={buffer}", f"zarr_write_empty_chunks={keep_empty}")
        target = tmp_path / name / "t.zarr"
        _fill_ingest(target, "fsspec", "direct", run1, options)
        _fill_ingest(target, "fsspec", "staged", run2, options)
        trees[name] = {
            p.relative_to(target).as_posix(): p.read_bytes()
            for p in sorted(target.rglob("*"))
            if p.is_file() and ".firecube" not in p.parts
        }

    assert trees["on"] == trees["off"]
    values = zarr.open_array(str(tmp_path / "on" / "t.zarr"), path="data/values", mode="r")
    assert np.asarray(values[:, 0, 0]).tolist() == [0.0, 0.0, 22.0, 13.0, 14.0, 15.0, 16.0, 17.0]
    rewritten = {k for k in trees["on"] if k.startswith(("data/values/c/0/", "data/values/c/1/"))}
    assert rewritten == ({"data/values/c/0/0/0", "data/values/c/1/0/0"} if keep_empty else set())
