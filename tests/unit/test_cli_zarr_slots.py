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

import datetime as dt
import json
import tracemalloc
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.config import StorageConfig
from firecube.core.controlplane import ChunkManager
from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.core.product.open import open_product
from firecube.ingestor.registry import loader
from firecube.ingestor.runtime import slot_plan
from firecube.ingestor.runtime.configure import configure_ingestor
from firecube.ingestor.runtime.slot_plan import (
    GroupSlotPlan,
    SlotPlanBlockedError,
    SlotPlanCeilingError,
    plan_slots,
)
from firecube.sdk import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginConfig,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)
from tests.helpers.rows import table_row


def _cli_index_spec(size: int) -> IndexSpec:
    return IndexSpec(
        name="cli_zarr_slots_test_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2026-01-01T00:00:00Z",
                cadence_s=1,
                mode="exact",
                slot_count=size,
            )
        },
    )


class _SlotsCapablePlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "slots_capable_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return _cli_index_spec(1000)

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        chunks=(100, 10),
                        shape=(1000, 10),
                        dtype=np.float32,
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


class _SlotsCapableRemainderPlugin(_SlotsCapablePlugin):
    PRODUCT_NAME: ClassVar[str] = "slots_capable_remainder_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return _cli_index_spec(950)


class _SlotsNonCapablePlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "slots_noncapable_product"

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        chunks=(100, 10),
                        shape=(1000, 10),
                        dtype=np.float32,
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.fixture(autouse=True)
def _register_slots_test_plugins() -> Iterator[None]:
    saved_ingestors = loader.AVAILABLE_INGESTORS.copy()
    saved_loaded = loader._LOADED
    register_ingestor("slots_test_capable")(_SlotsCapablePlugin)
    register_ingestor("slots_test_capable_remainder")(_SlotsCapableRemainderPlugin)
    register_ingestor("slots_test_noncapable")(_SlotsNonCapablePlugin)
    try:
        yield
    finally:
        loader.AVAILABLE_INGESTORS.clear()
        loader.AVAILABLE_INGESTORS.update(saved_ingestors)
        loader._LOADED = saved_loaded


def _required_args(tmp_path: Path, plugin: str, *, name: str = "x.zarr") -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        "zarr",
        "slots",
        plugin,
        "--target",
        (tmp_path / name).as_uri(),
        "--product-name",
        plugin,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def _required_args_without_storage_driver(
    tmp_path: Path,
    plugin: str,
    *,
    name: str = "x.zarr",
) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        "zarr",
        "slots",
        plugin,
        "--target",
        (tmp_path / name).as_uri(),
        "--product-name",
        plugin,
        "--write-mode",
        "direct",
    ]


def test_zarr_slots_json_and_ranges(subtests: Any, tmp_path: Path) -> None:
    with table_row(subtests, "zarr_slots_emits_valid_json_schema", tmp_path) as (workdir, _):
        result = CliRunner().invoke(cli, _required_args(workdir, "slots_test_capable"))

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert list(payload) == [
            "schema_version",
            "product_name",
            "target",
            "storage_type",
            "storage_driver",
            "write_mode",
            "strategy",
            "groups",
            "ranges",
        ]
        assert payload["schema_version"] == "v1"
        assert payload["strategy"] == "indexed-region"
        assert payload["product_name"] == "slots_test_capable"

    with table_row(
        subtests, "zarr_slots_emits_resolved_storage_driver_when_flag_omitted", tmp_path
    ) as (workdir, _):
        # Keep this focused on the omitted-flag path; env override coverage would
        # depend on fixture ordering and is intentionally left out here.
        result = CliRunner().invoke(
            cli, _required_args_without_storage_driver(workdir, "slots_test_capable")
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["storage_driver"] == "fsspec"

    with table_row(subtests, "zarr_slots_ranges_have_env_and_cli_args", tmp_path) as (workdir, _):
        result = CliRunner().invoke(cli, _required_args(workdir, "slots_test_capable"))

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        ranges = payload["ranges"]
        assert len(ranges) > 0
        for entry in ranges:
            assert entry["env"]["FIRECUBE_SLOT_START"] == str(entry["slot_start"])
            assert entry["env"]["FIRECUBE_SLOT_END"] == str(entry["slot_end"])
            assert entry["env"]["FIRECUBE_SLOT_GROUP"] == entry["group"]
            assert entry["cli_args"] == [
                "--slot-start",
                str(entry["slot_start"]),
                "--slot-end",
                str(entry["slot_end"]),
                "--slot-group",
                entry["group"],
            ]

    with table_row(subtests, "zarr_slots_last_range_handles_remainder", tmp_path) as (workdir, _):
        result = CliRunner().invoke(cli, _required_args(workdir, "slots_test_capable_remainder"))

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        ranges = [r for r in payload["ranges"] if r["group"] == "data"]
        assert ranges[-1]["slot_start"] == 900
        assert ranges[-1]["slot_end"] == 950
        assert ranges[-1]["slot_end"] - ranges[-1]["slot_start"] == 50

    with table_row(subtests, "zarr_slots_no_resume_emits_full_ranges", tmp_path) as (workdir, _):
        args = [*_required_args(workdir, "slots_test_capable"), "--no-resume"]
        result = CliRunner().invoke(cli, args)

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        group = next(g for g in payload["groups"] if g["name"] == "data")
        assert group["covered_ranges"] == []
        assert group["remaining_ranges"] == [[0, 1000]]
        data_ranges = [r for r in payload["ranges"] if r["group"] == "data"]
        assert data_ranges[0]["slot_start"] == 0
        assert data_ranges[-1]["slot_end"] == 1000
        assert sum(r["slot_end"] - r["slot_start"] for r in data_ranges) == 1000


def test_zarr_slots_help_and_refusals(subtests: Any, tmp_path: Path) -> None:
    with table_row(subtests, "zarr_slots_help", tmp_path):
        result = CliRunner().invoke(cli, ["zarr", "slots", "--help"])

        assert result.exit_code == 0
        assert "Examples:" in result.output
        assert "--slot-size" in result.output

    with table_row(subtests, "zarr_slots_missing_write_mode", tmp_path):
        result = CliRunner().invoke(
            cli,
            [
                "zarr",
                "slots",
                "test_product",
                "--product-name",
                "test",
                "--target",
                "file:///tmp/x.zarr",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
            ],
        )

        assert result.exit_code != 0
        assert "write-mode" in result.output.lower() or "write_mode" in result.output.lower()

    with table_row(subtests, "zarr_slots_missing_target", tmp_path):
        result = CliRunner().invoke(
            cli,
            [
                "zarr",
                "slots",
                "test_product",
                "--product-name",
                "test",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "direct",
            ],
        )

        assert result.exit_code != 0
        assert "target" in result.output.lower()

    with table_row(subtests, "slots_rejects_file_uri_with_s3_storage_type", tmp_path):
        result = CliRunner().invoke(
            cli,
            [
                "zarr",
                "slots",
                "slots_test_capable",
                "--product-name",
                "x",
                "--target",
                "file:///tmp/x.zarr",
                "--storage-type",
                "s3",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "staged",
                "--no-resume",
            ],
            prog_name="firecube",
        )

        assert result.exit_code != 0, result.output
        assert "incompatible" in result.output

    with table_row(subtests, "slots_rejects_s3_uri_with_local_storage_type", tmp_path):
        result = CliRunner().invoke(
            cli,
            [
                "zarr",
                "slots",
                "slots_test_capable",
                "--product-name",
                "x",
                "--target",
                "s3://bucket/x.zarr",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "staged",
                "--no-resume",
            ],
            prog_name="firecube",
        )

        assert result.exit_code != 0, result.output
        assert "incompatible" in result.output

    with table_row(subtests, "zarr_slots_capability_check_fails_for_non_capable", tmp_path) as (
        workdir,
        _,
    ):
        result = CliRunner().invoke(cli, _required_args(workdir, "slots_test_noncapable"))

        assert result.exit_code != 0, result.output
        assert "returned no index_spec" in result.output

    with table_row(subtests, "slots_non_capable_plugin_is_rejected", tmp_path) as (workdir, _):
        config = workdir / "config.toml"
        config.write_text("", encoding="utf-8")

        result = CliRunner().invoke(
            cli,
            [
                "--config-file",
                str(config),
                "zarr",
                "slots",
                "direct_zarr_non_capable_test_plugin",
                "--target",
                (workdir / "slots.zarr").as_uri(),
                "--product-name",
                "direct_zarr_non_capable_test_plugin",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "direct",
                "--no-resume",
            ],
            catch_exceptions=True,
        )

        assert result.exit_code != 0
        assert "returned no index_spec" in result.output


def test_zarr_slots_does_not_mutate_target(tmp_path: Path) -> None:
    target_path = tmp_path / "x.zarr"
    assert not target_path.exists()

    result = CliRunner().invoke(cli, _required_args(tmp_path, "slots_test_capable"))

    assert result.exit_code == 0, result.output
    assert not target_path.exists()
    assert not (tmp_path / ".firecube").exists()


def test_slots_option_reaches_plugin_hook(tmp_path: Path) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    @dataclass
    class _SlotsCapableOptionConfig(PluginConfig):
        horizon_end_iso: str = "2024-01-01T01:00:00Z"

    @register_ingestor("direct_zarr_slots_capable_option_test")
    class _SlotsCapableOptionIngestor(plugin_module.DirectZarrCapableTestIngestor):
        plugin_config_class = _SlotsCapableOptionConfig

        def index_spec(self, ctx: PluginContext) -> IndexSpec:
            assert isinstance(self.plugin_config, _SlotsCapableOptionConfig)
            assert self.plugin_config.horizon_end_iso == "2024-01-01T02:00:00Z"
            return IndexSpec(
                name="direct_zarr_slots_capable_option_test_v1",
                groups={
                    "data": RegularTimeAxis(
                        coordinate="timestamp",
                        epoch="2024-01-01T00:00:00Z",
                        cadence_s=300,
                        mode="exact",
                        slot_count=24,
                    )
                },
            )

    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")

    result = CliRunner().invoke(
        cli,
        [
            "--config-file",
            str(config),
            "zarr",
            "slots",
            "direct_zarr_slots_capable_option_test",
            "--target",
            (tmp_path / "slots.zarr").as_uri(),
            "--product-name",
            "direct_zarr_slots_capable_option_test",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "direct",
            "--no-resume",
            "--option",
            "horizon_end_iso=2024-01-01T02:00:00Z",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    group = next(g for g in payload["groups"] if g["name"] == "data")
    assert group["total_slots"] == 24


def _register_sized_plugin(name: str, slots: int, chunks: tuple[int, int] | None = (1, 10)) -> None:
    array = ZarrArraySpec(name="data", chunks=chunks, shape=(slots, 10), dtype=np.float32)
    members = {
        "PRODUCT_NAME": name,
        "index_spec": lambda self, ctx: _cli_index_spec(slots),
        "zarr_schema": lambda self, ctx: [ZarrGroupSpec(group="data", arrays=[array])],
    }
    register_ingestor(name)(type(name, (_SlotsCapablePlugin,), members))


def test_zarr_slots_streams_large_plan_in_bounded_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = '"FIRECUBE_SLOT_START"'
    seen, tail = 0, ""

    def write(text: str) -> int:
        nonlocal seen, tail
        window = tail + text
        seen += window.count(marker)
        tail = window[-(len(marker) - 1) :]
        return len(text)

    sink = SimpleNamespace(write=write, flush=lambda: None, isatty=lambda: False)
    _register_sized_plugin("slots_test_stream", 100_000)
    args = [*_required_args(tmp_path, "slots_test_stream"), "--no-resume"]
    monkeypatch.setattr("sys.stdout", sink)
    tracemalloc.start()
    try:
        cli.main(args, prog_name="firecube", standalone_mode=False)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert seen == 100_000
    assert peak < 32 * 1024 * 1024


def test_zarr_slots_refuses_plan_above_range_ceiling_unless_confirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("firecube.ingestor.runtime.slot_plan.PLAN_RANGE_CEILING", 10, raising=False)
    _register_sized_plugin("slots_test_ceiling", 11)
    args = [*_required_args(tmp_path, "slots_test_ceiling"), "--no-resume"]

    refused = CliRunner().invoke(cli, args)
    confirmed = CliRunner().invoke(cli, [*args, "--yes-i-really-mean-it"])

    assert refused.exit_code != 0
    assert refused.stdout == ""
    assert all(s in refused.output for s in ("11", "--slot-size 2", "--yes-i-really-mean-it"))
    assert confirmed.exit_code == 0, confirmed.output
    assert len(json.loads(confirmed.output)["ranges"]) == 11


def test_zarr_slots_hints_on_stderr_above_range_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("firecube.cli.zarr._slots.PLAN_RANGE_HINT", 5, raising=False)
    _register_sized_plugin("slots_test_hint", 11, chunks=None)

    result = CliRunner().invoke(cli, _required_args(tmp_path, "slots_test_hint"))

    assert result.exit_code == 0, result.output
    assert len(json.loads(result.stdout)["ranges"]) == 11
    hints = [ln for ln in capfd.readouterr().err.splitlines() if "--slot-size" in ln]
    assert len(hints) == 1
    assert "11" in hints[0]


@pytest.mark.parametrize(
    ("chunks", "shards"),
    [pytest.param((250, 10), None, id="chunked"), pytest.param((1, 10), (250, 10), id="sharded")],
)
def test_zarr_slots_aligns_to_stored_units_of_existing_arrays(
    tmp_path: Path, chunks: tuple[int, int], shards: tuple[int, int] | None
) -> None:
    _register_sized_plugin("slots_test_stored", 1000, chunks=None)
    group = zarr.open_group(tmp_path / "x.zarr", mode="w", zarr_format=3).require_group("data")
    group.create_array("data", shape=(1000, 10), chunks=chunks, shards=shards, dtype="float32")
    args = [*_required_args(tmp_path, "slots_test_stored"), "--no-resume"]

    planned = CliRunner().invoke(cli, args)
    refused = CliRunner().invoke(cli, [*args, "--slot-size", "100"])

    assert planned.exit_code == 0, planned.output
    plan = json.loads(planned.output)
    assert (plan["groups"][0]["slot_size"], len(plan["ranges"])) == (250, 4)
    assert refused.exit_code != 0
    assert "250" in refused.output


def _plan_in_python(tmp_path: Path, groups: dict[str, tuple[int, int]], **kwargs: Any) -> Any:
    """Plan `groups` (name -> (slots, time chunk)) the way a Python caller does, without Click."""
    schema = [
        ZarrGroupSpec(
            group=name,
            arrays=[
                ZarrArraySpec(name="data", chunks=(chunk, 10), shape=(n, 10), dtype=np.float32)
            ],
        )
        for name, (n, chunk) in groups.items()
    ]
    spec = IndexSpec(
        name="plan_slots_test_v1",
        groups={
            name: RegularTimeAxis(
                coordinate="timestamp",
                epoch="2026-01-01T00:00:00Z",
                cadence_s=1,
                mode="exact",
                slot_count=n,
            )
            for name, (n, _) in groups.items()
        },
    )
    members = {
        "PRODUCT_NAME": "plan_slots_product",
        "index_spec": lambda self, ctx: spec,
        "zarr_schema": lambda self, ctx: schema,
    }
    target = (tmp_path / "x.zarr").as_uri()
    with open_product(target, format="zarr", require_label=False) as product:
        ingestor = type("PlanSlotsPlugin", (_SlotsCapablePlugin,), members)(
            chunk_manager=product.manager
        )
        ctx = configure_ingestor(ingestor, target=target, options={}, run_id="run-1")
        return plan_slots(
            product,
            ingestor,
            plugin_ctx=ctx,
            plugin_name="plan_slots_test",
            storage_config=StorageConfig(storage_type="local"),
            coverage_storage_config=lambda: StorageConfig(storage_type="local"),
            **kwargs,
        )


@pytest.mark.contract
@pytest.mark.integration
def test_plan_slots_fresh_target_gives_lazy_plan_per_group(tmp_path: Path) -> None:
    plans = _plan_in_python(tmp_path, {"b": (300, 100), "a": (1000, 100)}, resume=False)

    assert [type(p) for p in plans] == [GroupSlotPlan, GroupSlotPlan]
    assert [p.name for p in plans] == ["a", "b"]
    ranges = plans[0].ranges()
    assert iter(ranges) is ranges
    assert next(ranges) == (0, 100)
    assert [(p.range_count, len(list(p.ranges()))) for p in plans] == [(10, 10), (3, 3)]

    (big,) = _plan_in_python(tmp_path, {"data": (200_000, 1)}, resume=False)
    tracemalloc.start()
    try:
        count = big.range_count
        first = next(big.ranges())
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()

    assert (count, first) == (200_000, (0, 1))
    assert peak < 256 * 1024


@pytest.mark.contract
@pytest.mark.integration
def test_plan_slots_refuses_blocked_work_with_its_intervals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(slot_plan, "_query_slots_coverage", lambda **kw: {"a": [(0, 73)], "b": []})

    with pytest.raises(SlotPlanBlockedError) as refused:
        _plan_in_python(tmp_path, {"a": (1000, 100), "b": (300, 100)})

    assert refused.value.blocked == {"a": [(73, 100)]}


@pytest.mark.contract
@pytest.mark.integration
def test_plan_slots_refuses_plan_above_ceiling_unless_overridden(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(slot_plan, "PLAN_RANGE_CEILING", 10)

    with pytest.raises(SlotPlanCeilingError) as refused:
        _plan_in_python(tmp_path, {"data": (11, 1)}, resume=False)
    plans = _plan_in_python(tmp_path, {"data": (11, 1)}, resume=False, allow_above_ceiling=True)

    assert (refused.value.range_count, refused.value.suggested_slot_size) == (11, 2)
    assert [p.range_count for p in plans] == [11]


@pytest.mark.parametrize("planning_fails", [False, True], ids=["run_succeeds", "run_raises"])
def test_zarr_slots_closes_its_planning_chunk_manager(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planning_fails: bool
) -> None:
    # Instruments the class, so every manager built during the run is counted, however the
    # command reaches the constructor.
    built: list[ChunkManager] = []
    closes: list[ChunkManager] = []
    real_init, real_close = ChunkManager.__init__, ChunkManager.close

    def recording_init(self: ChunkManager, *args: Any, **kwargs: Any) -> None:
        built.append(self)
        real_init(self, *args, **kwargs)

    def recording_close(self: ChunkManager) -> None:
        closes.append(self)
        real_close(self)

    monkeypatch.setattr(ChunkManager, "__init__", recording_init)
    monkeypatch.setattr(ChunkManager, "close", recording_close)
    if planning_fails:

        def _boom(*_: Any, **__: Any) -> Any:
            raise RuntimeError("planning failed after the manager was built")

        monkeypatch.setattr("firecube.cli.zarr._slots.plan_slots", _boom)

    result = CliRunner().invoke(cli, _required_args(tmp_path, "slots_test_capable"))

    assert (result.exit_code != 0) is planning_fails, result.output
    assert built
    assert [closes.count(m) for m in built] == [1] * len(built)
