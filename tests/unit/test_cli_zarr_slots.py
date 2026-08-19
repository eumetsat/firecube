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

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import SlotAxis, SlotIndexModel
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginConfig,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)
from firecube.ingestor.registry import loader


def _cli_slot_model() -> SlotIndexModel:
    return SlotIndexModel(
        name="cli_zarr_slots_test_v1",
        epoch="2026-01-01T00:00:00Z",
        groups={"data": SlotAxis(cadence_s=1, mode="exact")},
    )


class _SlotsCapablePlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "slots_capable_product"
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        return {"data": 1000}

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        return _cli_slot_model()

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

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        return {"data": 950}


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


def test_zarr_slots_help() -> None:
    result = CliRunner().invoke(cli, ["zarr", "slots", "--help"])

    assert result.exit_code == 0
    assert "Examples:" in result.output
    assert "--slot-size" in result.output


def test_zarr_slots_missing_write_mode() -> None:
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


def test_zarr_slots_missing_target() -> None:
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


def test_slots_rejects_file_uri_with_s3_storage_type() -> None:
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


def test_slots_rejects_s3_uri_with_local_storage_type() -> None:
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


def test_zarr_slots_emits_valid_json_schema(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _required_args(tmp_path, "slots_test_capable"))

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


def test_zarr_slots_emits_resolved_storage_driver_when_flag_omitted(tmp_path: Path) -> None:
    # Keep this focused on the omitted-flag path; env override coverage would
    # depend on fixture ordering and is intentionally left out here.
    result = CliRunner().invoke(
        cli, _required_args_without_storage_driver(tmp_path, "slots_test_capable")
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["storage_driver"] == "fsspec"


def test_zarr_slots_ranges_have_env_and_cli_args(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _required_args(tmp_path, "slots_test_capable"))

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


def test_zarr_slots_last_range_handles_remainder(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _required_args(tmp_path, "slots_test_capable_remainder"))

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    ranges = [r for r in payload["ranges"] if r["group"] == "data"]
    assert ranges[-1]["slot_start"] == 900
    assert ranges[-1]["slot_end"] == 950
    assert ranges[-1]["slot_end"] - ranges[-1]["slot_start"] == 50


def test_zarr_slots_no_resume_emits_full_ranges(tmp_path: Path) -> None:
    args = [*_required_args(tmp_path, "slots_test_capable"), "--no-resume"]
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


def test_zarr_slots_capability_check_fails_for_non_capable(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _required_args(tmp_path, "slots_test_noncapable"))

    assert result.exit_code != 0, result.output
    assert "has not opted into slot-range parallelism" in result.output


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

        def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
            assert isinstance(self.plugin_config, _SlotsCapableOptionConfig)
            assert self.plugin_config.horizon_end_iso == "2024-01-01T02:00:00Z"
            return {"data": 24}

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


def test_slots_non_capable_plugin_is_rejected(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
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
            (tmp_path / "slots.zarr").as_uri(),
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
    assert "slot-range parallelism" in result.output
