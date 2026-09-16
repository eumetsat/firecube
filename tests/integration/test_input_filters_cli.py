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
from pathlib import Path
from typing import ClassVar

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import GenericZarrIngestor
from firecube.ingestor.registry import loader
from tests.integration.test_slots_auto_input_data import (
    PLUGIN_NAME as SLOT_PLUGIN,
)
from tests.integration.test_slots_auto_input_data import (
    _SlotsAutoInputDataIngestor,
)

pytestmark = pytest.mark.integration


class _FilterReader(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "input_filter_reader"
    name = "input_filter_reader"
    time_dim_name: ClassVar[str] = "time"

    def build_dataset(self, group, items, ctx):
        datasets = []
        for item in items:
            path = ctx.materialize(item)
            if path.suffix == ".csv":
                time, value = path.read_text().strip().split(",")
                datasets.append(
                    xr.Dataset(
                        {"value": ("time", [float(value)])},
                        coords={"time": [np.datetime64(time, "ns")]},
                    )
                )
            else:
                with xr.open_dataset(path) as ds:
                    datasets.append(ds.load())
        return xr.concat(datasets, dim="time").sortby("time")


@pytest.fixture(autouse=True)
def registered(monkeypatch):
    monkeypatch.setattr(loader, "_LOADED", True)
    monkeypatch.setitem(loader.AVAILABLE_INGESTORS, _FilterReader.name, _FilterReader)
    monkeypatch.setitem(loader.AVAILABLE_INGESTORS, SLOT_PLUGIN, _SlotsAutoInputDataIngestor)


def args(tmp_path, command="ingest", plugin=None):
    config = tmp_path / "config.toml"
    if not config.exists():
        config.write_text("")
    prefix = ["ingest"] if command == "ingest" else ["zarr", command]
    return [
        "--config-file",
        str(config),
        *prefix,
        plugin or (_FilterReader.name if command == "ingest" else SLOT_PLUGIN),
        "--input-data",
        str(tmp_path / "inputs"),
        "--target",
        (tmp_path / "output.zarr").as_uri(),
        "--product-name",
        "filtered",
        "--write-mode",
        "direct",
    ]


def test_native_filters_select_written_values(tmp_path):
    source = tmp_path / "inputs"
    source.mkdir()
    for name, date, value in [
        ("mynetcdf.nc", "2026-01-01", 1.0),
        ("myothernetcdf.nc", "2026-01-02", 99.0),
    ]:
        xr.Dataset(
            {"value": ("time", [value])}, coords={"time": [np.datetime64(date, "ns")]}
        ).to_netcdf(source / name)
    (source / "measurement.csv").write_text("2026-01-03,3")
    (source / "draft_01.csv").write_text("2026-01-04,99")
    (source / "sample.hdf").write_bytes(b"must not reach the reader")
    result = CliRunner().invoke(
        cli,
        [
            *args(tmp_path),
            "--input-filters",
            '["*.csv","!myothernetcdf.nc","!*.hdf","!draft_*.csv"]',
        ],
    )
    assert result.exit_code == 0, result.output
    with xr.open_zarr(tmp_path / "output.zarr", group="default", consolidated=False) as actual:
        np.testing.assert_array_equal(actual.value.values, [1.0, 3.0])


def test_show_options_names_the_native_flag():
    result = CliRunner().invoke(cli, ["ingest", _FilterReader.name, "--show-options"])
    assert result.exit_code == 0, result.output
    assert "--input-filters" in result.output
    assert "--option input_filters" not in result.output
    assert "include_patterns" not in result.output


@pytest.mark.parametrize("command", ["ingest", "slots", "preallocate"])
@pytest.mark.parametrize(
    "value", ["*.csv", "[*.csv]", "null", "{}", "[1]", "[null]", '[""]', '["!"]']
)
def test_native_flag_rejects_invalid_values_before_output(tmp_path, command, value):
    result = CliRunner().invoke(cli, [*args(tmp_path, command), "--input-filters", value])
    assert result.exit_code == 2, result.output
    assert "--input-filters" in result.output
    assert "quoted JSON list" in result.output
    assert not (tmp_path / "output.zarr").exists()


@pytest.mark.parametrize("command", ["ingest", "slots", "preallocate"])
@pytest.mark.parametrize("key", ["include_patterns", "input_filters"])
def test_option_spelling_rejected(tmp_path, command, key):
    result = CliRunner().invoke(cli, [*args(tmp_path, command), "--option", f'{key}=["*.csv"]'])
    assert result.exit_code == 2, result.output
    assert "--input-filters" in result.output
    assert not (tmp_path / "output.zarr").exists()


@pytest.mark.parametrize("command", ["ingest", "slots", "preallocate"])
def test_repeated_flag_rejected(tmp_path, command):
    result = CliRunner().invoke(
        cli, [*args(tmp_path, command), "--input-filters", "[]", "--input-filters", '["*.nc"]']
    )
    assert result.exit_code == 2, result.output
    assert "once" in result.output


@pytest.mark.parametrize("command", ["ingest", "slots", "preallocate"])
@pytest.mark.parametrize("override", [None, [], ["!other.nc"]])
def test_configuration_precedence_reaches_discovery(tmp_path, monkeypatch, command, override):
    source = tmp_path / "inputs"
    source.mkdir()
    (source / "keep.nc").write_bytes(b"keep")
    (source / "other.nc").write_bytes(b"other")
    plugin = _FilterReader.name if command == "ingest" else SLOT_PLUGIN
    (tmp_path / "config.toml").write_text(f'[plugins.{plugin}]\ninput_filters = ["!keep.nc"]\n')
    captured = []

    # Exercise real discovery; stop at the reader/inspection boundary so the same
    # test verifies ingest, index planning, and preallocation without fake writes.
    def stop_at_reader(self, *values):
        ctx = values[-1]
        captured.append(list(ctx.option("input_filters")))
        raise RuntimeError("input filter probe reached reader")

    monkeypatch.setattr(_FilterReader, "build_dataset", stop_at_reader)
    seen = []

    def inspect(self, item, ctx):
        captured.append(list(ctx.option("input_filters")))
        seen.append(Path(item).name)
        # Unique timestamps if [] clears exclusions.
        from firecube.core.api import ItemInfo

        return ItemInfo(
            coordinate=np.datetime64(
                "2026-01-01" if Path(item).name == "keep.nc" else "2026-01-02", "ns"
            )
        )

    monkeypatch.setattr(_SlotsAutoInputDataIngestor, "inspect_item", inspect)
    command_args = args(tmp_path, command)
    if command == "preallocate":
        command_args += ["--dry-run"]
    if override is not None:
        command_args += ["--input-filters", json.dumps(override)]
    result = CliRunner().invoke(cli, command_args)
    assert captured, result.output
    expected = ["!keep.nc"] if override is None else override
    assert all(value == expected for value in captured)
    if command != "ingest":
        assert result.exit_code == 0, result.output
        assert set(seen) == (
            {"other.nc"}
            if override is None
            else {"keep.nc", "other.nc"}
            if not override
            else {"keep.nc"}
        )


@pytest.mark.parametrize("command", ["ingest", "slots", "preallocate"])
def test_legacy_config_rejected_even_with_new_flag(tmp_path, command):
    (tmp_path / "inputs").mkdir()
    plugin = _FilterReader.name if command == "ingest" else SLOT_PLUGIN
    (tmp_path / "config.toml").write_text(f"[plugins.{plugin}]\ninclude_patterns = []\n")
    result = CliRunner().invoke(cli, [*args(tmp_path, command), "--input-filters", "[]"])
    assert result.exit_code != 0, result.output
    assert "include_patterns has been removed" in result.output
    assert not (tmp_path / "output.zarr").exists()


def test_custom_discovery_keeps_ownership(tmp_path, monkeypatch):
    source = tmp_path / "inputs"
    source.mkdir()
    item = source / "keep.nc"
    item.touch()
    monkeypatch.setattr(
        _SlotsAutoInputDataIngestor, "discover_source_files", lambda self, ctx: [item]
    )
    result = CliRunner().invoke(cli, [*args(tmp_path, "slots"), "--input-filters", '["!*"]'])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["ranges"]
