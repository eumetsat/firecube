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

import importlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import WriteIntent, ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.registry import loader as _loader

pytestmark = [pytest.mark.integration, pytest.mark.contract]

PLUGIN = "direct_zarr_capable_test_plugin"
PRODUCT = "direct_zarr_capable_test_product"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module(PLUGIN))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(target_path: Path, *options: str) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    for option in options:
        args.extend(["--option", option])
    return args


def _ingest_args(target_path: Path) -> list[str]:
    return [
        "ingest",
        PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "resume_existing=true",
        "--option",
        "no_progress=true",
    ]


def _root(target_path: Path) -> Any:
    return zarr.open_group(store=str(target_path), mode="r", zarr_format=3)


def test_declared_attrs_written(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(target_path))

    assert result.exit_code == 0, result.output
    root = _root(target_path)
    data = cast(Any, root["data/data"])
    lat = cast(Any, root["data/lat"])
    assert data.attrs["long_name"] == "test data"
    assert data.attrs["units"] == "K"
    assert lat.attrs["units"] == "degrees_north"
    assert lat.attrs["standard_name"] == "latitude"


def test_round_trip_preallocate_to_ingest(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    preallocate_result = CliRunner().invoke(cli, _preallocate_args(target_path))
    ingest_result = CliRunner().invoke(cli, _ingest_args(target_path))

    assert preallocate_result.exit_code == 0, preallocate_result.output
    assert ingest_result.exit_code == 0, ingest_result.output
    assert "SchemaDriftError" not in ingest_result.output


def test_reserved_attr_fails_loudly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    def bad_zarr_schema(self: Any, ctx: Any) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        chunks=(100, 10),
                        shape=(1000, 10),
                        dtype="float32",
                        attrs={"_ARRAY_DIMENSIONS": ["x"]},
                    )
                ],
            )
        ]

    monkeypatch.setattr(plugin_module.DirectZarrCapableTestIngestor, "zarr_schema", bad_zarr_schema)

    with pytest.raises(ValueError, match=r"Reserved attr '_ARRAY_DIMENSIONS'"):
        CliRunner().invoke(
            cli,
            _preallocate_args(tmp_path / "out.zarr"),
            catch_exceptions=False,
        )


def test_dimension_names_written(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(target_path))

    assert result.exit_code == 0, result.output
    root = _root(target_path)
    data = cast(Any, root["data/data"])
    lat = cast(Any, root["data/lat"])
    assert data.metadata.dimension_names == ("timestamp", "x")
    assert lat.metadata.dimension_names == ("lat",)


# --- group attributes on the preallocate path and attributes on the root group "/" ----------

_GROUP_ATTRS = {"title": "root cube", "institution": "EUMETSAT"}


def _declare_group(monkeypatch: pytest.MonkeyPatch, group: str, attrs: dict[str, Any]) -> None:
    """Swap the fixture plugin's single group for ``group`` carrying ``attrs``."""
    import direct_zarr_capable_test_plugin as plugin_module

    cls = plugin_module.DirectZarrCapableTestIngestor
    original_schema = cls.zarr_schema
    original_index = cls.index_spec

    def zarr_schema(self: Any, ctx: Any) -> list[ZarrGroupSpec]:
        (spec,) = original_schema(self, ctx)
        return [ZarrGroupSpec(group=group, arrays=spec.arrays, attrs=attrs)]

    def index_spec(self: Any, ctx: Any) -> Any:
        spec = original_index(self, ctx)
        (axis,) = spec.groups.values()
        return type(spec)(name=spec.name, groups={group: axis})

    def build_write_intents(self: Any, batch: Any, ctx: Any) -> list[Any]:
        resolved = self.resolved_index(ctx)
        intents: list[Any] = []
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            intents.append(
                WriteIntent(
                    group=group,
                    array="data",
                    ts_index=resolved.position(group, info.coordinate),
                    data=np.full((10,), float(item), dtype="float32"),
                    kind="1d",
                )
            )
        intents.append(
            WriteIntent(
                group=group,
                array="lat",
                ts_index=0,
                data=np.arange(10, dtype=np.float64),
                kind="static",
            )
        )
        return intents

    monkeypatch.setattr(cls, "zarr_schema", zarr_schema)
    monkeypatch.setattr(cls, "index_spec", index_spec)
    monkeypatch.setattr(cls, "build_write_intents", build_write_intents)


def _group_attributes(target_path: Path, group: str) -> dict[str, Any]:
    zarr_json = target_path / group / "zarr.json" if group != "/" else target_path / "zarr.json"
    return json.loads(zarr_json.read_text())["attributes"]


def test_repro_root_attrs_window_group_attrs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A named group's declared attrs reach its zarr.json after preallocate and a window pod."""
    _declare_group(monkeypatch, "data", _GROUP_ATTRS)
    target_path = tmp_path / "out.zarr"

    prealloc = CliRunner().invoke(cli, _preallocate_args(target_path))
    window = CliRunner().invoke(
        cli, [*_ingest_args(target_path), "--slot-start", "0", "--slot-end", "100"]
    )

    assert prealloc.exit_code == 0, prealloc.output
    assert window.exit_code == 0, window.output
    assert _group_attributes(target_path, "data") == _GROUP_ATTRS


def test_repro_root_attrs_preallocate_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Preallocate accepts plugin attrs on the root group and keeps the index mirror beside them."""
    _declare_group(monkeypatch, "/", _GROUP_ATTRS)
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(target_path))

    assert result.exit_code == 0, result.output
    attributes = _group_attributes(target_path, "/")
    assert {k: attributes.get(k) for k in _GROUP_ATTRS} == _GROUP_ATTRS
    assert "firecube_resolved_index" in attributes
    assert "firecube_resolved_index_identity_hash" in attributes


def test_repro_root_attrs_serial_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A serial direct ingest of a root-group plugin keeps its attrs and the index mirror."""
    _declare_group(monkeypatch, "/", _GROUP_ATTRS)
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _ingest_args(target_path))

    assert result.exit_code == 0, result.output
    attributes = _group_attributes(target_path, "/")
    assert {k: attributes.get(k) for k in _GROUP_ATTRS} == _GROUP_ATTRS
    assert "firecube_resolved_index" in attributes
    assert "firecube_resolved_index_identity_hash" in attributes


def test_repro_root_attrs_staged_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Staged promotion of a root-group plugin keeps its attrs and the index mirror."""
    _declare_group(monkeypatch, "/", _GROUP_ATTRS)
    target_path = tmp_path / "out.zarr"
    args = _ingest_args(target_path)
    args[args.index("direct")] = "staged"

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    attributes = _group_attributes(target_path, "/")
    assert {k: attributes.get(k) for k in _GROUP_ATTRS} == _GROUP_ATTRS
    assert "firecube_resolved_index" in attributes
    assert "firecube_resolved_index_identity_hash" in attributes


def _add_groups(monkeypatch: pytest.MonkeyPatch, *extra: ZarrGroupSpec) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    cls = plugin_module.DirectZarrCapableTestIngestor
    declared = cls.zarr_schema
    monkeypatch.setattr(cls, "zarr_schema", lambda self, ctx: [*extra, *declared(self, ctx)])


def _outside_control_plane(target_path: Path, command: str, expect: str) -> list[str]:
    build = _preallocate_args if command == "preallocate" else _ingest_args
    result = CliRunner().invoke(cli, build(target_path))
    assert result.exit_code != 0
    assert expect in result.output
    return [p.name for p in target_path.glob("*") if p.name != ".firecube"]


@pytest.mark.parametrize("command", ["preallocate", "ingest"])
def test_guard_root_reserved_attr_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    _declare_group(monkeypatch, "/", {"firecube_resolved_index": {"forged": True}})

    written = _outside_control_plane(tmp_path / "o.zarr", command, "firecube_resolved_index")

    assert written == []


@pytest.mark.parametrize("command", ["preallocate", "ingest"])
def test_guard_root_beside_named_group_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    _declare_group(monkeypatch, "/", {})
    _add_groups(monkeypatch, ZarrGroupSpec(group="extra", arrays=[]))

    assert _outside_control_plane(tmp_path / "o.zarr", command, "beside named groups") == []


def test_preallocate_parent_group_attrs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _declare_group(monkeypatch, "science/data", {})
    _add_groups(
        monkeypatch,
        ZarrGroupSpec(group="science", arrays=[], attrs={"title": "parent"}),
        ZarrGroupSpec(group="aux/info", arrays=[], attrs={"comment": "nested"}),
    )
    target_path = tmp_path / "out.zarr"

    for rerun in (False, True):
        result = CliRunner().invoke(cli, _preallocate_args(target_path))
        assert result.exit_code == 0, result.output
        assert _group_attributes(target_path, "science") == {"title": "parent"}
        assert _group_attributes(target_path, "aux/info") == {"comment": "nested"}
        for group in () if rerun else ("science", "aux/info"):
            (zarr_json := target_path / group / "zarr.json").write_text(
                json.dumps({**json.loads(zarr_json.read_text()), "attributes": {}})
            )
