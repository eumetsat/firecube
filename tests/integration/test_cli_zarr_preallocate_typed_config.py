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

"""Typed config must reach zarr preallocate plugin hooks."""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.types import ResolvedIndexRecord
from firecube.core.index_spec import IndexSpec, RegularTimeAxis
from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

PLUGIN = "direct_zarr_capable_test_plugin"
PRODUCT = "direct_zarr_capable_test_product"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("direct_zarr_capable_test_plugin"))
    _loader._LOADED = True
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
    ]
    for option in options:
        args.extend(["--option", option])
    return args


def _ingest_args(target_path: Path, *options: str) -> list[str]:
    args = [
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
        "no_progress=true",
        "--option",
        "pipeline_parallel=false",
    ]
    for option in options:
        args.extend(["--option", option])
    return args


def _resolved_index_record(target_path: Path) -> ResolvedIndexRecord:
    record_path = target_path / ".firecube" / "index" / "current.json"
    return ResolvedIndexRecord.from_json_bytes(record_path.read_bytes())


def test_option_reaches_plugin_hook(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    captured: dict[str, EngineConfig | None] = {}
    original_zarr_schema = plugin_module.DirectZarrCapableTestIngestor.zarr_schema

    def capturing_zarr_schema(self: Any, ctx: Any) -> Any:
        captured["engine_config"] = getattr(self, "engine_config", None)
        return original_zarr_schema(self, ctx)

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "zarr_schema",
        capturing_zarr_schema,
    )

    result = CliRunner().invoke(
        cli,
        _preallocate_args(tmp_path / "preallocated.zarr", "pipeline_batch_size=300"),
    )

    assert result.exit_code == 0, result.output
    assert captured["engine_config"] is not None
    assert captured["engine_config"].pipeline_batch_size == 300


def test_no_option_regression(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _preallocate_args(tmp_path / "preallocated.zarr"))

    assert result.exit_code == 0, result.output


def test_unknown_option_still_fails(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        _preallocate_args(tmp_path / "preallocated.zarr", "does_not_exist=foo"),
    )

    assert result.exit_code != 0


def test_preallocate_and_ingest_stamp_same_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    def batch_sized_index_spec(self: Any, ctx: Any) -> IndexSpec:
        _ = ctx
        batch_size = getattr(getattr(self, "engine_config", None), "pipeline_batch_size", 0)
        return IndexSpec(
            name=f"direct_zarr_capable_fixture_batch_{batch_size}",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=batch_size,
                )
            },
        )

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "index_spec",
        batch_sized_index_spec,
    )
    preallocate_target = tmp_path / "preallocated.zarr"
    ingest_target = tmp_path / "ingested.zarr"

    preallocate_result = CliRunner().invoke(
        cli,
        _preallocate_args(preallocate_target, "pipeline_batch_size=300"),
    )
    ingest_result = CliRunner().invoke(
        cli,
        _ingest_args(ingest_target, "pipeline_batch_size=300"),
    )

    assert preallocate_result.exit_code == 0, preallocate_result.output
    assert ingest_result.exit_code == 0, ingest_result.output
    preallocate_record = _resolved_index_record(preallocate_target)
    ingest_record = _resolved_index_record(ingest_target)
    assert preallocate_record.identity_hash == ingest_record.identity_hash
    assert "300" in str(preallocate_record.index["name"])
