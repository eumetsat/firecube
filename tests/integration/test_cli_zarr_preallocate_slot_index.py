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

"""Integration tests for ``firecube zarr preallocate`` typed options and slot-index model.

Covers:

* ``--option`` flag plumbs values into ``PluginContext.options`` for
  ``slot_index_model(ctx)`` to consume.
* Typed coercion of int-typed engine options: ``pipeline_workers=7`` arrives
  inside the plugin as ``int(7)``, not ``str("7")``.
* Unknown option keys (outside the experimental ``x_*`` namespace) are
  rejected by ``coerce_options_for_plugin`` before any control-plane or
  Zarr write happens.
* ``x_*`` experimental keys pass through unchanged.
* A ``slot_index_model(ctx)`` failure exits non-zero AND leaves the target
  Zarr store empty (no ``zarr.json`` files, no ``.firecube/slot_index/current.json``).
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.index_spec import IndexSpec, RegularTimeAxis
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    """Reset plugin discovery state so fixture plugins re-discover per test."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("direct_zarr_capable_test_plugin"))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _args(target: str, *extra: str) -> list[str]:
    return [
        "zarr",
        "preallocate",
        "direct_zarr_capable_test_plugin",
        "--target",
        target,
        "--product-name",
        "direct_zarr_capable_test_product",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        *extra,
    ]


def _current_json_path(target_dir: Path) -> Path:
    return target_dir / ".firecube" / "slot_index" / "current.json"


def _zarr_json_files(target_dir: Path) -> list[Path]:
    if not target_dir.exists():
        return []
    return list(target_dir.rglob("zarr.json"))


def test_option_forwards_to_plugin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target_dir = tmp_path / "out.zarr"
    import direct_zarr_capable_test_plugin as plugin_module

    def index_spec_using_option(self, ctx):
        marker = str(ctx.options.get("x_test_key", "default"))
        return IndexSpec(
            name=f"forwarded_{marker}_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    size=10,
                )
            },
        )

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "index_spec",
        index_spec_using_option,
    )

    result = CliRunner().invoke(
        cli,
        _args(f"file://{target_dir}", "--option", "x_test_key=hello"),
    )

    assert result.exit_code == 0, result.output
    current_json = _current_json_path(target_dir)
    assert current_json.exists(), "slot_index/current.json not written"
    record = json.loads(current_json.read_text())
    assert record["model"]["name"] == "forwarded_hello_v1", record


def test_typed_coercion_for_int_option(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target_dir = tmp_path / "out.zarr"
    import direct_zarr_capable_test_plugin as plugin_module

    seen: dict[str, object] = {}

    def index_spec_capturing_type(self, ctx):
        value = ctx.options.get("pipeline_workers")
        seen["value"] = value
        seen["type"] = type(value).__name__
        return IndexSpec(
            name="typed_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    size=10,
                )
            },
        )

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "index_spec",
        index_spec_capturing_type,
    )

    result = CliRunner().invoke(
        cli,
        _args(f"file://{target_dir}", "--option", "pipeline_workers=7"),
    )

    assert result.exit_code == 0, result.output
    assert seen["value"] == 7, f"expected int 7, got {seen!r}"
    assert seen["type"] == "int", f"expected int, got {seen['type']!r} ({seen['value']!r})"


def test_unknown_option_rejected_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_dir = tmp_path / "out.zarr"
    import direct_zarr_capable_test_plugin as plugin_module

    called = {"index_spec": 0}

    def index_spec_should_not_run(self, ctx):
        called["index_spec"] += 1
        return IndexSpec(
            name="should_not_run_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    size=10,
                )
            },
        )

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "index_spec",
        index_spec_should_not_run,
    )

    result = CliRunner().invoke(
        cli,
        _args(f"file://{target_dir}", "--option", "totally_made_up_key=x"),
    )

    assert result.exit_code != 0, result.output
    assert called["index_spec"] == 0, "plugin was invoked despite unknown option"
    assert not _current_json_path(target_dir).exists(), (
        "slot_index/current.json must not be created when --option is rejected"
    )
    assert _zarr_json_files(target_dir) == [], (
        f"no Zarr metadata should exist after fail-fast, found {_zarr_json_files(target_dir)!r}"
    )


def test_x_namespace_passthrough(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target_dir = tmp_path / "out.zarr"
    import direct_zarr_capable_test_plugin as plugin_module

    received: dict[str, object] = {}

    def index_spec_reading_x_key(self, ctx):
        received["flag"] = ctx.options.get("x_experimental_flag")
        return IndexSpec(
            name="x_namespace_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    size=10,
                )
            },
        )

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "index_spec",
        index_spec_reading_x_key,
    )

    result = CliRunner().invoke(
        cli,
        _args(f"file://{target_dir}", "--option", "x_experimental_flag=true"),
    )

    assert result.exit_code == 0, result.output
    assert received["flag"] == "true", (
        f"expected raw 'true' string in x_* namespace (no typed coercion), got {received!r}"
    )


def test_slot_index_model_failure_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_dir = tmp_path / "out.zarr"
    import direct_zarr_capable_test_plugin as plugin_module

    def index_spec_raises(self, ctx):
        raise ConfigurationError("test-induced index_spec failure")

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "index_spec",
        index_spec_raises,
    )

    result = CliRunner().invoke(cli, _args(f"file://{target_dir}"))

    assert result.exit_code != 0, result.output
    assert "index_spec" in result.output, (
        f"expected the error to mention index_spec; got: {result.output!r}"
    )
    assert _zarr_json_files(target_dir) == [], (
        f"target must contain NO zarr.json after index_spec failure, found "
        f"{_zarr_json_files(target_dir)!r}"
    )
    assert not _current_json_path(target_dir).exists(), (
        "slot_index/current.json must not be created when index_spec raises"
    )
    assert "Traceback" not in result.output
    assert "ConfigurationError" not in result.output


def test_preallocate_help_lists_new_flags() -> None:
    result = CliRunner().invoke(cli, ["zarr", "preallocate", "--help"])

    assert result.exit_code == 0, result.output
    assert "--option" in result.output
    assert "--input-data" in result.output
