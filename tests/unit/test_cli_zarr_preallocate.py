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
import re
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, ItemInfo, RegularTimeAxis, ResolvedIndex
from firecube.core.product.open import open_product
from firecube.core.runtime import resolve_storage_config
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.ingestor.api import ConfigurationError, ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.contracts.interfaces import PreallocateHost
from firecube.ingestor.registry import loader as _loader
from firecube.ingestor.runtime.configure import configure_ingestor, ensure_run_id
from firecube.ingestor.runtime.preallocate import (
    PreallocateDryRunGroup,
    SlotWindowError,
    preallocate_product,
)
from firecube.ingestor.templates.config import TemplateConfig
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor
from firecube.ingestor.types.context import IngestContext, PluginContext
from tests.helpers.rows import table_row

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    """Reset plugin discovery state so fixture plugins re-discover per test."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("direct_zarr_capable_test_plugin"))
    importlib.reload(importlib.import_module("direct_zarr_non_capable_test_plugin"))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _args(
    target: str,
    plugin: str = "direct_zarr_capable_test_plugin",
    *,
    storage_type: str = "local",
) -> list[str]:
    return [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        target,
        "--product-name",
        f"{plugin}_product",
        "--storage-type",
        storage_type,
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_preallocate_help() -> None:
    result = CliRunner().invoke(cli, ["zarr", "preallocate", "--help"])

    assert result.exit_code == 0, result.output
    assert "Examples:" in result.output
    assert (
        "idempotent" in result.output.lower()
        or "no-op" in result.output.lower()
        or "safe to re-run" in result.output.lower()
    )


def test_setup_schema_removed() -> None:
    result = CliRunner().invoke(cli, ["zarr", "setup-schema", "--help"])

    assert result.exit_code != 0
    assert "No such command" in result.output


def test_preallocate_missing_required_options() -> None:
    result = CliRunner().invoke(cli, ["zarr", "preallocate", "test_product"])

    assert result.exit_code != 0
    assert "target" in result.output.lower() or "missing" in result.output.lower()


def test_preallocate_rejects_uri_storage_type_mismatch(subtests: Any, tmp_path: Path) -> None:
    rows = (
        ("test_preallocate_rejects_file_uri_with_s3_storage_type", "file:///tmp/x.zarr", "s3"),
        ("test_preallocate_rejects_s3_uri_with_local_storage_type", "s3://bucket/x.zarr", "local"),
    )
    for row_id, target, storage_type in rows:
        with table_row(subtests, row_id, tmp_path):
            result = CliRunner().invoke(
                cli, _args(target, storage_type=storage_type), prog_name="firecube"
            )

            assert result.exit_code != 0, result.output
            assert "incompatible" in result.output


def test_preallocate_not_top_level() -> None:
    assert cli.commands.get("preallocate") is None

    result = CliRunner().invoke(cli, ["preallocate", "--help"])
    assert result.exit_code != 0
    assert "No such command" in result.output


def test_non_capable_plugin_fails(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        _args(
            f"file://{tmp_path / 'out.zarr'}",
            plugin="direct_zarr_non_capable_test_plugin",
        ),
    )

    assert result.exit_code != 0, result.output
    assert "failed to resolve index_spec" in result.output


def test_preallocate_fresh_target(tmp_path: Path) -> None:
    """Fresh target: creates arrays, exits 0."""
    target_path = tmp_path / "out.zarr"
    result = CliRunner().invoke(cli, _args(f"file://{target_path}"))

    assert result.exit_code == 0, result.output
    assert "created" in result.output
    arr = cast(Any, zarr.open_group(store=str(target_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (1000, 10)


def test_preallocate_matching_arrays_noop(tmp_path: Path) -> None:
    """Re-run on matching arrays: exits 0, logs no-op."""
    target_path = tmp_path / "matching.zarr"
    args = _args(f"file://{target_path}")

    first = CliRunner().invoke(cli, args)
    assert first.exit_code == 0, first.output

    second = CliRunner().invoke(cli, args)
    assert second.exit_code == 0, second.output
    assert "no-op" in second.output or "match the plan" in second.output
    arr = cast(Any, zarr.open_group(store=str(target_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (1000, 10)


def test_preallocate_accepts_matching_existing_array(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_path = tmp_path / "existing.zarr"
    zarr.open_group(store=str(target_path), mode="w", zarr_format=3).require_group(
        "data"
    ).create_array("data", shape=(1000, 10), dtype=np.float32, chunks=(100, 10), fill_value=0.0)

    import direct_zarr_capable_test_plugin as plugin_module

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "zarr_schema",
        lambda self, ctx: [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        shape=(1000, 10),
                        dtype=np.float32,
                        chunks=(100, 10),
                        fill_value=0.0,
                    )
                ],
            )
        ],
    )

    result = CliRunner().invoke(cli, _args(f"file://{target_path}"))

    assert result.exit_code == 0, result.output
    assert "no-op" in result.output


def test_preallocate_mismatched_existing_array_errors(subtests: Any, tmp_path: Path) -> None:
    """An existing array that differs from the plan: exits non-zero with the diff."""
    rows = (
        (
            "test_preallocate_mismatched_shape_errors",
            (5, 10),
            (100, 10),
            ("Existing arrays mismatch the plan", "shape:"),
        ),
        ("test_preallocate_mismatched_chunks_errors", (1000, 10), (10, 10), ("chunks:",)),
    )
    for row_id, shape, chunks, parts in rows:
        with table_row(subtests, row_id, tmp_path) as (workdir, _):
            target_path = workdir / "drift.zarr"
            zarr.open_group(store=str(target_path), mode="w", zarr_format=3).require_group(
                "data"
            ).create_array("data", shape=shape, dtype=np.float32, chunks=chunks, fill_value=0.0)

            result = CliRunner().invoke(cli, _args(f"file://{target_path}"))

            assert result.exit_code != 0, result.output
            for part in (*parts, "expected", "found"):
                assert part in result.output


def _preallocate_as_a_python_caller(
    target_path: Path, host: Callable[[Any], PreallocateHost] | None = None, **kwargs: Any
) -> list[str]:
    """Run ``preallocate_product`` with no Click: configure, bind, call, close.

    Returns the run ids the control plane recorded. ``run_id`` defaults to ``py-caller``;
    ``run_id=None`` omits the keyword.
    """
    import direct_zarr_capable_test_plugin as plugin_module

    uri = f"file://{target_path}"
    storage_config = resolve_storage_config(
        config_file=None,
        overrides={"storage_type": "local", "storage_driver": "fsspec"},
        set_global=False,
    )
    with open_product(
        uri,
        format="zarr",
        driver=StorageDriverConfig.from_storage_config(storage_config),
        product_name="python_caller_product",
        require_label=False,
    ) as product:
        ingestor = plugin_module.DirectZarrCapableTestIngestor(chunk_manager=product.manager)
        assert isinstance(ingestor, DirectZarrIngestor)
        plugin_ctx = configure_ingestor(ingestor, target=uri, options={}, run_id="py-caller")
        kwargs = {"run_id": "py-caller", **kwargs}
        if kwargs["run_id"] is None:  # None omits the keyword, as the CLI does
            del kwargs["run_id"]
        preallocate_product(
            product,
            ingestor if host is None else host(ingestor),
            plugin_ctx=plugin_ctx,
            plugin_name="direct_zarr_capable_test_plugin",
            storage_config=storage_config,
            write_mode="direct",
            **kwargs,
        )
        return [run.run_id for run in product.manager.list_runs(product=product.label)]


@pytest.mark.contract
@pytest.mark.integration
def test_preallocate_product_reports_through_callbacks_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    cls = plugin_module.DirectZarrCapableTestIngestor
    bounded = cls.index_spec
    unbounded = RegularTimeAxis(coordinate="timestamp", epoch="2024-01-01T00:00:00Z", cadence_s=1)

    def mixed_index(self: Any, ctx: Any) -> IndexSpec:
        spec = bounded(self, ctx)
        return IndexSpec(name=spec.name, groups={**spec.groups, "open_ended": unbounded})

    monkeypatch.setattr(cls, "index_spec", mixed_index)
    target_path = tmp_path / "out.zarr"
    reported: list[str] = []
    warned: list[str] = []

    _preallocate_as_a_python_caller(target_path, report=reported.append, warn=warned.append)

    root = zarr.open_group(store=str(target_path), mode="r", zarr_format=3)
    assert cast(Any, root["data/data"]).shape == (1000, 10)
    assert cast(Any, root["data/lat"]).shape == (10,)
    summary = json.loads(reported[-1])
    assert (summary["status"], summary["groups"]) == ("ok", {"data": 1000})
    assert len(warned) == 1 and "skipping unbounded group 'open_ended'" in warned[0]
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", "")


@pytest.mark.contract
@pytest.mark.integration
def test_preallocate_product_mints_a_prefixed_run_id_when_none_is_given(tmp_path: Path) -> None:
    named = _preallocate_as_a_python_caller(tmp_path / "named.zarr", run_id="caller-chosen")
    first = _preallocate_as_a_python_caller(tmp_path / "first.zarr", run_id=None)
    second = _preallocate_as_a_python_caller(tmp_path / "second.zarr", run_id=None)

    assert named == ["caller-chosen"]
    assert len(first) == len(second) == 1 and first != second
    assert all(re.fullmatch(r"preallocate:[0-9a-f]{16}", r) for r in (*first, *second))


@pytest.mark.parametrize("key", [None, "run_id", "manifest_run_id"])
def test_ingest_run_id_shape_is_plugin_host_and_32_hex(
    key: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOSTNAME", "h")
    ctx = IngestContext(source=".", target=".", options={key: "given-id"} if key else {})

    run_id = ensure_run_id(ctx=ctx, plugin_name="mock")

    if key:
        assert run_id == "given-id"
    else:
        other = ensure_run_id(ctx=ctx, plugin_name="mock")
        assert run_id != other
        assert all(re.fullmatch(r"mock-h-[0-9a-f]{32}", r) for r in (run_id, other))


@pytest.mark.contract
@pytest.mark.integration
def test_preallocate_product_dry_run_previews_each_group_and_writes_nothing(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "out.zarr"
    previews: list[PreallocateDryRunGroup] = []
    reported: list[str] = []

    _preallocate_as_a_python_caller(
        target_path,
        dry_run=True,
        slot_start=0,
        slot_end=500,
        on_dry_run_group=previews.append,
        report=reported.append,
    )

    assert json.loads(reported[0])["recorded_by_run_id"] == "dry-run"
    assert not (target_path / "data").exists()
    assert [(p.group, p.window) for p in previews] == [("data", (0, 500))]


@pytest.mark.contract
@pytest.mark.integration
def test_preallocate_product_runs_on_a_host_that_only_has_the_protocol_members(
    tmp_path: Path,
) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    class ForwardingHost:
        """Forwards exactly the eleven `PreallocateHost` members and nothing else."""

        time_dim_name: ClassVar[str] = plugin_module.DirectZarrCapableTestIngestor.time_dim_name

        def __init__(self, plugin: Any) -> None:
            self._plugin = plugin
            self.template_config: TemplateConfig | None = plugin.template_config

        def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
            return self._plugin.zarr_schema(ctx)

        def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
            return self._plugin.index_spec(ctx)

        def resolved_index(self, ctx: PluginContext) -> ResolvedIndex:
            return self._plugin.resolved_index(ctx)

        def bind_index_at_startup(self, ctx: PluginContext) -> None:
            self._plugin.bind_index_at_startup(ctx)

        def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
            return self._plugin.discover_source_files(ctx)

        def discovered_source_files(self, ctx: PluginContext) -> tuple[Any, ...]:
            return self._plugin.discovered_source_files(ctx)

        def stac_discovery_record(self, ctx: PluginContext) -> dict[str, Any] | None:
            return self._plugin.stac_discovery_record(ctx)

        def filter_item(self, item: Any, ctx: PluginContext) -> bool:
            return self._plugin.filter_item(item, ctx)

        def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
            return self._plugin.inspect_item(item, ctx)

    as_host: Callable[[Any], PreallocateHost] = ForwardingHost
    target_path = tmp_path / "out.zarr"

    _preallocate_as_a_python_caller(target_path, host=as_host, dry_run=False)

    assert (target_path / "data").exists()


@pytest.mark.contract
@pytest.mark.integration
def test_preallocate_product_refuses_window_beyond_extent_before_creating_arrays(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "out.zarr"

    with pytest.raises(SlotWindowError) as excinfo:
        _preallocate_as_a_python_caller(target_path, slot_start=0, slot_end=1001)

    assert isinstance(excinfo.value, ConfigurationError)
    assert "1001" in str(excinfo.value) and "1000" in str(excinfo.value)
    context = excinfo.value.context
    assert (context.group, "slot_end" in context.options) == ("data", True)
    assert not (target_path / "data").exists()
