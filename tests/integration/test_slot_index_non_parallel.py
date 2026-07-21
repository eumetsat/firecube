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

"""Non-parallel ingest must still negotiate slot-index models for opt-in plugins."""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.errors import SlotIndexModelConflictError
from firecube.core.slot_index import (
    SLOT_INDEX_MODEL_ATTR,
    SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
    SlotAxis,
    SlotIndexModel,
)
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
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


def _ingest_args(
    plugin: str, product_name: str, target_path: Path, *, resume_existing: bool = False
) -> list[str]:
    args = [
        "ingest",
        plugin,
        "--target",
        f"file://{target_path}",
        "--product-name",
        product_name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
        "--option",
        "pipeline_batch_size=300",
        "--option",
        "pipeline_parallel=false",
    ]
    if resume_existing:
        args.extend(["--option", "resume_existing=true"])
    return args


def _run_ingest(plugin: str, product_name: str, target_path: Path) -> None:
    result = CliRunner().invoke(cli, _ingest_args(plugin, product_name, target_path))
    assert result.exit_code == 0, result.output


def _root_attrs(target_path: Path) -> dict[str, Any]:
    root = zarr.open_group(store=str(target_path), mode="r", zarr_format=3)
    return dict(root.attrs)


def _model(epoch: str) -> SlotIndexModel:
    return SlotIndexModel(
        name="direct_zarr_capable_fixture_v1",
        epoch=epoch,
        groups={"data": SlotAxis(cadence_s=1, mode="exact")},
    )


def test_fresh_store_stamps_model(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"
    expected = _model("2024-01-01T00:00:00Z")

    _run_ingest(
        "direct_zarr_capable_test_plugin",
        "direct_zarr_capable_test_product",
        target_path,
    )

    attrs = _root_attrs(target_path)
    assert attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR] == expected.identity_hash
    assert attrs[SLOT_INDEX_MODEL_ATTR] == expected.canonical_bytes().decode("utf-8")


def test_identical_model_reingest_is_idempotent(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"
    expected = _model("2024-01-01T00:00:00Z")

    _run_ingest(
        "direct_zarr_capable_test_plugin",
        "direct_zarr_capable_test_product",
        target_path,
    )
    first_attrs = _root_attrs(target_path)
    assert first_attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR] == expected.identity_hash

    # Second ingest with the same model — Row 2: idempotent no-op, no error.
    result = CliRunner().invoke(
        cli,
        _ingest_args(
            "direct_zarr_capable_test_plugin",
            "direct_zarr_capable_test_product",
            target_path,
            resume_existing=True,
        ),
    )
    assert result.exit_code == 0, result.output

    second_attrs = _root_attrs(target_path)
    assert second_attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR] == expected.identity_hash


def test_divergent_epoch_raises_conflict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    target_path = tmp_path / "out.zarr"
    _run_ingest(
        "direct_zarr_capable_test_plugin",
        "direct_zarr_capable_test_product",
        target_path,
    )

    def different_epoch_slot_index_model(self: Any, ctx: Any) -> SlotIndexModel:
        _ = (self, ctx)
        return _model("2025-01-01T00:00:00Z")

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "slot_index_model",
        different_epoch_slot_index_model,
    )
    with pytest.raises(SlotIndexModelConflictError):
        CliRunner().invoke(
            cli,
            _ingest_args(
                "direct_zarr_capable_test_plugin",
                "direct_zarr_capable_test_product",
                target_path,
                resume_existing=True,
            ),
            catch_exceptions=False,
        )


def test_non_declaring_plugin_unchanged(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    _run_ingest(
        "direct_zarr_non_capable_test_plugin",
        "direct_zarr_non_capable_test_product",
        target_path,
    )

    attrs = _root_attrs(target_path)
    assert SLOT_INDEX_MODEL_ATTR not in attrs
    assert SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR not in attrs
