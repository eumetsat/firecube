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

"""Integration tests for legacy ``.firecube/slot_index/current.json`` detection.

Covers the legacy-to-resolved-index migration boundary. When a cube carries
only the legacy slot-index record and has not yet produced the resolved-index
record, startup and ``firecube zarr preallocate`` must fail loud with a
rebuild-guidance error. Fresh cubes and already-migrated cubes (new record
present, regardless of legacy record) must succeed.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.manager import check_legacy_index_record
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
)
from firecube.core.errors import LegacyIndexRecordError
from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.core.product.identity import ProductIdentity
from firecube.core.slot_index import SlotAxis, SlotIndexModel
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)
from firecube.ingestor.registry import loader

pytestmark = pytest.mark.integration

PLUGIN_NAME = "legacy_slot_index_detection_test"
PRODUCT_NAME = "legacy_slot_index_detection_test_product"


def _index_spec() -> IndexSpec:
    return IndexSpec(
        name="legacy_detection_fixture_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2026-01-01T00:00:00Z",
                cadence_s=1,
                mode="exact",
                slot_count=10,
            )
        },
    )


class _DetectionPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = PRODUCT_NAME

    def __init__(self, *, chunk_manager: ChunkManager | None = None) -> None:
        super().__init__(name=PRODUCT_NAME, chunk_manager=chunk_manager)

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return _index_spec()

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        shape=(10, 4),
                        dtype=np.float32,
                        chunks=(5, 4),
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []


@pytest.fixture(autouse=True)
def _register_plugin() -> Iterator[None]:
    saved_ingestors = loader.AVAILABLE_INGESTORS.copy()
    saved_loaded = loader._LOADED
    register_ingestor(PLUGIN_NAME)(_DetectionPlugin)
    try:
        yield
    finally:
        loader.AVAILABLE_INGESTORS.clear()
        loader.AVAILABLE_INGESTORS.update(saved_ingestors)
        loader._LOADED = saved_loaded


def _manager(target_dir: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(target_dir)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name=PRODUCT_NAME),
        driver=StorageDriverConfig(driver="fsspec"),
    )
    return ChunkManager(binding=binding, workspace=target_dir.parent)


def _index_current_path(target_dir: Path) -> Path:
    return target_dir / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME


def _legacy_current_path(target_dir: Path) -> Path:
    return target_dir / ".firecube" / SLOT_INDEX_DIRNAME / SLOT_INDEX_CURRENT_FILENAME


def _plugin_ctx() -> Any:
    return SimpleNamespace(
        _ctx=object(),
        run_id="legacy-detection-run",
        storage=None,
        option=lambda key, default=None: default,
    )


def _seed_legacy_record(manager: ChunkManager) -> None:
    manager.ensure_slot_index_model(
        product=PRODUCT_NAME,
        model=SlotIndexModel(
            name="legacy_only_v1",
            epoch="2026-01-01T00:00:00Z",
            groups={"data": SlotAxis(cadence_s=1, mode="exact")},
        ),
        run_id="legacy-seed",
    )


def _preallocate_args(target_dir: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        PLUGIN_NAME,
        "--target",
        target_dir.as_uri(),
        "--product-name",
        PRODUCT_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_helper_raises_when_only_legacy_record_present(tmp_path: Path) -> None:
    target_dir = tmp_path / "legacy-only.zarr"
    manager = _manager(target_dir)
    _seed_legacy_record(manager)
    assert _legacy_current_path(target_dir).exists()
    assert not _index_current_path(target_dir).exists()

    with pytest.raises(LegacyIndexRecordError) as excinfo:
        check_legacy_index_record(
            manager,
            product=PRODUCT_NAME,
            plugin_name=PLUGIN_NAME,
        )

    message = str(excinfo.value)
    legacy_path = _legacy_current_path(target_dir)
    assert "Legacy index record detected at" in message
    assert str(legacy_path) in message
    assert "firecube zarr index rebuild" in message
    assert f"--plugin {PLUGIN_NAME}" in message


def test_helper_passes_when_both_records_present(tmp_path: Path) -> None:
    target_dir = tmp_path / "both-records.zarr"
    manager = _manager(target_dir)
    _seed_legacy_record(manager)

    from firecube.core.index_resolve import resolve_index_spec

    resolved = resolve_index_spec(_index_spec(), time_dim_name="timestamp")
    manager.ensure_resolved_index(
        product=PRODUCT_NAME,
        record=resolved.as_resolved_index_record(run_id="new-seed"),
        run_id="new-seed",
    )
    assert _legacy_current_path(target_dir).exists()
    assert _index_current_path(target_dir).exists()

    check_legacy_index_record(
        manager,
        product=PRODUCT_NAME,
        plugin_name=PLUGIN_NAME,
    )


def test_helper_passes_on_fresh_cube(tmp_path: Path) -> None:
    target_dir = tmp_path / "fresh.zarr"
    manager = _manager(target_dir)
    assert not _legacy_current_path(target_dir).exists()
    assert not _index_current_path(target_dir).exists()

    check_legacy_index_record(
        manager,
        product=PRODUCT_NAME,
        plugin_name=PLUGIN_NAME,
    )


def test_direct_zarr_startup_raises_on_legacy_only_cube(tmp_path: Path) -> None:
    target_dir = tmp_path / "startup-legacy-only.zarr"
    manager = _manager(target_dir)
    _seed_legacy_record(manager)

    ingestor = _DetectionPlugin(chunk_manager=manager)
    ctx = cast(Any, _plugin_ctx())
    ingestor._bind_index_at_startup(ctx)

    with pytest.raises(LegacyIndexRecordError) as excinfo:
        ingestor._ensure_index_record_at_startup(ctx)

    message = str(excinfo.value)
    assert "Legacy index record detected at" in message
    assert "firecube zarr index rebuild" in message
    assert not _index_current_path(target_dir).exists(), (
        "resolved-index current.json must NOT be written when legacy detection fires"
    )


def test_direct_zarr_startup_succeeds_on_fresh_cube(tmp_path: Path) -> None:
    target_dir = tmp_path / "startup-fresh.zarr"
    manager = _manager(target_dir)
    ingestor = _DetectionPlugin(chunk_manager=manager)
    ctx = cast(Any, _plugin_ctx())

    ingestor._bind_index_at_startup(ctx)
    ingestor._ensure_index_record_at_startup(ctx)

    assert _index_current_path(target_dir).exists()
    assert not _legacy_current_path(target_dir).exists()


def test_preallocate_cli_fails_on_legacy_only_cube(tmp_path: Path) -> None:
    target_dir = tmp_path / "preallocate-legacy-only.zarr"
    manager = _manager(target_dir)
    _seed_legacy_record(manager)
    manager.close()

    result = CliRunner().invoke(cli, _preallocate_args(target_dir))

    assert result.exit_code != 0, result.output
    assert "Legacy index record detected at" in result.output
    assert "firecube zarr index rebuild" in result.output
    assert f"--plugin {PLUGIN_NAME}" in result.output
    assert not _index_current_path(target_dir).exists()


def test_preallocate_cli_succeeds_on_fresh_cube(tmp_path: Path) -> None:
    target_dir = tmp_path / "preallocate-fresh.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target_dir))

    assert result.exit_code == 0, result.output
    assert _index_current_path(target_dir).exists()
    assert not _legacy_current_path(target_dir).exists()
