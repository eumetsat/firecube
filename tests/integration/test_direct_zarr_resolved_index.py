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
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
    ResolvedIndexRecord,
)
from firecube.core.index_resolve import resolve_index_spec
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

PLUGIN_NAME = "direct_zarr_resolved_index_test"
PRODUCT_NAME = "direct_zarr_resolved_index_test_product"


def _index_spec(*, name: str = "resolved_index_fixture_v1", size: int = 10) -> IndexSpec:
    return IndexSpec(
        name=name,
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


class _ResolvedIndexPlugin(DirectZarrIngestor):
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
    register_ingestor(PLUGIN_NAME)(_ResolvedIndexPlugin)
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
        run_id="startup-run",
        storage=None,
        option=lambda key, default=None: default,
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


def _slots_args(target_dir: Path) -> list[str]:
    return [
        "zarr",
        "slots",
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
        "--no-resume",
    ]


def test_direct_zarr_startup_writes_resolved_index_only(tmp_path: Path) -> None:
    target_dir = tmp_path / "startup.zarr"
    manager = _manager(target_dir)
    ingestor = _ResolvedIndexPlugin(chunk_manager=manager)
    ctx = cast(Any, _plugin_ctx())

    ingestor._bind_index_at_startup(ctx)
    ingestor._ensure_index_record_at_startup(ctx)

    current_json = _index_current_path(target_dir)
    assert current_json.exists()
    assert not _legacy_current_path(target_dir).exists()
    record = ResolvedIndexRecord.from_json_bytes(current_json.read_bytes())
    assert record.recorded_by_run_id == "startup-run"
    assert record.identity_hash == ingestor.resolved_index(ctx).identity_hash


def test_preallocate_writes_resolved_index_and_rerun_matches_without_rewrite(
    tmp_path: Path,
) -> None:
    target_dir = tmp_path / "preallocated.zarr"
    args = _preallocate_args(target_dir)

    first = CliRunner().invoke(cli, args)
    assert first.exit_code == 0, first.output
    assert "resolved index: created" in first.output
    current_json = _index_current_path(target_dir)
    assert current_json.exists()
    assert not _legacy_current_path(target_dir).exists()
    first_bytes = current_json.read_bytes()
    first_mtime_ns = current_json.stat().st_mtime_ns

    second = CliRunner().invoke(cli, args)
    assert second.exit_code == 0, second.output
    assert "resolved index: matched_existing" in second.output
    assert current_json.read_bytes() == first_bytes
    assert current_json.stat().st_mtime_ns == first_mtime_ns
    assert not _legacy_current_path(target_dir).exists()


def test_zarr_slots_resolved_index_staleness_tri_state(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    runner = CliRunner()

    fresh_target = tmp_path / "state-c-fresh.zarr"
    fresh = runner.invoke(cli, _slots_args(fresh_target))
    fresh_fd = capfd.readouterr()
    assert fresh.exit_code == 0, fresh.output
    assert "resolved index" not in fresh.output.lower()
    assert "legacy slot-index" not in fresh.output.lower()
    assert "resolved index" not in fresh_fd.err.lower()
    assert "legacy slot-index" not in fresh_fd.err.lower()

    legacy_target = tmp_path / "state-b-legacy.zarr"
    legacy_manager = _manager(legacy_target)
    legacy_manager.ensure_slot_index_model(
        product=PRODUCT_NAME,
        model=SlotIndexModel(
            name="legacy_only_v1",
            epoch="2026-01-01T00:00:00Z",
            groups={"data": SlotAxis(cadence_s=1, mode="exact")},
        ),
        run_id="legacy-seed",
    )
    legacy = runner.invoke(cli, _slots_args(legacy_target))
    legacy_fd = capfd.readouterr()
    assert legacy.exit_code == 0, legacy.output
    assert "legacy slot-index record detected" not in legacy.output.lower()
    assert "legacy slot-index record detected" in legacy_fd.err.lower()
    assert "firecube zarr index rebuild" in legacy_fd.err.lower()

    stale_target = tmp_path / "state-a-stale.zarr"
    stale_manager = _manager(stale_target)
    stale_record = resolve_index_spec(
        _index_spec(name="stale_v1", size=11), time_dim_name="timestamp"
    ).as_resolved_index_record(run_id="stale-seed")
    stale_manager.ensure_resolved_index(
        product=PRODUCT_NAME,
        record=stale_record,
        run_id="stale-seed",
    )
    stale = runner.invoke(cli, _slots_args(stale_target))
    stale_fd = capfd.readouterr()
    assert stale.exit_code == 0, stale.output
    assert "plugin resolved index differs from persisted record" not in stale.output.lower()
    assert "plugin resolved index differs from persisted record" in stale_fd.err.lower()
    assert "firecube zarr index rebuild" in stale_fd.err.lower()
