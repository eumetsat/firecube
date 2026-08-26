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

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.core.controlplane import ChunkManager
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import GenericZarrIngestor, PluginContext, register_ingestor
from tests.helpers.storage import make_test_binding, make_test_context


@register_ingestor("test_pipeline_manifest")
class PipelineIngestorForTesting(GenericZarrIngestor):
    PRODUCT_NAME = "test_pipeline_manifest"
    name = "test_pipeline_manifest"

    def build_dataset(self, group: str, items: list[object], ctx: PluginContext):
        _ = (group, items, ctx)
        dims = ["timestamp", "lat", "lon"]
        dates = pd.date_range("2024-01-01", periods=2)
        data = np.random.rand(2, 10, 10)
        return xr.Dataset(
            {"data": (dims, data)},
            coords={
                "timestamp": dates,
                "lat": np.arange(10),
                "lon": np.arange(10),
            },
        )


@pytest.mark.parametrize("pipeline_workers", [2, 1])
def test_pipeline_manifest_creation(tmp_path, pipeline_workers):
    """Execution creates WAL immediately and snapshots only on explicit rebuild."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "input.nc").touch()

    target_dir = (tmp_path / "output.zarr").resolve()
    target_dir.mkdir()
    ctx = make_test_context(
        target_dir.parent,
        source=str(source_dir),
        product=target_dir.name,
        options={
            "pipeline_workers": pipeline_workers,
            "pipeline_batch_size": 1,
            "include_patterns": ["*.nc"],
            "write_mode": "direct",
        },
    )

    ingestor = PipelineIngestorForTesting()
    result = ingestor.run(ctx)
    assert result.output_path

    output_path = Path(StorageUri.parse(str(result.output_path)).path)
    control_root = output_path / ".firecube"
    assert control_root.exists(), f"Control root not found at {control_root}"
    assert (control_root / "schema.json").exists()
    assert not (control_root / "LATEST.json").exists()

    runs_dir = control_root / "runs"
    assert any(runs_dir.iterdir()), "Expected at least one run WAL directory"

    manager = ChunkManager(
        binding=make_test_binding(target_dir.parent), workspace=tmp_path / "cm-work"
    )
    rebuild = manager.rebuild_snapshot(target_dir.name)

    assert rebuild["records"] >= 1
    assert (control_root / "LATEST.json").exists()
    assert any((control_root / "snapshots").glob("snapshot-*.jsonl"))
