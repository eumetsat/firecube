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

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import zarr

from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    IndexedRegionStrategy,
)
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.types.context import PipelineBatch, PluginContext


class _StubDirectIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "stub_direct"
    name = "stub_direct"

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data_1km",
                arrays=[
                    ZarrArraySpec(
                        name="counts",
                        shape=(0, 100, 100),
                        dtype=np.float32,
                        chunks=(1, 100, 100),
                        fill_value=np.nan,
                    ),
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(0,),
                        dtype="datetime64[s]",
                        chunks=(256,),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return [
            WriteIntent(
                group="data_1km",
                array="timestamp",
                ts_index=0,
                data=None,
                kind="timestamp",
                timestamp_val=np.datetime64("2025-01-01T00:00:00", "s"),
            ),
            WriteIntent(
                group="data_1km",
                array="counts",
                ts_index=0,
                data=np.ones((50, 100), dtype=np.float32),
                kind="region",
                y_slice=slice(0, 50),
            ),
        ]


class TestDirectZarrIngestorAbstract:
    def test_cannot_instantiate_without_hooks(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            DirectZarrIngestor(name="broken")  # type: ignore[abstract]

    def test_subclass_with_hooks_instantiates(self) -> None:
        ingestor = _StubDirectIngestor()
        assert ingestor.name == "stub_direct"

    def test_aggregate_metrics_merges_batch_results(self) -> None:
        ingestor = _StubDirectIngestor()
        ctx = MagicMock()
        ctx.output_format = "zarr"
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=["f1.nc"])
        state = MagicMock()
        state.results = (
            MagicMock(
                success=True,
                metrics={
                    "count": 2,
                    "zarr": {"coverage": [{"group": "data_1km"}]},
                    "storage_handled": True,
                },
                duration_s=1.25,
                cpu_time_s=0.5,
                io_time_s=0.25,
                batch=batch,
            ),
        )

        result = ingestor._aggregate_metrics(ctx, state)

        assert result["count"] == 2
        assert result["zarr"]["coverage"] == [{"group": "data_1km"}]


class TestDirectZarrProcessBatch:
    def test_empty_intents_returns_success_with_zero_count(self) -> None:
        class _EmptyIngestor(DirectZarrIngestor):
            PRODUCT_NAME = "empty"
            name = "empty"

            def zarr_schema(self, ctx):
                return []

            def build_write_intents(self, batch, ctx):
                return []

        ingestor = _EmptyIngestor()
        ingestor.engine_config = MagicMock(write_mode="direct")
        ctx = MagicMock(spec=PluginContext)
        ctx.output_name = "test_product"
        ctx.run_id = "run-001"
        ctx.option = MagicMock(return_value="unknown")
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=[])

        with patch.object(ingestor, "resolve_output_uri", return_value="/tmp/test.zarr"):
            result = ingestor._process_batch(batch, ctx)

        assert result.success is True
        assert result.metrics["count"] == 0

    def test_process_batch_error_returns_failure(self) -> None:
        ingestor = _StubDirectIngestor()
        ingestor.engine_config = MagicMock(write_mode="direct")
        ctx = MagicMock(spec=PluginContext)
        ctx.output_name = "test_product"
        ctx.run_id = "run-001"
        ctx.option = MagicMock(return_value="unknown")
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=["f1.nc"])

        with patch.object(ingestor, "resolve_output_uri", side_effect=RuntimeError("boom")):
            result = ingestor._process_batch(batch, ctx)

        assert result.success is False
        assert result.error is not None
        assert "boom" in result.error


class TestWriteIntentDataclass:
    def test_defaults(self) -> None:
        intent = WriteIntent(group="g", array="a", ts_index=0, data=np.array([1]))
        assert intent.kind == "region"
        assert intent.y_slice is None
        assert intent.channel_index is None
        assert intent.timestamp_val is None

    def test_frozen(self) -> None:
        intent = WriteIntent(group="g", array="a", ts_index=0, data=np.array([1]))
        with pytest.raises(AttributeError):
            intent.group = "other"  # type: ignore[misc]


class TestZarrSpecDataclasses:
    def test_array_spec_fields(self) -> None:
        spec = ZarrArraySpec(name="counts", shape=(10, 100), dtype=np.float32)
        assert spec.name == "counts"
        assert spec.chunks is None
        assert spec.fill_value is None

    def test_group_spec_defaults(self) -> None:
        spec = ZarrGroupSpec(group="data_1km")
        assert spec.arrays == []
        assert spec.coord_names == frozenset({"y", "x", "channel"})


class TestIndexedRegionStrategy:
    def test_dispatch_unknown_kind_raises(self) -> None:
        writer = MagicMock()
        intent = MagicMock(kind="invalid_kind")
        with pytest.raises(ValueError, match="Unknown WriteIntent kind"):
            IndexedRegionStrategy._dispatch_intent(writer, intent)

    def test_dispatch_region(self) -> None:
        writer = MagicMock()
        intent = WriteIntent(
            group="g",
            array="counts",
            ts_index=0,
            data=np.zeros((10, 10)),
            kind="region",
            y_slice=slice(0, 10),
            channel_index=2,
        )
        IndexedRegionStrategy._dispatch_intent(writer, intent)
        writer.write_region.assert_called_once_with(
            group="g",
            array_name="counts",
            ts_index=0,
            y_slice=slice(0, 10),
            data=intent.data,
            channel_index=2,
        )

    def test_dispatch_1d(self) -> None:
        writer = MagicMock()
        intent = WriteIntent(
            group="g",
            array="calibration",
            ts_index=5,
            data=np.array([1.0, 2.0]),
            kind="1d",
        )
        IndexedRegionStrategy._dispatch_intent(writer, intent)
        writer.write_1d.assert_called_once_with(
            group="g",
            array_name="calibration",
            ts_index=5,
            data=intent.data,
        )

    def test_dispatch_timestamp(self) -> None:
        writer = MagicMock()
        ts_val = np.datetime64("2025-06-15T12:00:00", "s")
        intent = WriteIntent(
            group="g",
            array="timestamp",
            ts_index=3,
            data=None,
            kind="timestamp",
            timestamp_val=ts_val,
        )
        IndexedRegionStrategy._dispatch_intent(writer, intent)
        writer.write_timestamp.assert_called_once_with(
            group="g",
            ts_index=3,
            timestamp_val=ts_val,
        )

    def test_write_groups_returns_coverage_and_duration(self, tmp_path: Path) -> None:
        store_path = str(tmp_path / "test.zarr")
        zarr.open_group(store=store_path, mode="w", zarr_format=3)

        schema = [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(1, 10),
                        dtype=np.float32,
                        chunks=(1, 10),
                        fill_value=0.0,
                    ),
                ],
            )
        ]

        strategy = IndexedRegionStrategy(
            store_uri=store_path,
            schema=schema,
            coord_names_by_group={"data": frozenset()},
        )

        ts_val = np.datetime64("2025-01-01", "s")
        intents = [
            WriteIntent(
                group="data",
                array="timestamp",
                ts_index=0,
                data=None,
                kind="timestamp",
                timestamp_val=ts_val,
            ),
        ]

        result = strategy.write_groups(
            group_to_intents={"data": intents},
            schema=schema,
        )

        assert "coverage" in result
        assert "duration_s" in result
        assert isinstance(result["duration_s"], float)
        assert result["duration_s"] >= 0
        coverage = result["coverage"][0]
        assert coverage["group"] == "data"
        assert coverage["arrays"] == ["timestamp"]
        assert coverage["time_index_ranges"] == [[0, 0]]
        assert coverage["aligned"] is True
        assert coverage["state_array"] == "data/firecube_timestamp_state"
        assert coverage["state_deleted_value"] == 2
        assert str(coverage["time_min"]) == "2025-01-01T00:00:00Z"
        assert str(coverage["time_max"]) == "2025-01-01T00:00:00Z"
        assert coverage["time_dim_name"] == "timestamp"


class TestApiReExports:
    def test_direct_zarr_ingestor_importable(self) -> None:
        from firecube.ingestor.api import DirectZarrIngestor as D

        assert D is DirectZarrIngestor

    def test_indexed_region_strategy_importable(self) -> None:
        from firecube.ingestor.api import IndexedRegionStrategy as I

        assert I is IndexedRegionStrategy

    def test_write_intent_importable(self) -> None:
        from firecube.ingestor.api import WriteIntent as W

        assert W is WriteIntent

    def test_zarr_specs_importable(self) -> None:
        from firecube.ingestor.api import ZarrArraySpec as PublicArraySpec
        from firecube.ingestor.api import ZarrGroupSpec as PublicGroupSpec

        assert PublicArraySpec is ZarrArraySpec
        assert PublicGroupSpec is ZarrGroupSpec
