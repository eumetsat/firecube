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

"""Regression tests for DirectZarrIngestor time dimension wiring."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock

import numpy as np
import pytest
import zarr

from firecube.ingestor.api import DirectZarrIngestor, WriteIntent, ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import _setup_global_zarr_schema
from firecube.ingestor.types.context import PipelineBatch


class _TimeDirectIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "td_direct"
    time_dim_name: ClassVar[str] = "time"

    def ingest(self, ctx):  # pragma: no cover - abstract hook not used here
        raise NotImplementedError

    def zarr_schema(self, ctx):
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="time",
                        shape=(0,),
                        dtype="datetime64[s]",
                        chunks=(1,),
                    ),
                    ZarrArraySpec(
                        name="x",
                        shape=(0,),
                        dtype=np.float32,
                        chunks=(1,),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch, ctx):
        return [
            WriteIntent(
                group="data",
                array="timestamp",
                ts_index=0,
                data=None,
                kind="timestamp",
                timestamp_val=np.datetime64("2025-01-01T00:00:00", "s"),
            )
        ]

    def _aggregate_metrics(self, ctx, state):
        return {}


class _RecordingIndexedRegionStrategy:
    instances: ClassVar[list[_RecordingIndexedRegionStrategy]] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.__class__.instances.append(self)

    def write_groups(self, **kwargs):
        self.write_groups_kwargs = kwargs
        return {"coverage": ["data"]}


def _plugin_ctx() -> Any:
    return SimpleNamespace(run_id="run-001", storage=None, option=MagicMock(return_value=None))


@pytest.mark.unit
def test_directzarr_resolves_declared_time_dim_name() -> None:
    ingestor = _TimeDirectIngestor()

    assert ingestor.time_dim_name == "time"
    assert ingestor._resolve_time_dim_name() == "time"


@pytest.mark.unit
def test_process_batch_builds_region_strategy_with_declared_time_dim(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import firecube.ingestor.api as ingestor_api

    _RecordingIndexedRegionStrategy.instances.clear()
    monkeypatch.setattr(ingestor_api, "IndexedRegionStrategy", _RecordingIndexedRegionStrategy)

    ingestor = _TimeDirectIngestor()
    ingestor.engine_config = EngineConfig(write_mode="direct")
    cast(Any, ingestor)._chunk_manager = SimpleNamespace(
        storage_config=None,
        acquire_claim=MagicMock(
            return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock())
        ),
    )
    monkeypatch.setattr(
        ingestor,
        "resolve_output_uri",
        MagicMock(return_value=str(tmp_path / "out.zarr")),
    )

    result = ingestor._process_batch(
        PipelineBatch(batch_id="batch-1", data_path=tmp_path, items=["source.nc"]),
        _plugin_ctx(),
    )

    assert result.success is True
    assert _RecordingIndexedRegionStrategy.instances[0].kwargs["time_coord_name"] == "time"
    assert result.metrics["coverage"] == ["data"]


@pytest.mark.unit
def test_parallel_startup_schema_setup_uses_declared_time_dim(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import firecube.ingestor.api as ingestor_api
    from firecube.ingestor.templates import direct_zarr

    setup_global_schema = MagicMock()
    _RecordingIndexedRegionStrategy.instances.clear()
    monkeypatch.setattr(ingestor_api, "IndexedRegionStrategy", _RecordingIndexedRegionStrategy)
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", setup_global_schema)

    ingestor = _TimeDirectIngestor()
    ingestor.engine_config = EngineConfig(write_mode="direct")
    parallel_state = SimpleNamespace(
        global_expected={"data": 1},
        schema_verified={},
    )
    cast(Any, ingestor)._parallel_execution_state = parallel_state
    cast(Any, ingestor)._chunk_manager = SimpleNamespace(
        storage_config=None,
        record_schema_verification=MagicMock(),
    )
    monkeypatch.setattr(
        ingestor,
        "resolve_output_uri",
        MagicMock(return_value=str(tmp_path / "out.zarr")),
    )

    ingestor._verify_schema_at_pod_startup(_plugin_ctx())

    strategy = _RecordingIndexedRegionStrategy.instances[0]
    assert strategy.kwargs["time_coord_name"] == "time"
    setup_global_schema.assert_called_once()
    assert setup_global_schema.call_args.kwargs["time_coord_name"] == "time"
    assert parallel_state.schema_verified == {"data": True}


@pytest.mark.unit
def test_global_schema_setup_infers_dimension_names_for_declared_time_dim(
    tmp_path: Path,
) -> None:
    store_uri = str(tmp_path / "parallel.zarr")
    schema = [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="time",
                    shape=(0,),
                    dtype="datetime64[s]",
                    chunks=(1,),
                )
            ],
        )
    ]
    strategy = IndexedRegionStrategy(
        store_uri=store_uri,
        schema=schema,
        time_coord_name="time",
    )

    _setup_global_zarr_schema(
        strategy=strategy,
        schema=schema,
        global_expected={"data": 1},
        product="product.zarr",
        run_id="run-001",
        chunk_manager=None,
        time_coord_name="time",
    )

    arr = cast(Any, zarr.open_group(store=store_uri, mode="r")["data/time"])
    assert arr.metadata.dimension_names == ("time",)
