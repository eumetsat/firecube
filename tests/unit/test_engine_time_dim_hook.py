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

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest


@pytest.mark.unit
def test_zarr_pre_batch_hook_seeds_declared_time_coordinate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from firecube.ingestor.runtime import engine
    from firecube.ingestor.runtime.zarr import batch_runner

    seed = MagicMock()
    monkeypatch.setattr(batch_runner, "seed_staged_metadata_pre_batch", seed)
    host = SimpleNamespace(
        _log=MagicMock(),
        _resolve_time_dim_name=MagicMock(return_value="time"),
    )
    ctx = SimpleNamespace(output_format="zarr")

    hook = engine._zarr_pre_batch_hook(cast(Any, host), cast(Any, ctx))
    assert hook is not None

    hook()

    seed.assert_called_once_with(
        host=host,
        ctx=ctx,
        logger=host._log,
        coordinate_arrays=["time"],
    )


@pytest.mark.unit
def test_zarr_pre_batch_hook_skips_non_zarr_outputs() -> None:
    from firecube.ingestor.runtime import engine

    host = SimpleNamespace(
        _log=MagicMock(),
        _resolve_time_dim_name=MagicMock(return_value="time"),
    )
    ctx = SimpleNamespace(output_format="parquet")

    assert engine._zarr_pre_batch_hook(cast(Any, host), cast(Any, ctx)) is None
    host._resolve_time_dim_name.assert_not_called()
