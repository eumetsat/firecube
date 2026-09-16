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

"""Unit tests for zarr_sharding + zarr_shard_shape config-time validation."""

import pytest

from firecube.core.errors import ConfigurationError
from firecube.ingestor.templates.config import ZarrTemplateConfig


def test_sharding_true_with_chunk_no_shard_raises() -> None:
    """zarr_sharding=True + zarr_chunk_shape set + no zarr_shard_shape → ConfigurationError."""
    with pytest.raises(ConfigurationError, match="zarr_shard_shape"):
        ZarrTemplateConfig(
            zarr_sharding=True,
            zarr_chunk_shape={"time": 5},
            zarr_shard_shape=None,
        )


def test_sharding_true_with_shard_and_chunk_passes() -> None:
    """zarr_sharding=True + valid zarr_chunk_shape + valid zarr_shard_shape → no exception."""
    cfg = ZarrTemplateConfig(
        zarr_sharding=True,
        zarr_chunk_shape={"time": 5, "lat": 10, "lon": 10},
        zarr_shard_shape={"time": 10, "lat": 100, "lon": 100},
    )
    assert cfg.zarr_sharding is True
    assert cfg.zarr_shard_shape == {"time": 10, "lat": 100, "lon": 100}


def test_shard_not_multiple_of_chunk_raises() -> None:
    """zarr_shard_shape not a multiple of zarr_chunk_shape per dimension → ConfigurationError."""
    with pytest.raises(ConfigurationError, match="multiple of zarr_chunk_shape"):
        ZarrTemplateConfig(
            zarr_sharding=True,
            zarr_chunk_shape={"time": 5},
            zarr_shard_shape={"time": 7},
        )


def test_sharding_false_no_validation() -> None:
    """zarr_sharding=False → no sharding validation even if zarr_chunk_shape is set."""
    cfg = ZarrTemplateConfig(
        zarr_sharding=False,
        zarr_chunk_shape={"time": 5},
        zarr_shard_shape=None,
    )
    assert cfg.zarr_sharding is False
