"""Reproduce shard-sharing data-loss risk in slot alignment.

Zarr v3 sharding makes the shard the parallel write-safety unit. Inner chunks
inside one shard are written through read-modify-write of the shard object, so
two workers must not own different time slots within the same shard.
"""

from __future__ import annotations

import numpy as np
import pytest

from firecube.cli._slot_planning import _resolve_per_group_slot_sizes
from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.types.planned_range import validate_chunk_alignment

pytestmark = pytest.mark.slow


def test_slot_range_alignment_uses_shard_size_when_array_is_sharded() -> None:
    """A worker range smaller than a shard must be rejected."""
    with pytest.raises(ConfigurationError, match=r"\(4, 100\)"):
        validate_chunk_alignment(
            0,
            1,
            {"data": [(1, 100)]},
            shards_per_group={"data": [(4, 100)]},
        )


def test_slot_planning_uses_shard_size_for_time_indexed_sharded_arrays() -> None:
    """Automatic and explicit slot sizes use shard size, not inner chunk size."""
    schema = [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="temperature",
                    shape=(8, 100),
                    chunks=(1, 100),
                    shards=(4, 100),
                    dtype=np.float32,
                ),
                ZarrArraySpec(
                    name="latitude",
                    shape=(100,),
                    chunks=(100,),
                    shards=(100,),
                    dtype=np.float32,
                    time_indexed=False,
                ),
            ],
        )
    ]

    assert _resolve_per_group_slot_sizes(schema, explicit=None) == {"data": 4}
    with pytest.raises(ConfigurationError):
        validate_chunk_alignment(
            0,
            1,
            {"data": [(1, 100)]},
            shards_per_group={"data": [(4, 100)]},
        )
