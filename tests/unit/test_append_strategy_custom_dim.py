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

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest

from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy


@pytest.mark.unit
def test_append_strategy_uses_constructor_append_dim():
    """AppendStrategy passes the constructor-supplied append_dim to append_time_groups.

    Confirms the host-free architecture: the strategy never resolves time_dim_name
    from a host object — the caller (typically GenericZarrIngestor) is responsible
    for resolving and passing the value at construction time.
    """
    zarr_handle = cast(Any, SimpleNamespace(store=object()))
    strategy = AppendStrategy(store=object(), store_handle=zarr_handle, append_dim="time")

    with patch(
        "firecube.ingestor.runtime.zarr.append.append_time_groups", autospec=True
    ) as mock_append:
        mock_append.return_value = {"chunks_written": 0}
        strategy.write_groups(
            group_to_timestamps={"G": []},
            dataset_for_batch=lambda g, ts: None,
            batch_size=1,
        )

    assert mock_append.call_args.kwargs["append_dim"] == "time"


@pytest.mark.unit
def test_append_strategy_back_compat_default_timestamp():
    """When append_dim is not supplied, default 'timestamp' is used (back-compat)."""
    zarr_handle = cast(Any, SimpleNamespace(store=object()))
    strategy = AppendStrategy(store=object(), store_handle=zarr_handle)

    with patch(
        "firecube.ingestor.runtime.zarr.append.append_time_groups", autospec=True
    ) as mock_append:
        mock_append.return_value = {"chunks_written": 0}
        strategy.write_groups(
            group_to_timestamps={"G": []},
            dataset_for_batch=lambda g, ts: None,
            batch_size=1,
        )

    assert mock_append.call_args.kwargs["append_dim"] == "timestamp"
