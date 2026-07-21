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

from unittest.mock import MagicMock, patch

import pytest

from firecube.core.config import StorageConfig
from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy


@pytest.mark.unit
def test_append_strategy_passes_constructed_session_when_self_session_is_none(tmp_path):
    """T5.4 (Finding 3): constructed fallback session must reach append_time_groups."""
    mock_session = MagicMock(name="fallback_session")
    mock_zarr_store = MagicMock(name="fallback_zarr_store")
    mock_zarr_store.store = object()
    mock_session.zarr.create_store.return_value = mock_zarr_store

    strategy = AppendStrategy(
        store=object(),
        store_uri=str(tmp_path / "output.zarr"),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
    )

    with (
        patch(
            "firecube.ingestor.runtime.zarr.strategies.append._session_for_store",
            return_value=mock_session,
        ) as mock_session_for_store,
        patch(
            "firecube.ingestor.runtime.zarr.append.append_time_groups", autospec=True
        ) as mock_append,
    ):
        mock_append.return_value = {"chunks_written": 0}
        strategy.write_groups(
            group_to_timestamps={"G": []},
            dataset_for_batch=lambda g, ts: None,
            batch_size=1,
        )

    mock_session_for_store.assert_called_once()
    assert mock_append.called
    assert mock_append.call_args.kwargs["session"] is mock_session
