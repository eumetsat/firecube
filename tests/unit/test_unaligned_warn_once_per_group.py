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

import logging
from unittest.mock import patch

import pytest

from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.ingestor.runtime.zarr.alignment import AlignmentMonitor
from firecube.ingestor.runtime.zarr.append_services import AppendWriteExecutor

pytestmark = pytest.mark.unit


def _executor(logger: logging.Logger | None = None) -> AppendWriteExecutor:
    handle = ZarrStoreHandle(store="dummy", storage_options=None, target_uri="dummy")
    return AppendWriteExecutor(
        zarr_store=handle,
        chunk_shape={"timestamp": 10},
        shard_shape=None,
        sharding=False,
        compression=False,
        append_dim="timestamp",
        logger=logger or logging.getLogger("test"),
        alignment=AlignmentMonitor(),
    )


def test_alignment_warn_separate_groups_each_warn_once() -> None:
    logger = logging.getLogger("test.multi_group")
    writer = _executor(logger)

    with patch.object(logger, "warning") as mock_warn:
        writer.check_alignment(start_i=0, count=15, chunk_len=10, group="grp1")
        writer.check_alignment(start_i=15, count=15, chunk_len=10, group="grp1")
        writer.check_alignment(start_i=0, count=15, chunk_len=10, group="grp2")
        writer.check_alignment(start_i=15, count=15, chunk_len=10, group="grp2")

    assert mock_warn.call_count == 2
