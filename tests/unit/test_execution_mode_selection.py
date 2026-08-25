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

"""Execution-mode selection is decided by ``pipeline_workers`` alone."""

from __future__ import annotations

import pytest

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.runtime.configure import ExecutionMode, determine_execution_mode

pytestmark = pytest.mark.unit


def test_default_config_runs_sequentially() -> None:
    assert determine_execution_mode(EngineConfig()) is ExecutionMode.SEQUENTIAL


def test_one_worker_runs_sequentially() -> None:
    config = EngineConfig(pipeline_workers=1)
    assert determine_execution_mode(config) is ExecutionMode.SEQUENTIAL


def test_two_or_more_workers_select_pipeline() -> None:
    assert determine_execution_mode(EngineConfig(pipeline_workers=2)) is ExecutionMode.PIPELINE
    assert determine_execution_mode(EngineConfig(pipeline_workers=8)) is ExecutionMode.PIPELINE


@pytest.mark.parametrize("workers", [0, -1])
def test_invalid_pipeline_workers_rejected_at_construction(workers: int) -> None:
    with pytest.raises(ValueError, match="pipeline_workers must be >= 1"):
        EngineConfig(pipeline_workers=workers)


def test_invalid_pipeline_batch_size_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="pipeline_batch_size must be >= 1"):
        EngineConfig(pipeline_batch_size=0)


def test_invalid_extract_workers_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="extract_workers must be >= 1"):
        EngineConfig(extract_workers=0)


def test_extract_workers_does_not_affect_execution_mode() -> None:
    config = EngineConfig(extract_workers=8)
    assert determine_execution_mode(config) is ExecutionMode.SEQUENTIAL
