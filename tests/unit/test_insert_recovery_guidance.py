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

"""Insertion errors must not recommend retrying an unsupported write."""

from pathlib import Path

from firecube.ingestor.api import PipelineBatch, PipelineResult
from firecube.ingestor.errors import InsertRefusedError
from firecube.ingestor.runtime.engine import _failed_batches_message


def test_insert_refusal_explains_rebuild():
    error = InsertRefusedError(refused_timestamps=["2024-01-02"], reason="insert")
    assert "new target" in str(error)
    assert "cannot insert" in str(error)
    assert "Use force_reingest=True" not in str(error)


def test_terminal_message_does_not_promise_insertion_or_double_punctuation():
    error = InsertRefusedError(refused_timestamps=["2024-01-02"], reason="insert")
    result = PipelineResult(
        batch=PipelineBatch(batch_id="b", data_path=Path(".")), success=False, error=str(error)
    )
    message = _failed_batches_message(run_id="r", failed=[result], not_attempted=1)
    assert ".." not in message
    assert "Neither option can insert" in message
