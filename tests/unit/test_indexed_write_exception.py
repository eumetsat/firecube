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

"""Contract tests for ``IndexedWriteCompilationError`` and direct-Zarr fallback paths."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from firecube.core.errors import IndexedWriteCompilationError
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor
from tests.helpers.rows import table_row


def test_indexed_write_compilation_error_contract(subtests: Any, tmp_path: Path) -> None:
    with table_row(subtests, "compilation_error_fields", tmp_path):
        coordinate = datetime(2026, 1, 1, tzinfo=UTC)
        err = IndexedWriteCompilationError(coordinate, "bad shape", "IndexedWrite(...)")

        assert err.coordinate is coordinate
        assert err.reason == "bad shape"
        assert err.iw_repr == "IndexedWrite(...)"
        assert isinstance(err, ValueError)

    with table_row(subtests, "compilation_error_message_shape", tmp_path):
        coordinate = datetime(2026, 1, 1, tzinfo=UTC)
        err = IndexedWriteCompilationError(coordinate, "bad shape", "IndexedWrite(...)")

        message = str(err)
        assert "IndexedWrite compilation failed" in message
        assert "bad shape" in message
        assert repr(coordinate) in message

    with table_row(subtests, "compilation_error_is_value_error", tmp_path):
        assert issubclass(IndexedWriteCompilationError, ValueError)

    with table_row(subtests, "exported_from_sdk", tmp_path):
        from firecube.sdk import IndexedWriteCompilationError as SdkError

        assert SdkError is IndexedWriteCompilationError

    with table_row(subtests, "abstract_error_fires_when_neither_hook_overridden", tmp_path):
        ingestor = _MinimalDirectZarrIngestor()

        with pytest.raises(NotImplementedError) as excinfo:
            ingestor.build_write_intents(cast(Any, []), cast(Any, None))

        message = str(excinfo.value)
        assert "build_write_intents" in message
        assert "WriteIntent and IndexedWrite" in message


class _MinimalDirectZarrIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "test-indexed-write-exception"

    def zarr_schema(self, ctx):
        _ = ctx
        return []
