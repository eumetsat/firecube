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
from typing import Any, cast

import pytest

from firecube.core.errors import IndexedWriteCompilationError
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor


def test_compilation_error_fields() -> None:
    coordinate = datetime(2026, 1, 1, tzinfo=UTC)
    err = IndexedWriteCompilationError(coordinate, "bad shape", "IndexedWrite(...)")

    assert err.coordinate is coordinate
    assert err.reason == "bad shape"
    assert err.iw_repr == "IndexedWrite(...)"
    assert isinstance(err, ValueError)


def test_compilation_error_message_shape() -> None:
    coordinate = datetime(2026, 1, 1, tzinfo=UTC)
    err = IndexedWriteCompilationError(coordinate, "bad shape", "IndexedWrite(...)")

    message = str(err)
    assert "IndexedWrite compilation failed" in message
    assert "bad shape" in message
    assert repr(coordinate) in message


def test_compilation_error_is_value_error() -> None:
    assert issubclass(IndexedWriteCompilationError, ValueError)


def test_reexport_from_core_api() -> None:
    from firecube.core.api import IndexedWriteCompilationError as CoreIndexedWriteCompilationError

    assert CoreIndexedWriteCompilationError is IndexedWriteCompilationError


def test_reexport_from_ingestor_api() -> None:
    from firecube.ingestor.api import (
        IndexedWriteCompilationError as IngestorIndexedWriteCompilationError,
    )

    assert IngestorIndexedWriteCompilationError is IndexedWriteCompilationError


class _MinimalDirectZarrIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "test-indexed-write-exception"

    def zarr_schema(self, ctx):
        _ = ctx
        return []


def test_abstract_error_fires_when_neither_hook_overridden() -> None:
    ingestor = _MinimalDirectZarrIngestor()

    with pytest.raises(NotImplementedError) as excinfo:
        ingestor.build_write_intents(cast(Any, []), cast(Any, None))

    message = str(excinfo.value)
    assert "build_indexed_write" in message
    assert "build_write_intents" in message
