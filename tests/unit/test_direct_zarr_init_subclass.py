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

import pytest

from firecube.core.api import SlotAxis, SlotIndexModel
from firecube.ingestor.api import DirectZarrIngestor, IngestResult


def _slot_model() -> SlotIndexModel:
    return SlotIndexModel(
        name="init_subclass_test_v1",
        epoch="2026-01-01T00:00:00Z",
        groups={"g": SlotAxis(cadence_s=1, mode="exact")},
    )


class _CapableDirectZarr(DirectZarrIngestor):
    PRODUCT_NAME = "capable_direct"
    name = "capable_direct"
    SUPPORTS_SLOT_RANGE_PARALLELISM = True

    def timestamp_to_ts_index(self, group, timestamp_val):
        return 0

    def global_expected_time_count(self, ctx):
        return {"g": 1}

    def slot_index_model(self, ctx):
        return _slot_model()

    def zarr_schema(self, ctx):
        return []

    def build_write_intents(self, batch, ctx):
        return []

    def ingest(self, *args, **kwargs) -> IngestResult:
        return None  # type: ignore[return-value]


def test_supports_false_default_no_validation() -> None:
    class PlainDirectZarr(DirectZarrIngestor):
        PRODUCT_NAME = "plain_direct"
        name = "plain_direct"

        def zarr_schema(self, ctx):
            return []

        def build_write_intents(self, batch, ctx):
            return []

        def ingest(self, *args, **kwargs) -> IngestResult:
            return None  # type: ignore[return-value]

    assert PlainDirectZarr.__name__ == "PlainDirectZarr"


def test_supports_true_with_both_overrides_passes() -> None:
    class CapableDirectZarr(DirectZarrIngestor):
        PRODUCT_NAME = "capable_ok"
        name = "capable_ok"
        SUPPORTS_SLOT_RANGE_PARALLELISM = True

        def timestamp_to_ts_index(self, group, timestamp_val):
            return 1

        def global_expected_time_count(self, ctx):
            return {"g": 2}

        def slot_index_model(self, ctx):
            return _slot_model()

        def zarr_schema(self, ctx):
            return []

        def build_write_intents(self, batch, ctx):
            return []

        def ingest(self, *args, **kwargs) -> IngestResult:
            return None  # type: ignore[return-value]

    assert CapableDirectZarr.__name__ == "CapableDirectZarr"


def test_supports_true_missing_timestamp_to_ts_index_fails() -> None:
    with pytest.raises(TypeError, match="timestamp_to_ts_index"):

        class BrokenTimestampDirectZarr(DirectZarrIngestor):
            PRODUCT_NAME = "broken_timestamp"
            name = "broken_timestamp"
            SUPPORTS_SLOT_RANGE_PARALLELISM = True

            def global_expected_time_count(self, ctx):
                return {"g": 1}

            def zarr_schema(self, ctx):
                return []

            def build_write_intents(self, batch, ctx):
                return []

            def ingest(self, *args, **kwargs) -> IngestResult:
                return None  # type: ignore[return-value]


def test_supports_true_missing_global_expected_time_count_fails() -> None:
    with pytest.raises(TypeError, match="global_expected_time_count"):

        class BrokenGlobalCountDirectZarr(DirectZarrIngestor):
            PRODUCT_NAME = "broken_global_count"
            name = "broken_global_count"
            SUPPORTS_SLOT_RANGE_PARALLELISM = True

            def timestamp_to_ts_index(self, group, timestamp_val):
                return 1

            def zarr_schema(self, ctx):
                return []

            def build_write_intents(self, batch, ctx):
                return []

            def ingest(self, *args, **kwargs) -> IngestResult:
                return None  # type: ignore[return-value]


def test_filter_items_not_required_when_supports_true() -> None:
    class CapableWithoutFilter(DirectZarrIngestor):
        PRODUCT_NAME = "capable_no_filter"
        name = "capable_no_filter"
        SUPPORTS_SLOT_RANGE_PARALLELISM = True

        def timestamp_to_ts_index(self, group, timestamp_val):
            return 2

        def global_expected_time_count(self, ctx):
            return {"g": 3}

        def slot_index_model(self, ctx):
            return _slot_model()

        def zarr_schema(self, ctx):
            return []

        def build_write_intents(self, batch, ctx):
            return []

        def ingest(self, *args, **kwargs) -> IngestResult:
            return None  # type: ignore[return-value]

    assert CapableWithoutFilter.__name__ == "CapableWithoutFilter"


def test_subclass_of_subclass_inherits_correctly() -> None:
    class SubCapable(_CapableDirectZarr):
        PRODUCT_NAME = "sub_capable"
        name = "sub_capable"

    assert SubCapable().timestamp_to_ts_index("g", 0) == 0
