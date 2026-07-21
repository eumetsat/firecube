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


def _make_model() -> SlotIndexModel:
    return SlotIndexModel(
        name="test_v1",
        epoch="2026-01-01T00:00:00Z",
        groups={"g": SlotAxis(cadence_s=1, mode="exact")},
    )


def test_parallel_without_override_fails() -> None:
    with pytest.raises(TypeError, match="slot_index_model"):

        class MissingSlotModel(DirectZarrIngestor):
            PRODUCT_NAME = "missing_slot_model"
            name = "missing_slot_model"
            SUPPORTS_SLOT_RANGE_PARALLELISM = True

            def timestamp_to_ts_index(self, group, timestamp_val):
                return 0

            def global_expected_time_count(self, ctx):
                return {"g": 1}

            def zarr_schema(self, ctx):
                return []

            def build_write_intents(self, batch, ctx):
                return []

            def ingest(self, *args, **kwargs) -> IngestResult:
                return None  # type: ignore[return-value]


def test_non_parallel_without_override_ok() -> None:
    class PlainNoSlotModel(DirectZarrIngestor):
        PRODUCT_NAME = "plain_no_slot_model"
        name = "plain_no_slot_model"

        def zarr_schema(self, ctx):
            return []

        def build_write_intents(self, batch, ctx):
            return []

        def ingest(self, *args, **kwargs) -> IngestResult:
            return None  # type: ignore[return-value]

    assert PlainNoSlotModel.__name__ == "PlainNoSlotModel"


def test_parallel_with_override_ok() -> None:
    class CapableWithSlotModel(DirectZarrIngestor):
        PRODUCT_NAME = "capable_with_slot_model"
        name = "capable_with_slot_model"
        SUPPORTS_SLOT_RANGE_PARALLELISM = True

        def timestamp_to_ts_index(self, group, timestamp_val):
            return 0

        def global_expected_time_count(self, ctx):
            return {"g": 1}

        def slot_index_model(self, ctx):
            return _make_model()

        def zarr_schema(self, ctx):
            return []

        def build_write_intents(self, batch, ctx):
            return []

        def ingest(self, *args, **kwargs) -> IngestResult:
            return None  # type: ignore[return-value]

    assert CapableWithSlotModel.__name__ == "CapableWithSlotModel"
    assert CapableWithSlotModel.slot_index_model is not DirectZarrIngestor.slot_index_model


def test_default_raises_not_implemented() -> None:
    class PlainForDefaultCheck(DirectZarrIngestor):
        PRODUCT_NAME = "plain_for_default_check"
        name = "plain_for_default_check"

        def zarr_schema(self, ctx):
            return []

        def build_write_intents(self, batch, ctx):
            return []

        def ingest(self, *args, **kwargs) -> IngestResult:
            return None  # type: ignore[return-value]

    instance = PlainForDefaultCheck()
    with pytest.raises(NotImplementedError, match="slot_index_model"):
        instance.slot_index_model(ctx=None)  # type: ignore[arg-type]
