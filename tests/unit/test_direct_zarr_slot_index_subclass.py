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

from firecube.ingestor.api import DirectZarrIngestor, IngestResult


def test_index_spec_without_inspect_item_override_allowed_at_class_definition() -> None:
    class MissingInspectItem(DirectZarrIngestor):
        PRODUCT_NAME = "missing_inspect_item"
        name = "missing_inspect_item"

        def index_spec(self, ctx):
            return None

        def zarr_schema(self, ctx):
            return []

        def build_write_intents(self, batch, ctx):
            return []

        def ingest(self, *args, **kwargs) -> IngestResult:
            return None  # type: ignore[return-value]

    assert MissingInspectItem.__name__ == "MissingInspectItem"


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
    class CapableWithInspectItem(DirectZarrIngestor):
        PRODUCT_NAME = "capable_with_inspect_item"
        name = "capable_with_inspect_item"

        def index_spec(self, ctx):
            return None

        def inspect_item(self, item, ctx):
            return None

        def zarr_schema(self, ctx):
            return []

        def build_write_intents(self, batch, ctx):
            return []

        def ingest(self, *args, **kwargs) -> IngestResult:
            return None  # type: ignore[return-value]

    assert CapableWithInspectItem.__name__ == "CapableWithInspectItem"
    assert CapableWithInspectItem.inspect_item is not DirectZarrIngestor.inspect_item


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
    with pytest.raises(NotImplementedError, match="inspect_item"):
        instance.inspect_item(item=None, ctx=None)  # type: ignore[arg-type]
