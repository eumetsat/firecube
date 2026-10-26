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

"""Several sources per run: ``ctx.sources``, ``ctx.source`` and the item type plugins get."""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest

from firecube.core.api import AUTO, IndexSpec, IrregularTimeAxis, ItemInfo, SourceItem
from firecube.ingestor.api import (
    ConfigurationError,
    DirectZarrIngestor,
    IngestContext,
    PluginContext,
    RuntimeIngestContext,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.runtime.configure import configure_ingestor
from firecube.ingestor.types.context import OutputPaths, PipelineResult, StorageContext
from tests.helpers.fake_stac import FakeStacApi, install_fake_stac, stac_item
from tests.helpers.storage import make_test_session

pytestmark = pytest.mark.unit

API = "https://stac.test/api"
STAC_SOURCE = "stac://stac.test/api/collections/coll"


def _plugin_ctx(ingest_ctx: IngestContext) -> PluginContext:
    runtime = RuntimeIngestContext.from_ingest_context(
        ingest_ctx, run_id="run-1", temp_root=None, materializer=None
    )
    return PluginContext(runtime)


def test_sources_hold_every_input_in_order():
    ctx = _plugin_ctx(IngestContext(sources=("/data/a", "s3://bucket/b", STAC_SOURCE)))

    assert ctx.sources == ("/data/a", "s3://bucket/b", STAC_SOURCE)


def test_source_with_two_inputs_raises_configuration_error():
    ctx = _plugin_ctx(IngestContext(sources=("/data/a", "/data/b")))

    with pytest.raises(ConfigurationError, match="2 sources") as refused:
        _ = ctx.source

    assert "sources" in str(refused.value)


def test_source_returns_the_single_entry_or_empty():
    assert _plugin_ctx(IngestContext(sources=("/data/a",))).source == "/data/a"
    assert _plugin_ctx(IngestContext(sources=())).source == ""


def test_a_lone_source_argument_is_mirrored_into_sources():
    ctx = _plugin_ctx(IngestContext(source="/data/a"))

    assert ctx.sources == ("/data/a",)
    assert ctx.source == "/data/a"


# -- what filter_item and inspect_item receive ------------------------------------------


class _RecordingIngestor(DirectZarrIngestor):
    """AUTO irregular axis over the default discovery; records the item types it gets."""

    PRODUCT_NAME: ClassVar[str] = "source_items_recording"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.seen: dict[str, list[type]] = {"filter_item": [], "inspect_item": []}
        self._next = 0

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="source_items_recording_v1",
            groups={"data": IrregularTimeAxis(coordinate="timestamp", values=AUTO)},
        )

    def filter_item(self, item: Any, ctx: PluginContext) -> bool:
        self.seen["filter_item"].append(type(item))
        return True

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        self.seen["inspect_item"].append(type(item))
        self._next += 1
        return ItemInfo(
            coordinate=np.datetime64("2025-06-01T00:00:00") + np.timedelta64(self._next, "s")
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(2,),
                        dtype="datetime64[ns]",
                        fill_value=np.datetime64("NaT", "ns"),
                        time_indexed=True,
                        dimension_names=("timestamp",),
                    )
                ],
            )
        ]

    def build_write_intents(self, batch, ctx):
        return []


class _SourceItemsIngestor(_RecordingIngestor):
    PRODUCT_NAME: ClassVar[str] = "source_items_recording_typed"
    SOURCE_ITEMS: ClassVar[bool] = True


def _bind(ingestor: _RecordingIngestor, tmp_path: Path, sources: tuple[str, ...]) -> None:
    session = make_test_session(tmp_path, product="out.zarr")
    ctx = configure_ingestor(
        ingestor,
        target=session.product.product_uri.to_str(),
        options={},
        run_id="run-1",
        sources=sources,
        storage=StorageContext(output=session),
    )
    ingestor.bind_index_at_startup(ctx)


@pytest.fixture
def two_sources(monkeypatch, tmp_path: Path) -> tuple[str, ...]:
    local = tmp_path / "raw"
    local.mkdir()
    (local / "granule-1.nc").write_bytes(b"")
    api = FakeStacApi(
        API,
        [
            stac_item(
                "item-1",
                "coll",
                {"data": ("https://data.test/granule-2.nc", ["data"])},
                base=API,
            )
        ],
    )
    install_fake_stac(monkeypatch, api)
    return (str(local), STAC_SOURCE)


def test_plugin_without_source_items_receives_href_strings(tmp_path, two_sources):
    ingestor = _RecordingIngestor()

    _bind(ingestor, tmp_path, two_sources)

    assert ingestor.seen == {"filter_item": [str, str], "inspect_item": [str, str]}


def test_plugin_with_source_items_receives_source_items(tmp_path, two_sources):
    ingestor = _SourceItemsIngestor()

    _bind(ingestor, tmp_path, two_sources)

    assert ingestor.seen == {
        "filter_item": [SourceItem, SourceItem],
        "inspect_item": [SourceItem, SourceItem],
    }


class _RunIngestor(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "sources_run"

    def _process_batch(self, batch, ctx):
        return PipelineResult(
            batch=batch, outputs=OutputPaths(primary=str(ctx.target or "")), success=True
        )

    def _aggregate_metrics(self, ctx, state):
        return {}


def test_a_run_over_two_sources_batches_both(tmp_path, two_sources):
    ingestor = _RunIngestor(name="sources_run")  # pyright: ignore[reportAbstractUsage]
    session = make_test_session(tmp_path, product="out.zarr")
    batched: list[Any] = []
    original = ingestor.filter_item

    def record(item: Any, ctx: PluginContext) -> bool:
        batched.append(item)
        return original(item, ctx)

    ingestor.filter_item = record  # type: ignore[method-assign]

    ingestor.run(
        IngestContext(
            sources=two_sources,
            target=session.product.product_uri.to_str(),
            output_format="zarr",
            storage=StorageContext(output=session),
        )
    )

    assert batched == [
        str(Path(two_sources[0]) / "granule-1.nc"),
        "https://data.test/granule-2.nc",
    ]
