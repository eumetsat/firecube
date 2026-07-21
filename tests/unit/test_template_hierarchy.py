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

from abc import abstractmethod
from pathlib import Path
from typing import Any, cast

import pytest

from firecube.ingestor.api import (
    BaseIngestor,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
    discover_ingestors,
)
from tests.helpers.storage import make_test_context

_FIXTURE_PLUGINS = (
    "cli_test_plugin",
    "direct_zarr_capable_test_plugin",
    "direct_zarr_non_capable_test_plugin",
    "multi_group_capable_test_plugin",
    "cf_time_dim",
)


@pytest.mark.unit
@pytest.mark.parametrize("plugin_name", _FIXTURE_PLUGINS)
def test_fixture_plugins_discover_instantiate_and_run_batch_lifecycle(
    plugin_name: str,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    item = source / "item.dat"
    item.write_text("payload")

    discovered = discover_ingestors()
    assert plugin_name in discovered

    ingestor_cls = cast(type[BaseIngestor], discovered[plugin_name])
    ingestor = ingestor_cls(name=plugin_name)
    assert ingestor.PRODUCT_NAME
    assert ingestor.time_dim_name

    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        make_test_context(
            tmp_path,
            source=str(source),
            product=f"{plugin_name}.zarr",
            options={"batch_size": 1, "write_mode": "direct"},
        ),
        run_id=f"{plugin_name}-run",
        temp_root=tmp_path,
        materializer=lambda source_item: Path(source_item),
    )
    ctx = PluginContext(runtime_ctx)
    batch = PipelineBatch(batch_id="batch-1", data_path=source, items=[item], metadata={})

    ingestor.batch_setup(ctx)
    assert ingestor.prepare_batch_data(batch, ctx) is None
    ingestor.cleanup_batch_data(batch, ctx)
    ingestor.batch_teardown(ctx)
    assert ingestor.item_size_bytes(item) == item.stat().st_size
    assert ingestor.get_batch_groups([item], ctx)


@pytest.mark.unit
def test_abstract_template_product_name_exemption_survives() -> None:
    class _AbstractTemplateProbe(BaseIngestor):
        @abstractmethod
        def build_dataset(self) -> None:
            raise NotImplementedError

        def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
            raise NotImplementedError

        def _aggregate_metrics(
            self,
            ctx: RuntimeIngestContext,
            state: PipelineRunState,
        ) -> dict[str, Any]:
            return {}

    assert "PRODUCT_NAME" not in _AbstractTemplateProbe.__dict__


@pytest.mark.unit
def test_concrete_base_subclass_still_requires_product_name() -> None:
    with pytest.raises(TypeError, match="PRODUCT_NAME"):

        class _ConcreteProbe(BaseIngestor):
            def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
                raise NotImplementedError

            def _aggregate_metrics(
                self,
                ctx: RuntimeIngestContext,
                state: PipelineRunState,
            ) -> dict[str, Any]:
                return {}
