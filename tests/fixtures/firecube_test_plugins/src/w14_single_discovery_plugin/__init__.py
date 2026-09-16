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

"""Fixture ingestors exercising single-discovery guarantees.

The three ingestors here back ``tests/integration/test_zero_input_no_firecube_created.py``:

- ``OverrideEmptyIngestor``: overrides ``discover_source_files`` to return an
  empty list; the pipeline treats plugin-overridden discovery as legitimate
  even when the resulting item set is empty.
- ``CounterSuccessIngestor``: returns a single item; used to prove that
  ``discover_source_files`` is called exactly once per ingest.
- ``LazySuccessIngestor``: same behavior via a generator so the engine cannot
  quietly re-iterate the discovery result.

Each hook bumps a counter file identified by ``W14_DISCOVERY_COUNTER``; the
tests read the counter to assert single-invocation.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any, ClassVar

from firecube.ingestor.api import (
    BaseIngestor,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    register_ingestor,
)


def _bump() -> None:
    counter = os.environ.get("W14_DISCOVERY_COUNTER")
    if not counter:
        return
    path = Path(counter)
    current = int(path.read_text(encoding="utf-8") or "0") if path.exists() else 0
    path.write_text(str(current + 1), encoding="utf-8")


class _Base(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "w14_single_discovery"

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return PipelineResult(
            batch=batch,
            outputs=OutputPaths(primary=str(ctx.target or "")),
            success=True,
        )


@register_ingestor("w14_override_empty")
class OverrideEmptyIngestor(_Base):
    PRODUCT_NAME: ClassVar[str] = "w14_override_empty"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        _bump()
        return []


@register_ingestor("w14_counter_success")
class CounterSuccessIngestor(_Base):
    PRODUCT_NAME: ClassVar[str] = "w14_counter_success"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        _bump()
        return [Path(ctx.source) / "one.nc"]


@register_ingestor("w14_lazy_success")
class LazySuccessIngestor(_Base):
    PRODUCT_NAME: ClassVar[str] = "w14_lazy_success"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        _bump()
        if int(Path(os.environ["W14_DISCOVERY_COUNTER"]).read_text(encoding="utf-8")) > 1:
            raise RuntimeError("discover_source_files called more than once")
        yield Path(ctx.source) / "one.nc"
