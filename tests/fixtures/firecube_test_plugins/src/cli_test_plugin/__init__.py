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

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from firecube.sdk import (
    AttributePatch,
    BaseIngestor,
    MetadataPlan,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginConfig,
    PluginContext,
    RuntimeIngestContext,
    StoreView,
    register_ingestor,
)

PRODUCT_NAME = "cli_test_product"


@dataclass
class CliTestPluginConfig(PluginConfig):
    test_int: int = 0
    test_str: str = "default"
    test_bool: bool = False


@register_ingestor("cli_test_plugin")
class CliTestIngestor(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = PRODUCT_NAME
    plugin_config_class = CliTestPluginConfig

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=Path(ctx.target or "")))

    def _aggregate_metrics(
        self,
        ctx: RuntimeIngestContext,
        state: PipelineRunState,
    ) -> Mapping[str, Any]:
        return self.default_aggregate_metrics(ctx, state)


class _FixtureStep:
    """Base of the migration steps that the migrate tests install under `firecube.migrations`."""

    id: str = ""
    depends_on: tuple[str, ...] = ()
    from_release: str | None = None
    to_release: str | None = None
    descriptive_keys: frozenset[str] = frozenset()

    def __init__(self, options: Mapping[str, Any]) -> None:
        self.options = options

    def plan(self, view: StoreView) -> MetadataPlan:
        raise NotImplementedError


REPLAN_FORBIDDEN = False  # a test sets it to prove that a resume replays the journal


class RetitleStep(_FixtureStep):
    """Set two root attributes and one attribute of the array `temp`."""

    id = "cli-test:retitle"

    def plan(self, view: StoreView) -> MetadataPlan:
        if REPLAN_FORBIDDEN:
            raise AssertionError("plan() was called again after the migration was interrupted")
        return (
            AttributePatch("", set={"title": "Retitled", "institution": "Fixture Institute"}),
            AttributePatch("temp", set={"long_name": "Air temperature"}),
        )


class ReleaseStep(_FixtureStep):
    """Set one root attribute and cut release 0.1.1 inside the run."""

    id = "cli-test:release"
    to_release = "0.1.1"

    def plan(self, view: StoreView) -> MetadataPlan:
        return (AttributePatch("", set={"product_version": "0.1.1"}),)


class NeedsRetitleStep(_FixtureStep):
    """Set a root comment once `cli-test:retitle` has run."""

    id = "cli-test:needs-retitle"
    depends_on = ("cli-test:retitle",)

    def plan(self, view: StoreView) -> MetadataPlan:
        return (AttributePatch("", set={"comment": "after retitle"}),)


class FromReleaseStep(_FixtureStep):
    """Set a root comment, but only on a store at release 9.9.9."""

    id = "cli-test:from-release"
    from_release = "9.9.9"

    def plan(self, view: StoreView) -> MetadataPlan:
        return (AttributePatch("", set={"comment": "from 9.9.9"}),)
