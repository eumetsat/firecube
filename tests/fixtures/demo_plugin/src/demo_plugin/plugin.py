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

from dataclasses import dataclass

from firecube.ingestor.api import (
    BaseIngestor,
    IngestContext,
    IngestResult,
    PluginConfig,
    register_ingestor,
)


@dataclass
class DemoPluginConfig(PluginConfig):
    greeting: str = "Hello"
    count: int = 1
    verbose: bool = False


@register_ingestor("demo_plugin")
class DemoIngestor(BaseIngestor):
    """A demo ingestor for testing."""

    PRODUCT_NAME = "demo_plugin"
    plugin_config_class = DemoPluginConfig

    def run(self, ctx: IngestContext) -> IngestResult:
        _ = ctx
        print("Demo run")
        return IngestResult(output_format="demo")

    def _aggregate_metrics(self, ctx, state):
        return {}
