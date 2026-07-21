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

import click

from firecube.ingestor.runtime.configure import TierConfigurator
from firecube.ingestor.types.context import IngestContext


def test_output_name_option_is_rejected_with_migration_hint() -> None:
    configurator = TierConfigurator(None, None, plugin_name="cli_test_plugin")
    ctx = IngestContext(source="/tmp/source", options={"output_name": "legacy"})

    try:
        configurator.configure(ctx)
    except click.UsageError as exc:
        message = str(exc)
        assert "output_name" in message
        assert "--product-name" in message
        assert "default_product_name" in message
    else:
        raise AssertionError("output_name should be rejected")
