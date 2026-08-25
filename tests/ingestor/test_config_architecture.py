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

"""Verification tests for 3-Tier Configuration Architecture."""

from dataclasses import dataclass
from typing import Any

import pytest

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.runtime.configure import ensure_run_id
from firecube.ingestor.templates.config import TemplateConfig
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import IngestContext

# --- Mocks ---


@dataclass
class MockTemplateConfig(TemplateConfig):
    template_param: str = "default"


@dataclass
class MockPluginConfig(PluginConfig):
    plugin_param: int = 1


class MockIngestor(BaseIngestor):
    PRODUCT_NAME = "mock_tier_ingestor"
    name = "mock_tier_ingestor"
    template_config_class = MockTemplateConfig
    plugin_config_class = MockPluginConfig

    def _process_batch(self, batch, ctx):
        from firecube.ingestor.types.context import OutputPaths, PipelineResult

        return PipelineResult(batch=batch, outputs=OutputPaths(primary=""), success=True)

    def _aggregate_metrics(self, ctx, state):
        return {}

    def ingest(self, ctx: IngestContext) -> Any:
        return None  # No-op


# --- Tests ---


def test_engine_config_strictness():
    """Verify EngineConfig rejects unknown keys."""
    # Valid
    cfg = EngineConfig.from_options({"pipeline_workers": 2})
    assert cfg.pipeline_workers == 2

    # Invalid
    with pytest.raises(ValueError, match="Unknown Engine options"):
        EngineConfig.from_options({"invalid_engine_key": "foo"})


def test_option_split_logic():
    """Verify BaseIngestor correctly splits options into tiers."""
    ingestor = MockIngestor()

    options = {
        # Engine
        "pipeline_workers": "2",
        "cleanup_workspace": "false",
        # Template
        "template_param": "custom_template",
        # Plugin
        "plugin_param": "42",
        # System (Ignored but allowed)
        "run_id": "test_run",
    }

    ctx = IngestContext(source=".", target=".", options=options)

    engine_cfg, template_cfg, plugin_cfg = ingestor._configurator.configure(ctx)
    ingestor.engine_config = engine_cfg
    ingestor.template_config = template_cfg
    ingestor.plugin_config = plugin_cfg

    # Verify Engine
    assert ingestor.engine_config.pipeline_workers == 2
    assert ingestor.engine_config.cleanup_workspace is False

    # Verify Template
    assert isinstance(ingestor.template_config, MockTemplateConfig)
    assert ingestor.template_config.template_param == "custom_template"

    # Verify Plugin
    assert isinstance(ingestor.plugin_config, MockPluginConfig)
    assert ingestor.plugin_config.plugin_param == 42


def test_union_validator_rejects_typos():
    """Verify unknown keys raise error even if valid in other tiers?"""
    ingestor = MockIngestor()

    options = {
        "pipeline_workers": "2",
        "typo_param": "foo",  # Unknown
    }

    ctx = IngestContext(source=".", target=".", options=options)

    with pytest.raises(ValueError, match="Unknown configuration options"):
        ingestor._configurator.configure(ctx)


def test_collision_detection_at_build_time():
    """Verify conflicting keys raise TypeError on class definition."""

    with pytest.raises(TypeError, match=r"Configuration key collision \(Template/Plugin Config"):

        @dataclass
        class ConflictingPluginConfig(PluginConfig):
            template_param: str = "conflict"  # Same as MockTemplateConfig

        class BadIngestor(BaseIngestor):
            PRODUCT_NAME = "bad"
            name = "bad"
            template_config_class = MockTemplateConfig
            plugin_config_class = ConflictingPluginConfig


def test_collision_with_engine():
    """Verify plugin cannot override engine keys."""
    with pytest.raises(TypeError, match=r"Configuration key collision \(Engine/Plugin Config"):

        @dataclass
        class EngineOverridePlugin(PluginConfig):
            pipeline_workers: int = 1

        class EngineBadIngestor(BaseIngestor):
            PRODUCT_NAME = "engine_bad"
            name = "engine_bad"
            plugin_config_class = EngineOverridePlugin


def test_describe_options_structure():
    """Verify CLI introspection output."""
    desc = MockIngestor.describe_options()

    assert "Engine Options" in desc
    assert "pipeline_workers" in desc["Engine Options"]

    assert "Template Options" in desc
    assert "template_param" in desc["Template Options"]

    assert "Plugin Options" in desc
    assert "plugin_param" in desc["Plugin Options"]

    assert "System Keys" in desc
    assert "run_id" in desc["System Keys"]


def test_configure_is_pure_no_context_side_effects():
    ingestor = MockIngestor()
    options = {"force_reingest": True, "incremental": True, "dry_run": True}
    ctx = IngestContext(source=".", target=".", options=dict(options))

    engine_cfg, _, _ = ingestor._configurator.configure(ctx)

    assert engine_cfg.force_reingest is True
    assert engine_cfg.incremental is True
    assert engine_cfg.dry_run is True
    assert ctx.options == options
    assert not hasattr(ctx, "force_reingest")
    assert not hasattr(ctx, "incremental")
    assert not hasattr(ctx, "dry_run")


def test_ensure_run_id_is_pure():
    ctx = IngestContext(source=".", options={})

    run_id = ensure_run_id(ctx=ctx, plugin_name="mock")

    assert run_id.startswith("mock-")
    assert ctx.run_id is None
    assert "run_id" not in ctx.options
