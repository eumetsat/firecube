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

from dataclasses import dataclass

import click
import pytest

from firecube.cli._typed_options import TypedOptionsParam
from firecube.ingestor.config.engine import is_experimental_option_key
from firecube.ingestor.runtime.configure import TierConfigurator
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import IngestContext


@dataclass
class _ExperimentalTestPluginConfig(PluginConfig):
    plugin_flag: bool = False


@pytest.mark.unit
def test_x_prefix_passes_typed_options_param() -> None:
    """`--option x_foo=1` bypasses strict unknown-key rejection and reaches
    plugins as the untyped raw string ``"1"``."""
    key, value = TypedOptionsParam("cli_test_plugin").convert("x_foo=1", None, None)
    assert key == "x_foo"
    assert value == "1"
    assert is_experimental_option_key("x_foo") is True


@pytest.mark.unit
def test_unknown_non_x_key_still_rejected() -> None:
    """Unknown non-experimental keys remain hard-rejected with remediation
    that mentions the ``x_`` namespace."""
    with pytest.raises(click.BadParameter) as exc_info:
        TypedOptionsParam("cli_test_plugin").convert("zz_bogus=1", None, None)

    message = str(exc_info.value)
    assert "Unknown option 'zz_bogus' for plugin 'cli_test_plugin'" in message
    assert "x_ prefix" in message
    assert is_experimental_option_key("zz_bogus") is False


@pytest.mark.unit
def test_x_write_mode_does_not_shadow_typed_write_mode() -> None:
    """``x_write_mode`` is a distinct experimental key — it does NOT collide
    with the typed-flag-owned ``write_mode`` rejection and does NOT override
    the engine's resolved ``write_mode``."""
    key, value = TypedOptionsParam("cli_test_plugin").convert("x_write_mode=staged", None, None)
    assert key == "x_write_mode"
    assert value == "staged"

    ctx = IngestContext(
        source="input",
        options={"write_mode": "direct", "x_write_mode": "staged"},
    )
    configurator = TierConfigurator(
        template_config_class=None,
        plugin_config_class=None,
        plugin_name="cli_test_plugin",
    )
    engine_config, _template, _plugin = configurator.configure(ctx)

    assert engine_config.write_mode == "direct"
    assert ctx.options["x_write_mode"] == "staged"

    with pytest.raises(click.BadParameter):
        TypedOptionsParam("cli_test_plugin").convert("write_mode=staged", None, None)


@pytest.mark.unit
def test_x_prefix_passes_through_config_file_options(tmp_path) -> None:
    """A config-file ``[plugins.<name>] x_bar = true`` value reaches the
    tier configurator without being rejected as an unknown key, and remains
    in ``ctx.options`` for plugins to read untyped."""
    from firecube.core.config import get_plugin_defaults, load_config_file

    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[plugins.experimental_test_plugin]",
                "plugin_flag = true",
                "x_bar = true",
                "x_nested_key_42 = 'hello'",
                "",
            ]
        ),
        encoding="utf-8",
    )
    cfg = load_config_file(config_path)
    options = get_plugin_defaults(cfg, "experimental_test_plugin")
    ctx = IngestContext(source="input", options=options)

    configurator = TierConfigurator(
        template_config_class=None,
        plugin_config_class=_ExperimentalTestPluginConfig,
        plugin_name="experimental_test_plugin",
    )
    engine_config, _template, plugin_config = configurator.configure(ctx)

    assert engine_config is not None
    assert plugin_config is not None
    assert ctx.options["x_bar"] is True
    assert ctx.options["x_nested_key_42"] == "hello"
