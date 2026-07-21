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

import logging

from firecube.ingestor.registry.version_compat import (
    VERSION_ATTR,
    check_plugin_compatibility,
    warn_if_incompatible,
)


class _PluginNoDeclaration:
    pass


class _PluginMatching:
    firecube_core_min_version = "0.2.0"


class _PluginRequiresNewer:
    firecube_core_min_version = "9.9.9"


class _PluginRequiresOlder:
    firecube_core_min_version = "0.1.0"


class _PluginInvalidVersion:
    firecube_core_min_version = "not-a-version"


def test_matching_version_emits_no_warning(caplog) -> None:
    caplog.set_level(logging.WARNING)
    message = check_plugin_compatibility("matching", _PluginMatching, core_version="0.2.0")
    emitted = warn_if_incompatible("matching", _PluginMatching, core_version="0.2.0")
    assert message is None
    assert emitted is False
    assert not caplog.records


def test_incompatible_older_core_emits_warning(caplog) -> None:
    caplog.set_level(logging.WARNING)
    message = check_plugin_compatibility("needs_newer", _PluginRequiresNewer, core_version="0.2.0")
    emitted = warn_if_incompatible("needs_newer", _PluginRequiresNewer, core_version="0.2.0")
    assert message is not None
    assert "needs_newer" in message
    assert "9.9.9" in message
    assert "0.2.0" in message
    assert emitted is True
    assert any("needs_newer" in record.message for record in caplog.records)


def test_no_declaration_skips_check(caplog) -> None:
    caplog.set_level(logging.WARNING)
    message = check_plugin_compatibility("no_decl", _PluginNoDeclaration, core_version="0.2.0")
    emitted = warn_if_incompatible("no_decl", _PluginNoDeclaration, core_version="0.2.0")
    assert message is None
    assert emitted is False
    assert not caplog.records


def test_older_min_version_compatible_with_newer_core() -> None:
    message = check_plugin_compatibility("compat", _PluginRequiresOlder, core_version="0.2.0")
    assert message is None


def test_invalid_version_string_is_silently_ignored(caplog) -> None:
    caplog.set_level(logging.WARNING)
    message = check_plugin_compatibility("garbage", _PluginInvalidVersion, core_version="0.2.0")
    emitted = warn_if_incompatible("garbage", _PluginInvalidVersion, core_version="0.2.0")
    assert message is None
    assert emitted is False
    assert not caplog.records


def test_version_attr_constant_matches_documented_name() -> None:
    assert VERSION_ATTR == "firecube_core_min_version"
