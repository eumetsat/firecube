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

"""Plugin version compatibility check.

A plugin may declare ``firecube_core_min_version`` (string, PEP 440) on its
registered ingestor class. When ``discover_ingestors()`` discovers a plugin, the
declared minimum is compared against the running ``firecube.ingestor`` version.

The check is intentionally permissive: a missing declaration is fine, an
unparseable value is fine, and even an incompatible value only emits a warning.
The goal is to surface *informational* drift between the core plugin contract
and out-of-tree plugin builds, not to block ingestion.
"""

from __future__ import annotations

import logging
from typing import Any

from firecube.ingestor import __version__ as CORE_VERSION

_LOG = logging.getLogger("firecube.registry.version_compat")

VERSION_ATTR = "firecube_core_min_version"


def _parse(value: str):
    from packaging.version import InvalidVersion, Version

    try:
        return Version(value)
    except InvalidVersion:
        return None


def check_plugin_compatibility(
    plugin_name: str,
    plugin_cls: type[Any],
    *,
    core_version: str = CORE_VERSION,
) -> str | None:
    """Compare a plugin's declared minimum core version against ``core_version``.

    Returns a human-readable warning message when the plugin requires a newer
    core than the one currently running, otherwise ``None``. The message is
    purely informational — callers decide whether to log/raise.
    """
    declared = getattr(plugin_cls, VERSION_ATTR, None)
    if declared is None:
        return None
    if not isinstance(declared, str):
        return None

    plugin_min = _parse(declared)
    core_now = _parse(core_version)
    if plugin_min is None or core_now is None:
        return None

    if core_now >= plugin_min:
        return None

    return (
        f"Plugin '{plugin_name}' requires firecube.ingestor>={declared}, "
        f"but {core_version} is installed. The plugin may not work correctly."
    )


def warn_if_incompatible(
    plugin_name: str,
    plugin_cls: type[Any],
    *,
    core_version: str = CORE_VERSION,
    logger: logging.Logger | None = None,
) -> bool:
    """Emit a warning via ``logger`` when the plugin declares an incompatible minimum.

    Returns True when a warning was emitted, False otherwise.
    """
    message = check_plugin_compatibility(plugin_name, plugin_cls, core_version=core_version)
    if message is None:
        return False
    (logger or _LOG).warning(message)
    return True
