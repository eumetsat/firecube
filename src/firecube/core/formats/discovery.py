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

"""Deprecated module path: input file discovery moved to ``firecube.core.discovery``.

Import ``discover_input_files`` from ``firecube.core.api`` instead.
"""

from __future__ import annotations

import warnings

from firecube.core.discovery import (
    DEFAULT_INCLUDE_SUFFIXES,
    KNOWN_EXTENSIONS,
    discover_input_files,
)

warnings.warn(
    "firecube.core.formats.discovery is deprecated; import discover_input_files from "
    "firecube.core.api (implemented in firecube.core.discovery).",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["DEFAULT_INCLUDE_SUFFIXES", "KNOWN_EXTENSIONS", "discover_input_files"]
