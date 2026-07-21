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

"""Registry of array attribute keys that firecube manages internally.

Plugins must not set these keys in ZarrArraySpec.attrs; doing so would
conflict with firecube's own internal bookkeeping.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

__all__ = ["RESERVED_ARRAY_ATTRS", "assert_attrs_safe"]

RESERVED_ARRAY_ATTRS: frozenset[str] = frozenset(
    {
        "_ARRAY_DIMENSIONS",
        "_FillValue",
        "firecube_run_id",
        "firecube_span_id",
        "firecube_internal",
        "firecube_static_written",
    }
)


def assert_attrs_safe(attrs: Mapping[str, Any]) -> None:
    """Raise ValueError if *attrs* contains any reserved key."""
    for key in attrs:
        if key in RESERVED_ARRAY_ATTRS:
            raise ValueError(
                f"Reserved attr {key!r} is managed by firecube, not the plugin. "
                "Remove it from ZarrArraySpec.attrs."
            )
