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
from typing import Any, Final

__all__ = [
    "FIRECUBE_GROUP_IDENTITY_HASH_ATTR",
    "FIRECUBE_STATIC_WRITTEN_ATTR",
    "RESERVED_ARRAY_ATTRS",
    "assert_attrs_safe",
]

_ARRAY_DIMENSIONS_ATTR: Final[str] = "_ARRAY_DIMENSIONS"
_FILL_VALUE_ATTR: Final[str] = "_FillValue"
_FIRECUBE_RUN_ID_ATTR: Final[str] = "firecube_run_id"
_FIRECUBE_SPAN_ID_ATTR: Final[str] = "firecube_span_id"
_FIRECUBE_INTERNAL_ATTR: Final[str] = "firecube_internal"
FIRECUBE_STATIC_WRITTEN_ATTR: Final[str] = "firecube_static_written"
"""Zarr array attr Firecube stamps after a static array commit.

Examples:
    >>> FIRECUBE_STATIC_WRITTEN_ATTR
    'firecube_static_written'
"""

FIRECUBE_GROUP_IDENTITY_HASH_ATTR: Final[str] = "firecube_group_identity_hash"
"""Zarr array attr Firecube stamps on a bounded group's coord array.

Mirrors the per-group identity hash so ingest startup can verify each
bounded group independently for mixed-spec cubes without persisting a
full resolved-index record.

Examples:
    >>> FIRECUBE_GROUP_IDENTITY_HASH_ATTR
    'firecube_group_identity_hash'
"""

RESERVED_ARRAY_ATTRS: frozenset[str] = frozenset(
    {
        _ARRAY_DIMENSIONS_ATTR,
        _FILL_VALUE_ATTR,
        _FIRECUBE_RUN_ID_ATTR,
        _FIRECUBE_SPAN_ID_ATTR,
        _FIRECUBE_INTERNAL_ATTR,
        FIRECUBE_STATIC_WRITTEN_ATTR,
        FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
        "firecube_coord_managed",
        "firecube_preallocated",
        "firecube_consolidated_at",
    }
)
"""Reserved Zarr array attribute keys managed by Firecube.

Examples:
    >>> "firecube_static_written" in RESERVED_ARRAY_ATTRS
    True
    >>> "my_custom_attr" in RESERVED_ARRAY_ATTRS
    False
"""


def assert_attrs_safe(attrs: Mapping[str, Any]) -> None:
    """Raise ValueError if *attrs* contains any reserved key.

    Args:
        attrs: Candidate attribute mapping for ``ZarrArraySpec.attrs``.

    Returns:
        None: The function returns nothing.

    Raises:
        ValueError: If any key is reserved for Firecube bookkeeping.

    Examples:
        >>> assert_attrs_safe({"my_custom_attr": 42})
        >>> assert_attrs_safe({"firecube_static_written": True})
        Traceback (most recent call last):
        ...
        ValueError: Reserved attr 'firecube_static_written' is managed by firecube, not the plugin. Remove it from ZarrArraySpec.attrs.
    """
    for key in attrs:
        if key in RESERVED_ARRAY_ATTRS:
            raise ValueError(
                f"Reserved attr {key!r} is managed by firecube, not the plugin. "
                "Remove it from ZarrArraySpec.attrs."
            )
