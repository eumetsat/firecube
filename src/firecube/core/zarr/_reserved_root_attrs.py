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

"""Guard reserved root attrs supplied by external writers.

This guard is for code paths that accept root attrs from EXTERNAL sources — user CLI options,
plugin output, or any other non-firecube author. Use this guard wherever such attrs are about
to be persisted to a Zarr root.

Firecube-internal writers MUST NOT call this guard. The slot-index service
(ChunkManager.ensure_slot_index_model._mirror_attrs) is the authoritative writer for the
reserved root attrs and writes them DIRECTLY. Drift detection on subsequent runs is handled by
the precedence matrix in _apply_slot_model_precedence, not by this guard.

In short: "assert no reserved root attrs appear in user/plugin-supplied attrs" — that is the
entire contract.

This module is DISTINCT from _reserved_attrs.py which guards array attributes. Root attributes
and array attributes are separate protection surfaces.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from firecube.core.controlplane.types import (
    RESOLVED_INDEX_ATTR,
    RESOLVED_INDEX_IDENTITY_HASH_ATTR,
)
from firecube.core.slot_index import (
    SLOT_INDEX_MODEL_ATTR,
    SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
)

__all__ = ["RESERVED_ROOT_ATTRS", "assert_root_attrs_safe"]

RESERVED_ROOT_ATTRS: frozenset[str] = frozenset(
    {
        SLOT_INDEX_MODEL_ATTR,
        SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
        RESOLVED_INDEX_ATTR,
        RESOLVED_INDEX_IDENTITY_HASH_ATTR,
    }
)


def assert_root_attrs_safe(attrs: Mapping[str, Any]) -> None:
    """Raise ValueError if any key in attrs is a reserved firecube root attribute name.

    The check is NAME-ONLY: the value content of the attr is NOT inspected.
    Even a "correct-looking" value from user code is refused because this guard
    exists to prevent user/plugin code paths from writing into the reserved namespace,
    not to validate the value.

    Args:
        attrs: Mapping of root attribute names to values (typically from user/plugin
               code before persisting to a Zarr root).

    Raises:
        ValueError: If any key in attrs overlaps with RESERVED_ROOT_ATTRS.
    """
    offending = RESERVED_ROOT_ATTRS.intersection(attrs)
    if offending:
        raise ValueError(
            f"attrs contain reserved firecube root attribute(s): {sorted(offending)!r}. "
            f"Reserved root attributes are managed internally by firecube "
            f"(see RESERVED_ROOT_ATTRS = {sorted(RESERVED_ROOT_ATTRS)!r})."
        )
