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

_LEGACY_DEFAULT_CAP = (
    256  # NOTE: matches legacy hardcoded formula in _materialize_irregular_coord_array
)


def resolve_coord_chunks(spec: object | None, n: int) -> tuple[int, ...]:
    """Resolve coordinate-array chunk shape from a ZarrArraySpec with a spec-driven default.

    Args:
        spec: Optional array spec from which to read declared chunks. When spec is not
            None and spec.chunks is not None, those chunks are returned verbatim.
        n: Total number of coordinate elements. Used only when computing the default.

    Returns:
        A one-element tuple representing the chunk shape along the coordinate axis.

    Raises:
        ValueError: If spec.chunks is provided with a rank other than 1.
    """
    if spec is not None:
        chunks = getattr(spec, "chunks", None)
        if chunks is not None:
            if len(chunks) != 1:
                raise ValueError(
                    f"resolve_coord_chunks: spec.chunks must be rank-1, got {chunks!r}"
                )
            return tuple(chunks)
    return (max(1, min(_LEGACY_DEFAULT_CAP, n)),)
