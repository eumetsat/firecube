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

from collections.abc import Sequence
from typing import Protocol, cast

from firecube.core.zarr._coord_chunks import resolve_coord_chunks


class _ResolvedIndexLike(Protocol):
    def axis_for(self, group: str) -> object | None: ...

    def size(self, group: str) -> int: ...


class _ArraySpecLike(Protocol):
    # Structural view of the ingestor-owned ZarrArraySpec: core must not
    # import the ingestor layer, and only these three fields are read here.
    @property
    def name(self) -> str: ...

    @property
    def time_indexed(self) -> bool: ...

    @property
    def shape(self) -> tuple[int, ...]: ...


class _GroupSpecLike(Protocol):
    @property
    def group(self) -> str: ...

    @property
    def arrays(self) -> Sequence[_ArraySpecLike]: ...


def coord_chunk_sizes_by_group(
    schema: Sequence[_GroupSpecLike],
    resolved_index: object,
    windows_by_group: dict[str, tuple[int, int]],
) -> dict[str, int]:
    """Resolve the coordinate-array chunk extent per windowed group."""
    sizes: dict[str, int] = {}
    index = cast(_ResolvedIndexLike, resolved_index)
    schema_by_group = {group_spec.group: group_spec for group_spec in schema}
    for group in windows_by_group:
        group_spec = schema_by_group.get(group)
        if group_spec is None:
            continue
        axis = index.axis_for(group)
        coord_name = getattr(axis, "coordinate", None)
        if not isinstance(coord_name, str):
            continue
        coord_spec = next(
            (
                array_spec
                for array_spec in group_spec.arrays
                if array_spec.name == coord_name
                and array_spec.time_indexed
                and len(array_spec.shape) == 1
            ),
            None,
        )
        sizes[group] = int(resolve_coord_chunks(coord_spec, int(index.size(group)))[0])
    return sizes
