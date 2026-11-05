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

"""The contract between a metadata migration step and the executor that runs it.

A step reads a `StoreView` and returns a `MetadataPlan`. The executor owns every
read and write. A plan changes node attributes only, so a step never moves or
rewrites a chunk.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Literal, Protocol

from firecube.core.zarr.metadata_documents import ABSENT, NodeView

__all__ = [
    "ABSENT",
    "AttributePatch",
    "MetadataPlan",
    "MigrationStep",
    "NodeView",
    "ReleaseRecord",
    "StoreView",
]


def _node_path(path: str) -> str:
    """Spell a node path without leading or trailing slashes; `""` and `"/"` name the root."""
    if not isinstance(path, str):
        raise TypeError(f"a node path is a string, not {type(path).__name__}")
    return path.strip("/")


@dataclass(frozen=True)
class AttributePatch:
    """Set and remove attributes of one node.

    Keys outside `set` and `remove` keep their values. A key that `set` adds goes last.

    Attributes:
        node_path: The node path, `""` for the root group. Leading and trailing slashes are dropped.
        set: Attribute names and the JSON values to give them.
        remove: Attribute names to drop. A name the node lacks is ignored.

    Raises:
        TypeError: `set` is not a mapping with string keys, or `remove` holds a non-string.
        ValueError: A name is both in `set` and in `remove`.

    Examples:
        Set a title on the root group and drop a comment from an array:

            (
                AttributePatch("", set={"title": "Daily rainfall"}),
                AttributePatch("precip", remove=("comment",)),
            )
    """

    node_path: str
    set: Mapping[str, Any] = field(default_factory=dict)
    remove: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.set, Mapping) or not all(isinstance(k, str) for k in self.set):
            raise TypeError("AttributePatch.set must be a mapping with string keys")
        remove = (self.remove,) if isinstance(self.remove, str) else tuple(self.remove)
        if not all(isinstance(name, str) for name in remove):
            raise TypeError("AttributePatch.remove must hold attribute names")
        overlap = sorted(set(self.set) & set(remove))
        if overlap:
            raise ValueError(f"attributes both set and removed: {', '.join(overlap)}")
        object.__setattr__(self, "node_path", _node_path(self.node_path))
        object.__setattr__(self, "set", MappingProxyType(dict(self.set)))
        object.__setattr__(self, "remove", remove)


MetadataPlan = tuple[AttributePatch, ...]
"""The changes a migration step plans, applied in order.

An `AttributePatch` is the only change a plan can hold. Two patches of one node
apply one after the other.
"""


@dataclass(frozen=True)
class ReleaseRecord:
    """A named release of a product, bound to one coherent metadata snapshot.

    Attributes:
        version: The release name, kept verbatim. Firecube only requires it to be unique.
        created_at: Unix time in seconds when the release was created.
        previous: The version of the release before this one, or `None` for the first.
        snapshot: The id of the metadata snapshot the release names.
        runs_since_previous: The ids of the runs recorded since the previous release.
        producer: The producer identity of the latest run that records one, or `None`.
        producer_status: `"recorded"` when `producer` is set, `"unknown"` when no run records one.
        note: The free text given at creation, or `None`.
        created_by_run: The id of the maintenance run that created the release.
        retention: What the release retains of the chunk bytes, or `None` when it retains none.
    """

    version: str
    created_at: float
    previous: str | None
    snapshot: str
    runs_since_previous: tuple[str, ...]
    producer: Mapping[str, Any] | None
    producer_status: Literal["recorded", "unknown"]
    note: str | None
    created_by_run: str
    retention: Mapping[str, Any] | None = None


class StoreView:
    """A read-only view of a store's metadata, as one metadata snapshot captured it.

    The executor builds it from the snapshot it takes before a step plans, so the
    plan and the run's journal see the same documents. Paths have no leading or
    trailing slash, and `""` is the root group.

    Args:
        nodes: The parsed nodes of the store.
        release: The product's current release, or `None` when it has none.
        now: The time the executor started the run, timezone-aware.

    Raises:
        ValueError: Two nodes share a path.
    """

    __slots__ = ("_nodes", "_now", "_release")

    def __init__(
        self, nodes: Iterable[NodeView], *, release: ReleaseRecord | None, now: datetime
    ) -> None:
        by_path: dict[str, NodeView] = {}
        for node in nodes:
            if node.path in by_path:
                raise ValueError(f"two nodes share the path {node.path!r}")
            by_path[node.path] = node
        self._nodes = MappingProxyType(dict(sorted(by_path.items())))
        self._release = release
        self._now = now

    @property
    def release(self) -> ReleaseRecord | None:
        """The product's current release, or `None` when it has none."""
        return self._release

    @property
    def now(self) -> datetime:
        """The time the executor started the run, timezone-aware."""
        return self._now

    def _node(self, path: str, node_type: str) -> NodeView:
        key = _node_path(path)
        node = self._nodes.get(key)
        if node is None or node.node_type != node_type:
            raise KeyError(f"the store has no {node_type} at {key!r}")
        return node

    def group(self, path: str) -> NodeView:
        """Return the group at `path`.

        Raises:
            KeyError: No group stands at `path`.
        """
        return self._node(path, "group")

    def array(self, path: str) -> NodeView:
        """Return the array at `path`.

        Raises:
            KeyError: No array stands at `path`.
        """
        return self._node(path, "array")

    def nodes(self) -> Iterator[NodeView]:
        """Yield every group and array of the store, sorted by path."""
        return iter(self._nodes.values())


class MigrationStep(Protocol):
    """A metadata-only migration a plugin declares.

    The executor runs a step once per store under an exclusive claim. It refuses
    a rerun of the same step at another distribution version.

    Attributes:
        id: The step's name, unique among installed steps, for example `"my-plugin:0.1.0-0.1.1"`.
        depends_on: The ids of steps the store must have applied first.
        from_release: The release the store must be at, or `None` for any.
        to_release: The release the executor creates after the step, or `None` for none.
        descriptive_keys: Attribute names the step declares descriptive, beyond the built-in list.
            They cannot make a reserved or interpretation attribute descriptive.

    Examples:
        A step that titles the root group:

            class AddTitle:
                id = "my-plugin:add-title"
                depends_on = ()
                from_release = None
                to_release = None
                descriptive_keys = frozenset()

                def plan(self, view):
                    return (AttributePatch("", set={"title": "Daily rainfall"}),)
    """

    id: str
    depends_on: tuple[str, ...]
    from_release: str | None
    to_release: str | None
    descriptive_keys: Collection[str]

    def plan(self, view: StoreView) -> MetadataPlan:
        """Return the changes the step makes to the store `view` shows.

        Read only `view`: no I/O, no clock, no randomness. The same view must give the same plan.

        Args:
            view: The store's metadata before the step.

        Returns:
            The attribute patches, applied in order.
        """
        ...
