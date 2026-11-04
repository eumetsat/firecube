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

"""The built-in `firecube:set-attrs` migration step."""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from typing import Any

from firecube.ingestor.contracts.migration import AttributePatch, MetadataPlan, StoreView

__all__ = ["SetAttrs"]


def _parse_assignment(item: str) -> tuple[str, str]:
    """Split `KEY=VALUE` at the first `=`; the value is kept as a string, `=` signs included."""
    key, sep, value = item.partition("=")
    if not sep or not key:
        raise ValueError(f"expected KEY=VALUE, got {item!r}")
    return key, value


def _strings(value: Any, option: str) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, Iterable):
        raise TypeError(f"option {option!r} must be a list of strings")
    items = tuple(value)
    if not all(isinstance(item, str) for item in items):
        raise TypeError(f"option {option!r} must be a list of strings")
    return items


class SetAttrs:
    """Set and remove attributes of one node.

    Every set value is a string. The step plans one `AttributePatch` and fails when the
    node does not exist or a name is both set and removed.

    Attributes:
        id: `"firecube:set-attrs"`.
        depends_on: Empty.
        from_release: `None`: any release.
        to_release: `None`: no release is created.
        descriptive_keys: Empty.

    Args:
        options: The step options. `set` is a list of `KEY=VALUE` strings, `remove` a list of
            names, `node` a node path that defaults to the root group (`""` or `"/"`).

    Raises:
        TypeError: An option has the wrong type.
        ValueError: A `set` item is not `KEY=VALUE`, a key is set twice, a name is both set and
            removed, or an option is unknown.
    """

    id = "firecube:set-attrs"
    depends_on: tuple[str, ...] = ()
    from_release: str | None = None
    to_release: str | None = None
    descriptive_keys: Collection[str] = frozenset()

    def __init__(self, options: Mapping[str, Any]) -> None:
        unknown = sorted(set(options) - {"set", "remove", "node"})
        if unknown:
            raise ValueError(f"unknown option: {', '.join(unknown)}")
        assignments: dict[str, str] = {}
        for item in _strings(options.get("set", ()), "set"):
            key, value = _parse_assignment(item)
            if key in assignments:
                raise ValueError(f"attribute {key!r} is set twice")
            assignments[key] = value
        node = options.get("node", "")
        if not isinstance(node, str):
            raise TypeError("option 'node' must be a string")
        self._patch = AttributePatch(
            node, set=assignments, remove=_strings(options.get("remove", ()), "remove")
        )

    def plan(self, view: StoreView) -> MetadataPlan:
        """Return one patch for the node, after checking that the node exists."""
        path = self._patch.node_path
        try:
            view.group(path)
        except KeyError:
            try:
                view.array(path)
            except KeyError:
                raise ValueError(f"the store has no node at {path or '/'!r}") from None
        return (self._patch,)
