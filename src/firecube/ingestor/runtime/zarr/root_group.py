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

"""Schema checks for a DirectZarr cube written at the store root."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from firecube.core.errors import ConfigurationError, Message
from firecube.core.zarr._reserved_root_attrs import RESERVED_ROOT_ATTRS
from firecube.core.zarr.guard._unit_locks import canonical_array_path


def refuse_root_beside_named_groups(schema: Iterable[Any]) -> None:
    """Refuse a schema that declares the root group beside a named group.

    The root group's metadata authority covers every node of the store, so its
    schema setup could change a named group's documents. Its claims are distinct
    exact names and block no sibling.

    Raises:
        ConfigurationError: The schema declares ``""`` or ``"/"`` and at least one other group.
    """
    groups = {canonical_array_path(spec.group) for spec in schema}
    if "" in groups and len(groups) > 1:
        raise ConfigurationError(
            Message(
                "The root group cannot be declared beside named groups {others}: the root "
                "group's metadata authority covers every node of the store, theirs included.",
                others=sorted(groups - {""}),
            )
        )


def refuse_root_group_attrs(schema: Iterable[Any]) -> None:
    """Refuse root-group attributes that carry the name of a firecube index-mirror attribute.

    The root document holds the index mirror beside the plugin's attributes. Other names are allowed.

    Raises:
        ConfigurationError: The root group (``""`` or ``"/"``) declares a key of `RESERVED_ROOT_ATTRS`.
    """
    for spec in schema:
        if not spec.attrs or canonical_array_path(spec.group):
            continue
        reserved = sorted(RESERVED_ROOT_ATTRS.intersection(spec.attrs))
        if reserved:
            raise ConfigurationError(
                Message(
                    "The root group {group!r} cannot declare the attributes {reserved}: they "
                    "hold firecube's index mirror. Remove them from ZarrGroupSpec.attrs.",
                    group=spec.group,
                    reserved=reserved,
                )
            )
