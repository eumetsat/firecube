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

"""Refusals of source discovery.

Each is a `FirecubeError` and a `ValueError`, so callers that caught the `ValueError` of
`discover_input_files` keep working. The text is a `Message`; the CLI renders its
parameters as flags.
"""

from __future__ import annotations

from collections.abc import Sequence

from firecube.core.errors import FirecubeError, Message, Param

STAC_SEARCH_KEYS: tuple[str, ...] = ("datetime", "bbox", "intersects", "ids")


class DiscoveryError(FirecubeError, ValueError):
    """Base class of the source discovery refusals."""


class UnknownSourceSchemeError(DiscoveryError):
    """A source names a scheme no discovery backend serves.

    Attributes:
        source: The source as given.
        scheme: Its scheme.
    """

    def __init__(self, source: str, scheme: str) -> None:
        self.source = source
        self.scheme = scheme
        super().__init__(
            Message(
                "Source {source!r} has the scheme {scheme!r}, which source discovery does "
                "not read. {input_data} accepts a local path, a file:// or s3:// prefix, "
                "or a stac:// collection.",
                source=source,
                scheme=scheme,
                input_data=Param("input_data"),
            )
        )


class StacSearchError(DiscoveryError):
    """A STAC search filter is not a JSON object of the allowed keys.

    Attributes:
        keys: The refused keys, sorted; empty when the filter is not an object.
    """

    def __init__(self, keys: Sequence[str] = ()) -> None:
        self.keys = tuple(sorted(keys))
        allowed = ", ".join(STAC_SEARCH_KEYS)
        if not self.keys:
            message = Message(
                "{stac_search} must be a JSON object with any of the keys {allowed}.",
                stac_search=Param("stac_search"),
                allowed=allowed,
            )
        else:
            message = Message(
                "{stac_search} accepts only the keys {allowed}; got {refused}. "
                "The collection always comes from the stac:// source.",
                stac_search=Param("stac_search"),
                allowed=allowed,
                refused=", ".join(self.keys),
            )
        super().__init__(message)


class StacApiError(DiscoveryError):
    """A STAC API could not be opened or searched, or a result page could not be read.

    Attributes:
        source: The ``stac://`` source being searched.
        reason: What the client reported.
    """

    def __init__(self, source: str, reason: str) -> None:
        self.source = source
        self.reason = reason
        super().__init__(
            Message("Cannot search STAC source {source}: {reason}", source=source, reason=reason)
        )


class StacAssetHrefError(DiscoveryError):
    """A STAC asset href is relative and its item has no link to resolve it against.

    Attributes:
        source: The ``stac://`` source being searched.
        item_id: The STAC item.
        asset_key: The asset of that item.
    """

    def __init__(self, source: str, item_id: str, asset_key: str) -> None:
        self.source = source
        self.item_id = item_id
        self.asset_key = asset_key
        super().__init__(
            Message(
                "Cannot resolve the relative href of asset {asset_key!r} in STAC item "
                "{item_id!r} from {source}: the item has no self link.",
                source=source,
                item_id=item_id,
                asset_key=asset_key,
            )
        )
