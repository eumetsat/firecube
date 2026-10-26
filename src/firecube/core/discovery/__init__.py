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

"""Source discovery: resolve the sources of a run into source items.

`discover` is the one door. The private modules behind it are reached only through
this package.
"""

from __future__ import annotations

from firecube.core.discovery._filters import (
    LEGACY_INPUT_PATTERNS_MESSAGE,
    reject_legacy_input_patterns,
    split_input_filters,
    validate_input_filters,
    validate_stac_search,
)
from firecube.core.discovery._registry import backend_for
from firecube.core.discovery._stac_compat import require_stac
from firecube.core.discovery._types import (
    STAC_SCHEME,
    DiscoveryRequest,
    SourceItem,
    StacCollectionUri,
)
from firecube.core.discovery._walk import (
    DEFAULT_INCLUDE_SUFFIXES,
    KNOWN_EXTENSIONS,
    discover_input_files,
)
from firecube.core.discovery.errors import (
    STAC_SEARCH_KEYS,
    DiscoveryError,
    StacApiError,
    StacAssetHrefError,
    StacSearchError,
    UnknownSourceSchemeError,
)
from firecube.core.uris import href_basename


def discover(request: DiscoveryRequest) -> list[SourceItem]:
    """Discover the source items of every source of a run.

    Sources are read in the order given, each by the backend of its scheme:

    - A local path, ``file://`` URI or ``s3://`` prefix is walked as a file tree with
      the default selection of `discover_input_files`; ``input_filters`` add matches
      and ``!`` entries exclude.
    - A ``stac://<host>/<api-root>/collections/<id>`` source is searched at
      ``https://<host>/<api-root>`` with ``collections=[<id>]`` and ``stac_search``.
      Every result page is read, and each asset whose roles contain ``data`` becomes an
      item. The default suffix list does not apply; ``input_filters`` narrow on the
      basename of the href.

    Items are deduplicated on `SourceItem.identity` (the first occurrence wins), then
    sorted once by href basename and identity, so the result does not depend on the
    order of the sources or of the API's pages.

    Args:
        request: The sources and how to narrow them.

    Returns:
        The items, deduplicated and sorted. Empty when nothing matches.

    Raises:
        UnknownSourceSchemeError: If a source names a scheme no backend reads.
        ImportError: If a source is ``stac://`` and the ``stac`` extra is not installed.
        StacApiError: If a STAC API cannot be opened or searched.
        ValueError: If a file-tree source cannot be opened or listed, or a filter is
            invalid.

    Examples:
        The data assets of one collection in June 2025, plus a local directory:

            items = discover(
                DiscoveryRequest(
                    sources=(
                        "stac://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a",
                        "/data/raw",
                    ),
                    stac_search={"datetime": "2025-06-01T00:00:00Z/2025-06-30T23:59:59Z"},
                )
            )
            hrefs = [item.href for item in items]
    """
    found: dict[str, SourceItem] = {}
    for source in request.sources:
        for item in backend_for(source)(source, request):
            found.setdefault(item.identity, item)
    return sorted(found.values(), key=lambda item: (href_basename(item.href), item.identity))


__all__ = [
    "DEFAULT_INCLUDE_SUFFIXES",
    "KNOWN_EXTENSIONS",
    "LEGACY_INPUT_PATTERNS_MESSAGE",
    "STAC_SCHEME",
    "STAC_SEARCH_KEYS",
    "DiscoveryError",
    "DiscoveryRequest",
    "SourceItem",
    "StacApiError",
    "StacAssetHrefError",
    "StacCollectionUri",
    "StacSearchError",
    "UnknownSourceSchemeError",
    "discover",
    "discover_input_files",
    "reject_legacy_input_patterns",
    "require_stac",
    "split_input_filters",
    "validate_input_filters",
    "validate_stac_search",
]
