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

"""Which backend discovers a source: one entry per scheme."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final, Protocol

from firecube.core.discovery._stac import stac_source
from firecube.core.discovery._types import DiscoveryRequest, SourceItem
from firecube.core.discovery._walk import walk_source
from firecube.core.discovery.errors import UnknownSourceSchemeError
from firecube.core.uris import infer_target_protocol


class SourceBackend(Protocol):
    """Discovers the items of one source, deduplicated on identity and unsorted."""

    def __call__(self, source: str, request: DiscoveryRequest) -> list[SourceItem]: ...


BACKENDS: Final[Mapping[str, SourceBackend]] = {
    "file": walk_source,
    "s3": walk_source,
    "stac": stac_source,
}


def backend_for(source: str) -> SourceBackend:
    """Return the backend of ``source``'s scheme; a bare path is a ``file`` source.

    Raises:
        UnknownSourceSchemeError: If no backend serves the scheme.
    """
    scheme = infer_target_protocol(source)
    backend = BACKENDS.get(scheme)
    if backend is None:
        raise UnknownSourceSchemeError(source, scheme)
    return backend
