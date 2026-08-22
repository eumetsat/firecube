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

"""Contract guard for the public ``firecube.core.api`` / ``firecube.ingestor.api`` surface.

Removed (must NOT be re-exported):

* ``firecube.core.api``: ``open_fsspec_url`` → ``firecube.core.filesystem.ops._open_fsspec_url``
  (adapter boundary), ``fs_kwargs_for_uri`` → ``firecube.core.filesystem``,
  ``RegionZarrWriter`` → ``firecube.core.zarr.region_writer``.
* ``firecube.ingestor.api``: ``CoverageTracker`` → ``firecube.ingestor.runtime.coverage``,
  ``ScratchManager`` → module deleted (was at ``firecube.ingestor.runtime.scratch``; see DONE.md for this plan's rationale).
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.contract

_REMOVED_CORE_API: tuple[str, ...] = (
    "open_fsspec_url",
    "fs_kwargs_for_uri",
    "RegionZarrWriter",
)

_REMOVED_INGESTOR_API: tuple[str, ...] = (
    "CoverageTracker",
    "ScratchManager",
)

_KEPT_CORE_API: tuple[str, ...] = (
    "IntegerAxis",
    "RegionZarrWriterProtocol",
    "ResolvedIndexRecord",
    "SlotAxis",
    "SlotIndexModel",
    "decode_time_array",
    "epoch_s_to_iso",
    "iso_to_epoch_s",
    "normalize_epoch_iso",
    "read_chunk_grid_with_shards",
)

_KEPT_INGESTOR_API: tuple[str, ...] = (
    "IntegerAxis",
    "ResolvedIndexRecord",
    "RuntimeIngestContext",
    "verify_dim_compatibility",
)


@pytest.mark.parametrize("name", _REMOVED_CORE_API)
def test_removed_core_api_name_raises_import_error(name: str) -> None:
    import firecube.core.api as core_api

    assert not hasattr(core_api, name), (
        f"`firecube.core.api.{name}` must not be exposed; the alias was removed."
    )
    assert name not in core_api.__all__, f"`firecube.core.api.__all__` must not list {name!r}."
    with pytest.raises(ImportError):
        exec(f"from firecube.core.api import {name}")


@pytest.mark.parametrize("name", _REMOVED_INGESTOR_API)
def test_removed_ingestor_api_name_raises_import_error(name: str) -> None:
    import firecube.ingestor.api as ingestor_api

    assert not hasattr(ingestor_api, name), (
        f"`firecube.ingestor.api.{name}` must not be exposed; the alias was removed."
    )
    assert name not in ingestor_api.__all__, (
        f"`firecube.ingestor.api.__all__` must not list {name!r}."
    )
    with pytest.raises(ImportError):
        exec(f"from firecube.ingestor.api import {name}")


@pytest.mark.parametrize("name", _KEPT_CORE_API)
def test_kept_core_api_name_is_importable(name: str) -> None:
    import firecube.core.api as core_api

    assert hasattr(core_api, name), (
        f"`firecube.core.api.{name}` must remain importable for downstream consumers."
    )
    assert name in core_api.__all__, f"`firecube.core.api.__all__` must list {name!r}."


@pytest.mark.parametrize("name", _KEPT_INGESTOR_API)
def test_kept_ingestor_api_name_is_importable(name: str) -> None:
    import firecube.ingestor.api as ingestor_api

    assert hasattr(ingestor_api, name), (
        f"`firecube.ingestor.api.{name}` must remain importable for downstream consumers."
    )
    assert name in ingestor_api.__all__, f"`firecube.ingestor.api.__all__` must list {name!r}."
