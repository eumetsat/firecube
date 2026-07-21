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

"""Public exception contract for staged-metadata failures.

A plugin author who imports ``StagedMetadataError`` from the public
``firecube.ingestor.errors`` module must catch the exception that staged
metadata seeding actually raises. Historically two unrelated classes shared
the name (a never-raised public one and the real ``RuntimeError``-based one
in ``runtime/zarr/staged_metadata.py``), so public handlers silently caught
nothing. These tests lock the unified contract.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from firecube.ingestor.errors import IngestorError, StagedMetadataError

pytestmark = pytest.mark.unit


class _BrokenFilesystem:
    """Collaborator stub: any storage access fails like a real outage would."""

    def exists(self, uri: object) -> bool:
        raise OSError("simulated storage failure")


class _BrokenSession:
    def fs(self) -> _BrokenFilesystem:
        return _BrokenFilesystem()


def _seed_against_broken_storage(tmp_path: Path) -> None:
    from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata

    seed_staged_store_metadata(
        temp_store_uri=str(tmp_path / "temp_store"),
        final_target_uri=str(tmp_path / "final.zarr"),
        groups=["default"],
        strict=True,
        session=_BrokenSession(),  # type: ignore[arg-type]
    )


def test_public_exception_is_the_raised_class() -> None:
    """One class, not two namesakes: the public export IS the raised type."""
    from firecube.ingestor.runtime.zarr.staged_metadata import (
        StagedMetadataError as raised_cls,
    )

    assert StagedMetadataError is raised_cls


def test_seeding_failure_is_catchable_via_public_import(tmp_path: Path) -> None:
    """The exact thing a plugin author does: catch the publicly imported name."""
    with pytest.raises(StagedMetadataError, match="default"):
        _seed_against_broken_storage(tmp_path)


def test_seeding_failure_is_an_ingestor_error(tmp_path: Path) -> None:
    """Broad plugin handlers (`except IngestorError`) must also see it."""
    with pytest.raises(IngestorError):
        _seed_against_broken_storage(tmp_path)
