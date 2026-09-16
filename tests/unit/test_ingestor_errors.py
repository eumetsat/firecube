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

"""Ingestor error hierarchy and CLI dispatch invariants.

These tests pin the invariant that the CLI-boundary classifier
(``firecube.cli._errors._is_known_user_error``) recognises every
``FirecubeError`` subclass through an ``isinstance`` check rather than a
name-based whitelist, and that non-``FirecubeError`` user-facing
exceptions still gain explicit membership in
``_KNOWN_NON_FIRECUBE_USER_ERRORS``.

If a new ingestor exception is introduced, it must either inherit from
``FirecubeError`` (preferred — automatic isinstance coverage) or be added
to ``_KNOWN_NON_FIRECUBE_USER_ERRORS`` (only if inheriting from
``FirecubeError`` is genuinely impossible, e.g. a third-party base).
"""

import pytest

from firecube.cli._errors import _KNOWN_NON_FIRECUBE_USER_ERRORS, _is_known_user_error
from firecube.core.errors import ConfigurationError, FirecubeError, StorageError
from firecube.ingestor.errors import IngestorError, SchemaDriftReingestError

pytestmark = pytest.mark.unit


def test_schema_drift_reingest_is_ingestor_error() -> None:
    assert issubclass(SchemaDriftReingestError, IngestorError)


def test_schema_drift_reingest_is_firecube_error_transitively() -> None:
    assert issubclass(SchemaDriftReingestError, FirecubeError)


def test_dispatch_schema_drift_reingest_via_firecube_branch() -> None:
    exc = SchemaDriftReingestError(
        store_uri="file:///tmp/x.zarr",
        dataset_variable="v",
        reason="extra_incoming_variable",
    )
    assert _is_known_user_error(exc)
    assert type(exc).__name__ not in _KNOWN_NON_FIRECUBE_USER_ERRORS


def test_dispatch_configuration_error_via_firecube_branch() -> None:
    exc = ConfigurationError("bad config")
    assert _is_known_user_error(exc)
    assert type(exc).__name__ not in _KNOWN_NON_FIRECUBE_USER_ERRORS


def test_dispatch_storage_error_via_firecube_branch() -> None:
    exc = StorageError("s3 unreachable")
    assert _is_known_user_error(exc)
    assert type(exc).__name__ not in _KNOWN_NON_FIRECUBE_USER_ERRORS


def test_dispatch_unknown_error_returns_false() -> None:
    assert not _is_known_user_error(ValueError("some internal invariant"))


def test_dispatch_file_not_found_via_explicit_membership() -> None:
    assert _is_known_user_error(FileNotFoundError("missing input"))


def test_known_non_firecube_set_never_lists_firecube_subclasses() -> None:
    import builtins

    for name in _KNOWN_NON_FIRECUBE_USER_ERRORS:
        cls = getattr(builtins, name, None)
        if not isinstance(cls, type):
            continue
        assert not issubclass(cls, FirecubeError), (
            f"{name!r} is a FirecubeError subclass and must be removed from "
            f"_KNOWN_NON_FIRECUBE_USER_ERRORS (isinstance dispatch covers it)"
        )
