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
(``firecube.cli._errors.classify``) recognises every ``FirecubeError``
subclass through an ``isinstance`` check rather than a name-based whitelist,
so an ingestor exception that inherits from ``FirecubeError`` never ends a
command with the internal-error exit code.
"""

import pytest

from firecube.cli._errors import classify
from firecube.cli._exit_codes import ExitCode
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
    assert classify(exc) is ExitCode.CHECK_FAILED


def test_dispatch_configuration_error_via_firecube_branch() -> None:
    assert classify(ConfigurationError("bad config")) is ExitCode.USAGE


def test_dispatch_storage_error_via_firecube_branch() -> None:
    assert classify(StorageError("s3 unreachable")) is ExitCode.RUNTIME_ERROR


def test_dispatch_unknown_error_is_internal() -> None:
    assert classify(ValueError("some internal invariant")) is ExitCode.INTERNAL


def test_dispatch_file_not_found_is_a_runtime_error() -> None:
    assert classify(FileNotFoundError("missing input")) is ExitCode.RUNTIME_ERROR
