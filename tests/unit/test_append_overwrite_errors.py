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

import click
import pytest

from firecube.cli._errors import wrap_user_facing_errors
from firecube.ingestor.errors import (
    AppendOverwriteRefused,
    DuplicateExistingTimestampsError,
    IngestorError,
    InsertRefusedError,
    ResumeConflictError,
    SchemaDriftReingestError,
)


def test_error_hierarchy() -> None:
    assert issubclass(InsertRefusedError, AppendOverwriteRefused)
    assert issubclass(DuplicateExistingTimestampsError, AppendOverwriteRefused)
    assert issubclass(AppendOverwriteRefused, ResumeConflictError)


def test_error_message_contains_reason() -> None:
    e = InsertRefusedError(refused_timestamps=["2024-01-15"], reason="insert")
    assert "insert" in str(e)
    assert "2024-01-15" in str(e)


@pytest.mark.parametrize(
    "exc",
    [
        AppendOverwriteRefused(refused_timestamps=["2024-01-15"], reason="insert"),
        InsertRefusedError(refused_timestamps=["2024-01-15T00:00:00"], reason="insert"),
        DuplicateExistingTimestampsError(
            refused_timestamps=["2024-01-15"], reason="duplicates_existing"
        ),
        ResumeConflictError("existing data conflicts with resume/force-reingest"),
        SchemaDriftReingestError(
            store_uri="file:///tmp/x.zarr",
            dataset_variable="v",
            reason="extra_incoming_variable",
        ),
    ],
    ids=[
        "AppendOverwriteRefused",
        "InsertRefusedError",
        "DuplicateExistingTimestampsError",
        "ResumeConflictError",
        "SchemaDriftReingestError",
    ],
)
def test_public_ingestor_error_subclasses_produce_user_facing_exit(
    exc: IngestorError,
) -> None:
    assert isinstance(exc, IngestorError)

    @wrap_user_facing_errors
    def failing() -> None:
        raise exc

    with pytest.raises(click.ClickException) as excinfo:
        failing()

    assert excinfo.value.__cause__ is exc
    assert str(exc) == excinfo.value.message
