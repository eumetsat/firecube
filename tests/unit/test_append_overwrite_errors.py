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

from pathlib import Path

import click
import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli._exit_codes import ExitCode
from firecube.cli._formatter import FirecubeGroup
from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.core.zarr.time_decode import decode_time_array
from firecube.ingestor.errors import (
    AppendOverwriteRefused,
    DuplicateExistingTimestampsError,
    IngestorError,
    InsertRefusedError,
    ResumeConflictError,
    SchemaDriftReingestError,
)
from firecube.ingestor.runtime.zarr.append_order import AppendOrder


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

    @click.group(cls=FirecubeGroup)
    def root() -> None:
        pass

    @root.command()
    def failing() -> None:
        raise exc

    result = CliRunner().invoke(root, ["failing"])

    assert result.exit_code == ExitCode.CHECK_FAILED
    assert str(exc) in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize("incoming_day", [2, 1], ids=["equal-to-maximum", "before-maximum"])
def test_append_order_refuses_calendar_value_at_or_before_stored_maximum(
    tmp_path: Path, incoming_day: int
) -> None:
    attrs = {"units": "seconds since 2049-01-01", "calendar": "360_day"}
    store_dir = tmp_path / "store.zarr"
    group = zarr.open_group(store=str(store_dir), mode="w", zarr_format=3).create_group("grp")
    stored = group.create_array("time", shape=(3,), dtype="int64", chunks=(3,))
    stored[:] = np.arange(3) * 86400
    stored.attrs.update(attrs)
    handle = ZarrStoreHandle(
        store=str(store_dir), storage_options=None, target_uri=f"file://{store_dir}"
    )
    incoming = xr.Dataset(
        coords={"time": decode_time_array(np.array([incoming_day * 86400]), attrs)}
    )
    with pytest.raises(InsertRefusedError) as excinfo:
        AppendOrder().assert_append(
            incoming, group="grp", time_dim="time", write_store=handle, resume_store=None
        )
    assert excinfo.value.reason == "insert"
