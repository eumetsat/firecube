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

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
import pytest

from firecube.cli._command_schemas import (  # pyright: ignore[reportMissingImports]
    IngestCommandConfig,
)
from tests.helpers.rows import table_row


def _config(**overrides: object) -> IngestCommandConfig:
    values = {
        "plugin": "cli_test_plugin",
        "input_data": Path("input"),
        "target": "file:///tmp/out.zarr",
        "write_mode": "staged",
        "storage_type": "local",
        "storage_driver": "fsspec",
    }
    values.update(overrides)
    return IngestCommandConfig(**values)  # type: ignore[arg-type]


_FILE_TARGET = "file:///tmp/out.zarr"
_S3_TARGET = "s3://bucket/out.zarr"
_GS_UNSUPPORTED = "URI scheme 'gs' not supported."
_FILE_WITH_S3 = "--storage-type 's3' is incompatible with URI scheme 'file'"

# (row id, overrides of _config, expected attribute values)
_ACCEPTED: tuple[tuple[str, dict[str, object], dict[str, object]], ...] = (
    (
        "test_valid_config_constructs",
        {},
        {"plugin": "cli_test_plugin", "options": {}},
    ),
    (
        "test_missing_storage_type_is_valid",
        {"storage_type": None, "storage_driver": None},
        {"storage_type": None, "storage_driver": None},
    ),
    (
        "test_missing_storage_driver_is_valid",
        {"storage_driver": None},
        {"storage_type": "local", "storage_driver": None},
    ),
    ("test_none_product_name_accepted", {"product_name": None}, {"product_name": None}),
    (
        "test_storage_type_local_with_file_target_constructs",
        {"target": _FILE_TARGET, "storage_type": "local"},
        {"target": _FILE_TARGET, "storage_type": "local"},
    ),
    (
        "test_storage_type_s3_with_s3_target_constructs",
        {"target": _S3_TARGET, "storage_type": "s3"},
        {"target": _S3_TARGET, "storage_type": "s3"},
    ),
)


def test_accepted_configs_construct(subtests: Any, tmp_path: Path) -> None:
    for row_id, overrides, expected in _ACCEPTED:
        with table_row(subtests, row_id, tmp_path):
            config = _config(**overrides)

            for attr, value in expected.items():
                assert getattr(config, attr) == value, attr


# (row id, overrides of _config, substring of the UsageError message)
_REJECTED: tuple[tuple[str, dict[str, object], str], ...] = (
    ("test_missing_write_mode_raises", {"write_mode": None}, "--write-mode"),
    ("test_explicit_empty_product_name_raises", {"product_name": ""}, "--product-name"),
    ("test_invalid_write_mode_raises", {"write_mode": "invalid"}, "--write-mode must be one of"),
    (
        "test_storage_type_s3_with_file_target_raises",
        {"target": _FILE_TARGET, "storage_type": "s3"},
        _FILE_WITH_S3,
    ),
    (
        "test_storage_type_local_with_s3_target_raises",
        {"target": _S3_TARGET, "storage_type": "local"},
        "--storage-type 'local' is incompatible with URI scheme 's3'",
    ),
    (
        "test_unsupported_scheme_gs_with_local_storage_type_raises",
        {"target": "gs://bucket/out.zarr", "storage_type": "local"},
        _GS_UNSUPPORTED,
    ),
    (
        "test_unsupported_scheme_gs_with_s3_storage_type_raises",
        {"target": "gs://bucket/out.zarr", "storage_type": "s3"},
        _GS_UNSUPPORTED,
    ),
    (
        "test_aggregation_missing_driver_and_target_storage_type_mismatch",
        {"target": _FILE_TARGET, "storage_type": "s3", "storage_driver": None},
        _FILE_WITH_S3,
    ),
    (
        "test_uppercase_s3_scheme_raises",
        {"target": "S3://bucket/out.zarr", "storage_type": "s3"},
        "URI scheme must be lowercase",
    ),
    ("test_bare_target_raises", {"target": "/tmp/out.zarr"}, "URI scheme required"),
    (
        "test_localhost_target_raises",
        {"target": "file://localhost/tmp/out.zarr"},
        "must not name a host",
    ),
)


def test_rejected_configs_raise_usage_error(subtests: Any, tmp_path: Path) -> None:
    for row_id, overrides, message in _REJECTED:
        with table_row(subtests, row_id, tmp_path):
            with pytest.raises(click.UsageError) as exc_info:
                _config(**overrides)

            assert message in str(exc_info.value)


def test_storage_type_not_mutated_by_validation() -> None:
    config = _config(target="file:///tmp/out.zarr", storage_type="local")
    config.storage_type = "s3"

    with pytest.raises(click.UsageError):
        config.__post_init__()

    assert config.target == "file:///tmp/out.zarr"
    assert config.storage_type == "s3"
