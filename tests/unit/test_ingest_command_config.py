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

import click
import pytest

from firecube.cli._command_schemas import (  # pyright: ignore[reportMissingImports]
    IngestCommandConfig,
)


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


def test_valid_config_constructs() -> None:
    config = _config()

    assert config.plugin == "cli_test_plugin"
    assert config.output_format == "zarr"
    assert config.options == {}


def test_missing_write_mode_raises() -> None:
    with pytest.raises(click.UsageError, match="--write-mode"):
        _config(write_mode=None)


def test_missing_storage_type_is_valid() -> None:
    config = _config(storage_type=None, storage_driver=None)

    assert config.storage_type is None
    assert config.storage_driver is None


def test_missing_storage_driver_is_valid() -> None:
    config = _config(storage_driver=None)

    assert config.storage_type == "local"
    assert config.storage_driver is None


def test_explicit_empty_product_name_raises() -> None:
    with pytest.raises(click.UsageError, match="--product-name"):
        _config(product_name="")


def test_none_product_name_accepted() -> None:
    assert _config(product_name=None).product_name is None


def test_invalid_write_mode_raises() -> None:
    with pytest.raises(click.UsageError, match="--write-mode must be one of"):
        _config(write_mode="invalid")


def test_storage_type_local_with_file_target_constructs() -> None:
    config = _config(target="file:///tmp/out.zarr", storage_type="local")

    assert config.target == "file:///tmp/out.zarr"
    assert config.storage_type == "local"


def test_storage_type_s3_with_s3_target_constructs() -> None:
    config = _config(target="s3://bucket/out.zarr", storage_type="s3")

    assert config.target == "s3://bucket/out.zarr"
    assert config.storage_type == "s3"


def test_storage_type_s3_with_file_target_raises() -> None:
    with pytest.raises(click.UsageError) as exc_info:
        _config(target="file:///tmp/out.zarr", storage_type="s3")

    assert (
        "Target URI scheme 'file' is incompatible with --storage-type 's3'. "
        "Use --storage-type local for file:// targets, or change --target to an "
        "s3://-compatible URI."
    ) in str(exc_info.value)


def test_storage_type_local_with_s3_target_raises() -> None:
    with pytest.raises(click.UsageError) as exc_info:
        _config(target="s3://bucket/out.zarr", storage_type="local")

    assert (
        "Target URI scheme 's3' is incompatible with --storage-type 'local'. "
        "Use --storage-type s3 for s3:// targets, or change --target to a file:// URI."
    ) in str(exc_info.value)


def test_unsupported_scheme_gs_with_local_storage_type_raises() -> None:
    with pytest.raises(click.UsageError) as exc_info:
        _config(target="gs://bucket/out.zarr", storage_type="local")

    assert (
        "Target URI scheme 'gs' is not supported. Use a file:// URI for "
        "--storage-type local, or an s3:// URI for --storage-type s3."
    ) in str(exc_info.value)


def test_unsupported_scheme_gs_with_s3_storage_type_raises() -> None:
    with pytest.raises(click.UsageError) as exc_info:
        _config(target="gs://bucket/out.zarr", storage_type="s3")

    assert (
        "Target URI scheme 'gs' is not supported. Use a file:// URI for "
        "--storage-type local, or an s3:// URI for --storage-type s3."
    ) in str(exc_info.value)


def test_uppercase_s3_scheme_behavior() -> None:
    config = _config(target="S3://bucket/out.zarr", storage_type="s3")

    assert config.target == "S3://bucket/out.zarr"
    assert config.storage_type == "s3"


def test_aggregation_missing_driver_and_target_storage_type_mismatch() -> None:
    with pytest.raises(click.UsageError) as exc_info:
        _config(target="file:///tmp/out.zarr", storage_type="s3", storage_driver=None)

    message = str(exc_info.value)
    assert (
        "Target URI scheme 'file' is incompatible with --storage-type 's3'. "
        "Use --storage-type local for file:// targets, or change --target to an "
        "s3://-compatible URI."
    ) in message


def test_storage_type_not_mutated_by_validation() -> None:
    config = _config(target="file:///tmp/out.zarr", storage_type="local")
    config.storage_type = "s3"

    with pytest.raises(click.UsageError):
        config.__post_init__()

    assert config.target == "file:///tmp/out.zarr"
    assert config.storage_type == "s3"
