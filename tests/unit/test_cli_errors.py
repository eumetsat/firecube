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

import click
from click.testing import CliRunner

from firecube.cli._errors import (  # pyright: ignore[reportMissingImports]
    MissingProductNameError,
    MissingStorageDriverError,
    MissingStorageTypeError,
    MissingWriteModeError,
    UnknownOptionError,
)


def test_unknown_option_error_message_and_type() -> None:
    err = UnknownOptionError("foo", "test_product", ["bar", "baz"])

    assert isinstance(err, click.BadParameter)
    msg = str(err)
    assert "foo" in msg
    assert "test_product" in msg
    assert "bar" in msg
    assert "baz" in msg


def test_missing_product_name_error_message_and_type() -> None:
    err = MissingProductNameError("test_product")

    assert isinstance(err, click.UsageError)
    msg = str(err)
    assert "test_product" in msg
    assert "--product-name" in msg
    assert "default_product_name" in msg
    assert "PRODUCT_NAME" in msg


def test_missing_storage_type_error_message_and_type() -> None:
    err = MissingStorageTypeError("s3://bucket/x")

    assert isinstance(err, click.UsageError)
    msg = str(err)
    assert "s3://bucket/x" in msg
    assert "--storage-type" in msg
    assert "local|s3" in msg


def test_missing_storage_driver_error_message_and_type() -> None:
    err = MissingStorageDriverError("file:///tmp/x")

    assert isinstance(err, click.UsageError)
    msg = str(err)
    assert "file:///tmp/x" in msg
    assert "--storage-driver" in msg


def test_missing_write_mode_error_message_and_type() -> None:
    err = MissingWriteModeError()

    assert isinstance(err, click.UsageError)
    msg = str(err)
    assert "--write-mode" in msg
    assert "staged" in msg
    assert "direct" in msg


def test_unknown_option_error_exit_code_2() -> None:
    runner = CliRunner()

    @click.command()
    def cmd() -> None:
        raise UnknownOptionError("foo", "test_product", ["bar"])

    result = runner.invoke(cmd)

    assert result.exit_code == 2


def test_missing_product_name_error_exit_code_2() -> None:
    runner = CliRunner()

    @click.command()
    def cmd() -> None:
        raise MissingProductNameError("test_product")

    result = runner.invoke(cmd)

    assert result.exit_code == 2
