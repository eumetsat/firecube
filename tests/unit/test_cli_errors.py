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
from click.testing import CliRunner

from firecube.cli._errors import (  # pyright: ignore[reportMissingImports]
    MissingStorageDriverError,
    UnknownOptionError,
    classify,
    render,
)
from firecube.cli._exit_codes import ExitCode
from firecube.cli._recovery import _command
from firecube.core.errors import ConfigurationError, MissingProductNameError
from tests.helpers.rows import table_row


def test_cli_error_messages_and_exit_codes(subtests: Any, tmp_path: Path) -> None:
    with table_row(subtests, "unknown_option_error_message_and_type", tmp_path):
        err = UnknownOptionError("foo", "test_product", ["bar", "baz"])

        assert isinstance(err, click.BadParameter)
        msg = str(err)
        assert "foo" in msg
        assert "test_product" in msg
        assert "bar" in msg
        assert "baz" in msg

    with table_row(subtests, "missing_product_name_error_message_and_type", tmp_path):
        err = MissingProductNameError("test_product")

        assert isinstance(err, ConfigurationError)
        msg = render(err.args[0], click.Context(_command(("ingest",)), info_name="ingest"))
        assert "test_product" in msg
        assert "Option:  --product-name <name>" in msg
        assert "default_product_name" in msg
        assert "PRODUCT_NAME" in msg

    with table_row(subtests, "missing_storage_driver_error_message_and_type", tmp_path):
        err = MissingStorageDriverError("file:///tmp/x")

        assert isinstance(err, click.UsageError)
        msg = str(err)
        assert "file:///tmp/x" in msg
        assert "--storage-driver" in msg

    with table_row(subtests, "unknown_option_error_exit_code_2", tmp_path):
        runner = CliRunner()

        @click.command()
        def cmd() -> None:
            raise UnknownOptionError("foo", "test_product", ["bar"])

        result = runner.invoke(cmd)

        assert result.exit_code == 2

    with table_row(subtests, "missing_product_name_error_exit_code_2", tmp_path):
        assert classify(MissingProductNameError("test_product")) is ExitCode.USAGE
