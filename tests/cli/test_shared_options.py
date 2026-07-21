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

from firecube.cli._shared_options import (
    storage_driver_option,
    storage_type_option,
    write_mode_option,
)


def _option(command: click.Command, name: str) -> click.Option:
    return next(
        param for param in command.params if isinstance(param, click.Option) and param.name == name
    )


def test_storage_type_option_accepts_required_false() -> None:
    @click.command()
    @storage_type_option(required=False)
    def cmd(storage_type: str | None) -> None:
        del storage_type

    option = _option(cmd, "storage_type")
    assert option.required is False


def test_storage_type_option_direct_decorator_keeps_required_behavior() -> None:
    @click.command()
    @storage_type_option
    def cmd(storage_type: str) -> None:
        del storage_type

    runner = CliRunner()
    result = runner.invoke(cmd, [])

    assert result.exit_code != 0
    assert "Missing option '--storage-type'" in result.output


def test_storage_driver_option_accepts_required_false() -> None:
    @click.command()
    @storage_driver_option(required=False)
    def cmd(storage_driver: str | None) -> None:
        del storage_driver

    option = _option(cmd, "storage_driver")
    assert option.required is False


def test_write_mode_option_is_case_insensitive() -> None:
    @click.command()
    @write_mode_option()
    def cmd(write_mode: str) -> None:
        del write_mode

    option = _option(cmd, "write_mode")
    assert isinstance(option.type, click.Choice)
    assert tuple(option.type.choices) == ("staged", "direct")
    assert option.type.case_sensitive is False
