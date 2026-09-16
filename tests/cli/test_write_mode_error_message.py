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
import pytest

from firecube.cli._command_schemas import IngestCommandConfig

pytestmark = pytest.mark.unit


def _missing_write_mode_error() -> str:
    with pytest.raises(click.UsageError) as exc_info:
        IngestCommandConfig(
            plugin="test_plugin",
            input_data=None,
            target="file:///tmp/out.zarr",
            write_mode=None,
            storage_type=None,
            storage_driver=None,
        )
    return str(exc_info.value)


def test_write_mode_error_mentions_file_uri_guidance() -> None:
    msg = _missing_write_mode_error()
    assert "file://" in msg


def test_write_mode_error_mentions_staged_and_direct() -> None:
    msg = _missing_write_mode_error()
    assert "staged" in msg
    assert "direct" in msg
