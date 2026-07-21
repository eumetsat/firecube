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

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli


def _write_parquet(path: Path) -> None:
    pq.write_table(pa.table({"a": [1, 2, 3]}), path)


def _invoke_consolidate(args: list[str]):
    return CliRunner().invoke(cli, ["parquet", "consolidate", *args])


def test_file_uri_input_no_storage_flags_and_file_uri_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_path = tmp_path / "input.parquet"
    _write_parquet(input_path)
    monkeypatch.chdir(tmp_path)

    output_path = tmp_path / "out.parquet"
    result = _invoke_consolidate(
        ["-p", input_path.as_uri(), "-o", output_path.as_uri(), "--codec", "snappy"]
    )

    assert result.exit_code == 0, result.output
    assert "Missing option" not in result.output
    assert "Traceback" not in result.output

    assert output_path.exists()
    payload = json.loads(result.output[result.output.index("{") :])
    assert payload["status"] == "success"
    assert payload["output"] == str(output_path.resolve())
