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

import pytest

from firecube.core.config import build_storage_config, load_config_file


def _write_config(tmp_path: Path, content: str) -> Path:
    config_path = tmp_path / "test-config.toml"
    config_path.write_text(content, encoding="utf-8")
    return config_path


def _load_storage_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    content: str,
    *,
    env: dict[str, str] | None = None,
    overrides: dict[str, Any] | None = None,
):
    config_path = _write_config(tmp_path, content)
    monkeypatch.setenv("FIRECUBE_CONFIG", str(config_path))
    cfg = load_config_file()
    return build_storage_config(cfg, env or {}, overrides or {})


def test_config_file_anonymous_true_sets_storage_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _load_storage_config(
        tmp_path,
        monkeypatch,
        '[storage]\ntype = "s3"\nanonymous = true\n',
    )

    assert storage.anonymous is True


@pytest.mark.parametrize(
    ("content", "env", "overrides", "expected"),
    [
        ('[storage]\ntype = "s3"\n', {}, {}, False),
        ('[storage]\ntype = "s3"\nanonymous = true\n', {}, {}, True),
        (
            '[storage]\ntype = "s3"\nanonymous = true\n',
            {"FIRECUBE_S3_ANONYMOUS": "false"},
            {},
            False,
        ),
        (
            '[storage]\ntype = "s3"\nanonymous = false\n',
            {"FIRECUBE_S3_ANONYMOUS": "false"},
            {"anonymous": True},
            True,
        ),
    ],
)
def test_anonymous_precedence_cli_env_config_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    content: str,
    env: dict[str, str],
    overrides: dict[str, Any],
    expected: bool,
) -> None:
    storage = _load_storage_config(
        tmp_path,
        monkeypatch,
        content,
        env=env,
        overrides=overrides,
    )

    assert storage.anonymous is expected
