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
import os

import click
import pytest
from click.testing import CliRunner

from firecube.cli._shared_options import storage_driver_option, storage_type_option
from firecube.core.config import build_storage_config

pytestmark = pytest.mark.unit


@click.command()
@storage_type_option(required=False)
@storage_driver_option(required=False)
def _storage_probe(
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
) -> None:
    storage_config = build_storage_config(
        {"storage": {"type": "s3"}},
        os.environ,
        {
            "storage_type": storage_type,
            "storage_driver": storage_driver,
            "anonymous": storage_anonymous,
        },
    )
    click.echo(json.dumps({"anonymous": storage_config.anonymous}))


def test_anonymous_env_sets_storage_config() -> None:
    storage_config = build_storage_config(
        {"storage": {"type": "s3"}},
        {"FIRECUBE_S3_ANONYMOUS": "true"},
        {},
    )

    assert storage_config.anonymous is True


def test_anonymous_cli_sets_storage_config() -> None:
    result = CliRunner().invoke(_storage_probe, ["--storage-anonymous"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {"anonymous": True}


def test_anonymous_env_wins_when_cli_flag_absent() -> None:
    result = CliRunner().invoke(
        _storage_probe,
        [],
        env={"FIRECUBE_S3_ANONYMOUS": "true"},
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {"anonymous": True}


@pytest.mark.parametrize("value", ["true", "1", "yes", "TRUE", "YES"])
def test_anonymous_parse_true_variants(value: str) -> None:
    storage_config = build_storage_config(
        {"storage": {"type": "s3"}},
        {"FIRECUBE_S3_ANONYMOUS": value},
        {},
    )

    assert storage_config.anonymous is True


@pytest.mark.parametrize("value", ["false", "0", "no", "", "garbage"])
def test_anonymous_parse_false_variants(value: str) -> None:
    storage_config = build_storage_config(
        {"storage": {"type": "s3"}},
        {"FIRECUBE_S3_ANONYMOUS": value},
        {},
    )

    assert storage_config.anonymous is False


def test_anonymous_parse_unset_defaults_false() -> None:
    storage_config = build_storage_config({"storage": {"type": "s3"}}, {}, {})

    assert storage_config.anonymous is False
