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

from firecube.core.config import build_storage_config
from firecube.core.observability import logging as logging_module
from firecube.core.observability.telemetry import pushgateway


def test_build_storage_config_uses_firecube_env_names():
    storage = build_storage_config(
        cfg={},
        env={
            "FIRECUBE_STORAGE_TYPE": "s3",
            "FIRECUBE_BUCKET": "firecube",
            "FIRECUBE_ENDPOINT_URL": "https://example.invalid",
            "FIRECUBE_ACCESS_KEY": "firecube-ak",
            "FIRECUBE_SECRET_KEY": "firecube-sk",
            "FIRECUBE_REGION": "eu-central-1",
        },
        overrides={},
    )

    assert storage.endpoint_url == "https://example.invalid"
    assert storage.access_key == "firecube-ak"
    assert storage.secret_key == "firecube-sk"
    assert storage.region == "eu-central-1"


def test_build_storage_config_ignores_aws_aliases():
    storage = build_storage_config(
        cfg={},
        env={
            "FIRECUBE_STORAGE_TYPE": "s3",
            "FIRECUBE_BUCKET": "firecube",
            "AWS_ENDPOINT_URL": "https://legacy.invalid",
            "AWS_ACCESS_KEY_ID": "aws-ak",
            "AWS_SECRET_ACCESS_KEY": "aws-sk",
            "AWS_REGION": "us-east-1",
        },
        overrides={},
    )

    assert storage.endpoint_url is None
    assert storage.access_key is None
    assert storage.secret_key is None
    assert storage.region is None


def test_resolve_logging_env_prefers_firecube_vars(monkeypatch):
    monkeypatch.setenv("FIRECUBE_LOG_LEVEL", "debug")
    monkeypatch.setenv("FIRECUBE_LOG_FORMAT", "plain")
    monkeypatch.setenv("FIRECUBE_LOG_STRUCTURED_FIELDS", "level,message")
    monkeypatch.setenv("LOG_LEVEL", "warning")
    monkeypatch.setenv("LOG_FORMAT", "json")
    monkeypatch.setenv("LOG_STRUCTURED_FIELDS", "name,level")

    log_format, log_level, fields, firecube_debug = logging_module.resolve_logging_env()

    assert log_format == "plain"
    assert log_level == "DEBUG"
    assert fields == ["level", "message"]
    assert firecube_debug is False


def test_resolve_logging_env_ignores_legacy_vars(monkeypatch):
    monkeypatch.delenv("FIRECUBE_LOG_LEVEL", raising=False)
    monkeypatch.delenv("FIRECUBE_LOG_FORMAT", raising=False)
    monkeypatch.delenv("FIRECUBE_LOG_STRUCTURED_FIELDS", raising=False)
    monkeypatch.setenv("LOG_LEVEL", "warning")
    monkeypatch.setenv("LOG_FORMAT", "plain")
    monkeypatch.setenv("LOG_STRUCTURED_FIELDS", "name,level")

    log_format, log_level, fields, firecube_debug = logging_module.resolve_logging_env()

    assert log_format == "json"
    assert log_level == "INFO"
    assert "asctime" in fields
    assert firecube_debug is False


def test_load_pushgateway_url_uses_firecube_env_only(monkeypatch):
    monkeypatch.setattr(pushgateway, "load_config_file", lambda: {})
    monkeypatch.delenv("FIRECUBE_PUSHGATEWAY_URL", raising=False)
    monkeypatch.setenv("PUSHGATEWAY_URL", "http://legacy:9091")

    url = pushgateway.load_pushgateway_url()

    assert url is None


def test_load_pushgateway_url_uses_firecube_env(monkeypatch):
    monkeypatch.setenv("FIRECUBE_PUSHGATEWAY_URL", "http://pushgateway:9091")
    url = pushgateway.load_pushgateway_url()
    assert url == "http://pushgateway:9091"
