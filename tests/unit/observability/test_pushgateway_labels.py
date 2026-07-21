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

from firecube.core.observability.telemetry import pushgateway


def test_default_allowed_meta_keys_include_satellite():
    assert "satellite" in pushgateway.default_allowed_meta_keys()


def test_load_allowed_meta_keys_merges_config_and_env(monkeypatch):
    monkeypatch.setattr(
        pushgateway,
        "load_config_file",
        lambda: {"metrics": {"label_allowlist": ["frp_variant", "custom_label"]}},
    )
    monkeypatch.setenv("FIRECUBE_METRICS_LABEL_ALLOWLIST", " env_label , frp_variant ")

    keys = pushgateway.load_allowed_meta_keys()

    assert "satellite" in keys
    assert "frp_variant" in keys
    assert "custom_label" in keys
    assert "env_label" in keys
