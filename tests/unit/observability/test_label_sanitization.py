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

from firecube.core.observability.telemetry import (
    normalize_name_for_kind,
    sanitize_label_key,
    sanitize_label_value,
    sanitize_metric_name,
)


def test_sanitize_metric_name_and_counter_suffix():
    assert sanitize_metric_name("metric-x") == "firecube_metric_x"
    assert normalize_name_for_kind("firecube_rows", "counter") == "firecube_rows_total"


def test_sanitize_label_key_and_value():
    assert sanitize_label_key("test-region") == "test_region"
    assert sanitize_label_value({"x": 1}) == ""
    assert sanitize_label_value(["a", "b", "c", "d", "e", "f"]) == ""
