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

"""Tests for SchemaDriftError message templates."""

import pytest

from firecube.ingestor.errors import (
    STATIC_VAR_DRIFT_FORCE_REINGEST_TMPL,
    STATIC_VAR_DRIFT_MSG_TMPL,
    STATIC_VAR_NEW_ON_APPEND_TMPL,
)

pytestmark = pytest.mark.unit


def test_static_var_drift_msg_names_variable() -> None:
    msg = STATIC_VAR_DRIFT_MSG_TMPL.format(
        name="lat_bnds", time_dim="timestamp", store_uri="file:///tmp/x.zarr"
    )
    assert "lat_bnds" in msg
    assert "timestamp" in msg
    assert len(msg) <= 500


def test_force_reingest_template_guides_to_new_store() -> None:
    msg = STATIC_VAR_DRIFT_FORCE_REINGEST_TMPL.format(name="y_bnds", store_uri="file:///tmp/x.zarr")
    assert "y_bnds" in msg
    assert "new store" in msg.lower()


def test_new_var_on_append_template_names_variable() -> None:
    msg = STATIC_VAR_NEW_ON_APPEND_TMPL.format(name="lon_bnds", store_uri="file:///tmp/x.zarr")
    assert "lon_bnds" in msg
    assert "new store" in msg.lower() or "remove" in msg.lower()


def test_all_templates_have_name_format_param() -> None:
    ctx: dict[str, str] = {"name": "x", "time_dim": "t", "store_uri": "s"}
    STATIC_VAR_DRIFT_MSG_TMPL.format_map(ctx)
    STATIC_VAR_DRIFT_FORCE_REINGEST_TMPL.format_map(ctx)
    STATIC_VAR_NEW_ON_APPEND_TMPL.format_map(ctx)
