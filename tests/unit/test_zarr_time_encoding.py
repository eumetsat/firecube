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

import pytest

from firecube.core.errors import ConfigurationError
from firecube.ingestor.api import IngestContext, ZarrTemplateConfig
from firecube.ingestor.runtime.configure import TierConfigurator
from firecube.ingestor.templates.config import (
    validate_zarr_template_config,
    validate_zarr_writer_dict,
)


def test_time_encoding_is_rejected_when_binding_template_options():
    configurator = TierConfigurator(ZarrTemplateConfig, None, plugin_name="measurement")
    ctx = IngestContext(source="input", options={"zarr_time_encoding": "int64"})
    with pytest.raises(ConfigurationError, match="zarr_time_encoding is not implemented"):
        configurator.configure(ctx)


@pytest.mark.parametrize("key", ["zarr_time_encoding", "time_encoding"])
def test_time_encoding_is_rejected_in_dynamic_writer_config(key):
    with pytest.raises(ConfigurationError, match=rf"^{key} is not implemented"):
        validate_zarr_writer_dict({key: "int64"})


@pytest.mark.parametrize("value", [None, ""])
def test_unset_time_encoding_keeps_default_configuration_valid(value):
    cfg = ZarrTemplateConfig(zarr_time_encoding=value)
    validate_zarr_writer_dict({"time_encoding": cfg.zarr_time_encoding})


def test_empty_typed_option_does_not_hide_nonempty_writer_option():
    with pytest.raises(ConfigurationError, match=r"^time_encoding is not implemented"):
        validate_zarr_writer_dict({"zarr_time_encoding": None, "time_encoding": "int64"})


def test_template_config_rejects_nonempty_time_encoding():
    with pytest.raises(ConfigurationError, match=r"^zarr_time_encoding is not implemented"):
        validate_zarr_template_config({"zarr_time_encoding": "int64"})
