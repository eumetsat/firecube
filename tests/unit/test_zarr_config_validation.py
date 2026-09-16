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

"""Split zarr config validators: template-tier vs writer-dict."""

from __future__ import annotations

import pytest

from firecube.core.errors import ConfigurationError
from firecube.ingestor.templates.config import (
    validate_zarr_template_config,
    validate_zarr_writer_dict,
)

pytestmark = pytest.mark.unit


class TestTemplateConfigValidator:
    def test_accepts_prefixed_keys(self) -> None:
        validate_zarr_template_config(
            {
                "zarr_compression": True,
                "zarr_sharding": False,
                "zarr_chunk_shape": {"time": 5},
            }
        )

    def test_rejects_bare_compression(self) -> None:
        with pytest.raises(ConfigurationError, match="writer-dict key"):
            validate_zarr_template_config({"compression": True})

    def test_rejects_bare_sharding(self) -> None:
        with pytest.raises(ConfigurationError, match="writer-dict key"):
            validate_zarr_template_config({"sharding": True})

    def test_rejects_bare_chunk_shape(self) -> None:
        with pytest.raises(ConfigurationError, match="writer-dict key"):
            validate_zarr_template_config({"chunk_shape": {"time": 5}})

    def test_bad_compression_type_still_caught(self) -> None:
        with pytest.raises(ConfigurationError, match="zarr_compression must be bool"):
            validate_zarr_template_config({"zarr_compression": "yes"})


class TestWriterDictValidator:
    def test_accepts_bare_keys(self) -> None:
        validate_zarr_writer_dict(
            {
                "compression": True,
                "sharding": False,
                "chunk_shape": {"time": 5},
            }
        )

    def test_rejects_zarr_prefixed_compression(self) -> None:
        with pytest.raises(ConfigurationError, match="template key"):
            validate_zarr_writer_dict({"zarr_compression": True})

    def test_rejects_zarr_prefixed_sharding(self) -> None:
        with pytest.raises(ConfigurationError, match="template key"):
            validate_zarr_writer_dict({"zarr_sharding": True})

    def test_bare_compression_is_valid(self) -> None:
        validate_zarr_writer_dict({"compression": True})

    def test_zarr_codecs_keeps_prefix_in_writer_dict(self) -> None:
        validate_zarr_writer_dict({"compression": True, "zarr_codecs": None})
