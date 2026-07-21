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
from unittest.mock import MagicMock, patch

import pytest

from firecube.core.zarr.layers import DEFAULT_MULTIRES_RESOLUTIONS, build_multires_layers
from firecube.ingestor.extensions.duck import DuckDbMixin


@pytest.mark.unit
def test_invalid_resolutions_string_raises_value_error() -> None:
    session = MagicMock()
    with pytest.raises(ValueError, match="Invalid resolutions"):
        build_multires_layers(
            "file:///tmp/fake.zarr",
            session=session,
            resolutions="not_a_number",  # pyright: ignore[reportArgumentType]
        )


@pytest.mark.unit
def test_empty_resolutions_string_raises_value_error() -> None:
    session = MagicMock()
    with pytest.raises(ValueError, match="Invalid resolutions"):
        build_multires_layers(
            "file:///tmp/fake.zarr",
            session=session,
            resolutions=",,,",  # pyright: ignore[reportArgumentType]
        )


@pytest.mark.unit
def test_no_fwi_fallback_on_root_open_failure() -> None:
    session = MagicMock()
    session.zarr.open_dataset.side_effect = Exception("cannot open root")

    with pytest.raises(ValueError, match="Cannot open root dataset"):
        build_multires_layers(
            "file:///tmp/fake.zarr",
            session=session,
            group=None,
            strict=True,
        )

    for call in session.zarr.open_dataset.call_args_list:
        _, kwargs = call
        assert kwargs.get("group") != "FWI", "FWI fallback must not be attempted"


@pytest.mark.unit
def test_default_multires_resolutions_constant_is_single_source() -> None:
    assert DEFAULT_MULTIRES_RESOLUTIONS == (1.0, 0.5)


class _ProductMixin(DuckDbMixin):
    PRODUCT_NAME = "my_product"

    def __init__(self) -> None:
        super().__init__()


class _NoProductMixin(DuckDbMixin):
    def __init__(self) -> None:
        super().__init__()


@pytest.mark.unit
def test_duckdb_filename_derived_from_product_name(tmp_path: Path) -> None:
    ingestor = _ProductMixin()
    with patch("firecube.ingestor.utils.duckdb_utils.open_duckdb") as mock_open:
        mock_open.return_value = MagicMock()
        ingestor.setup_duckdb(workspace=tmp_path, in_memory=False)
        call_kwargs = mock_open.call_args[1]
        db_path: Path = call_kwargs["db_path"]
        assert db_path.name == "my_product.duckdb"


@pytest.mark.unit
def test_duckdb_filename_fallback_when_no_product_name(tmp_path: Path) -> None:
    ingestor = _NoProductMixin()
    with patch("firecube.ingestor.utils.duckdb_utils.open_duckdb") as mock_open:
        mock_open.return_value = MagicMock()
        ingestor.setup_duckdb(workspace=tmp_path, in_memory=False)
        call_kwargs = mock_open.call_args[1]
        db_path: Path = call_kwargs["db_path"]
        assert db_path.name == "product.duckdb"
        assert "fire_risk" not in db_path.name


@pytest.mark.unit
def test_duckdb_filename_option_overrides_product_name(tmp_path: Path) -> None:
    ingestor = _ProductMixin()
    with patch("firecube.ingestor.utils.duckdb_utils.open_duckdb") as mock_open:
        mock_open.return_value = MagicMock()
        ingestor.setup_duckdb(
            workspace=tmp_path,
            in_memory=False,
            options={"duckdb_filename": "custom.duckdb"},
        )
        call_kwargs = mock_open.call_args[1]
        db_path: Path = call_kwargs["db_path"]
        assert db_path.name == "custom.duckdb"
