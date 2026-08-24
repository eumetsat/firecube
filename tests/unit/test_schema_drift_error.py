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

from firecube.core import api as core_api
from firecube.core.errors import (
    ConfigurationError,
    FirecubeError,
    MissingIrregularCoordinateError,
    NoDiscoveredItemsError,
    SchemaDriftError,
)
from firecube.ingestor import api as ingestor_api
from firecube.ingestor import errors as ingestor_errors


def test_schema_drift_error_inherits_firecube_error() -> None:
    assert issubclass(SchemaDriftError, FirecubeError)


def test_schema_drift_error_reexported_from_ingestor_errors() -> None:
    assert ingestor_errors.SchemaDriftError is SchemaDriftError


def test_missing_irregular_coordinate_error_contract() -> None:
    error = MissingIrregularCoordinateError("timestamp", 7)
    missing_item = MissingIrregularCoordinateError("time", None)

    assert issubclass(MissingIrregularCoordinateError, ConfigurationError)
    assert isinstance(error, ConfigurationError)
    assert error.coordinate_name == "timestamp"
    assert error.item_id == 7
    assert str(error) == (
        "IrregularTimeAxis discovery: item 7 has no resolvable coordinate for 'timestamp'"
    )
    assert missing_item.coordinate_name == "time"
    assert missing_item.item_id is None
    assert str(missing_item) == (
        "IrregularTimeAxis discovery: item None has no resolvable coordinate for 'time'"
    )


def test_missing_irregular_coordinate_error_reexported_from_facades() -> None:
    assert core_api.MissingIrregularCoordinateError is MissingIrregularCoordinateError
    assert ingestor_api.MissingIrregularCoordinateError is MissingIrregularCoordinateError


def test_no_discovered_items_error_contract() -> None:
    error = NoDiscoveredItemsError("timestamp", "source.nc")
    missing_source = NoDiscoveredItemsError("time", None)

    assert issubclass(NoDiscoveredItemsError, ConfigurationError)
    assert isinstance(error, ConfigurationError)
    assert error.coordinate_name == "timestamp"
    assert error.source_ref == "source.nc"
    assert str(error) == (
        "IrregularTimeAxis discovery for 'timestamp': no items found in source 'source.nc'"
    )
    assert missing_source.coordinate_name == "time"
    assert missing_source.source_ref is None
    assert str(missing_source) == (
        "IrregularTimeAxis discovery for 'time': no items found in source 'None'"
    )


def test_no_discovered_items_error_reexported_from_facades() -> None:
    assert core_api.NoDiscoveredItemsError is NoDiscoveredItemsError
    assert ingestor_api.NoDiscoveredItemsError is NoDiscoveredItemsError
