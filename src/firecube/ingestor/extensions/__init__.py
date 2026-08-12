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

"""Optional plugin extensions: spatial regridding and DuckDB batch support.

Plugins may import from this package in addition to ``firecube.ingestor.api``
and ``firecube.core.api``. Symbols are imported lazily so that importing the
package does not require the optional dependencies of every extension.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from firecube.ingestor.extensions._binning import aggregate_by_position
    from firecube.ingestor.extensions.duck import DuckDbMixin
    from firecube.ingestor.extensions.grid import (
        LatLonBinner,
        build_latlon_binner,
        grid_data_to_latlon,
        grid_xarray_dataset,
    )
    from firecube.ingestor.extensions.healpix import (
        HealpixBinner,
        build_healpix_binner,
        cells_in_bbox,
        grid_data_to_healpix,
        grid_xarray_dataset_to_healpix,
    )

__all__ = [
    "DuckDbMixin",
    "HealpixBinner",
    "LatLonBinner",
    "aggregate_by_position",
    "build_healpix_binner",
    "build_latlon_binner",
    "cells_in_bbox",
    "grid_data_to_healpix",
    "grid_data_to_latlon",
    "grid_xarray_dataset",
    "grid_xarray_dataset_to_healpix",
]

_LAZY_EXPORTS = {
    "DuckDbMixin": "firecube.ingestor.extensions.duck",
    "HealpixBinner": "firecube.ingestor.extensions.healpix",
    "LatLonBinner": "firecube.ingestor.extensions.grid",
    "aggregate_by_position": "firecube.ingestor.extensions._binning",
    "build_healpix_binner": "firecube.ingestor.extensions.healpix",
    "build_latlon_binner": "firecube.ingestor.extensions.grid",
    "cells_in_bbox": "firecube.ingestor.extensions.healpix",
    "grid_data_to_healpix": "firecube.ingestor.extensions.healpix",
    "grid_data_to_latlon": "firecube.ingestor.extensions.grid",
    "grid_xarray_dataset": "firecube.ingestor.extensions.grid",
    "grid_xarray_dataset_to_healpix": "firecube.ingestor.extensions.healpix",
}


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(name)
    from importlib import import_module

    return getattr(import_module(module_name), name)


def __dir__() -> list[str]:
    return sorted([*globals().keys(), *__all__])
