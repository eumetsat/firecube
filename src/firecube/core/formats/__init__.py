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

"""Generic input format helpers (ZIP, HDF5, NetCDF).

Public imports remain stable at ``firecube.core.formats``.
"""

from firecube.core.formats.hdf5 import materialize_hdf5_path, read_hdf5_array
from firecube.core.formats.netcdf import (
    clean_netcdf_encoding,
    normalize_string_vars,
    prepare_netcdf_for_zarr,
    rename_time_dim,
)
from firecube.core.formats.zip import (
    extract_all_from_zips,
    extract_hdf5_from_zip,
    stream_hdf5_from_zip,
)

__all__ = [
    "clean_netcdf_encoding",
    "extract_all_from_zips",
    "extract_hdf5_from_zip",
    "materialize_hdf5_path",
    "normalize_string_vars",
    "prepare_netcdf_for_zarr",
    "read_hdf5_array",
    "rename_time_dim",
    "stream_hdf5_from_zip",
]
