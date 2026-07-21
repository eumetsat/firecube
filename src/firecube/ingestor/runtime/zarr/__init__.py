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

"""Zarr write-strategy subsystem.

Public surface:

- ``AppendWriteStrategy`` — Protocol for xarray-append write strategies.
- ``RegionWriteStrategy`` — Protocol for direct region write strategies.
- ``AppendStrategy`` — xarray-append implementation (wraps ``append_time_groups``).
- ``IndexedRegionStrategy`` — direct zarr-python region writes via ``RegionZarrWriter``.
"""

from firecube.ingestor.runtime.zarr.contracts import AppendWriteStrategy, RegionWriteStrategy
from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy

__all__ = [
    "AppendStrategy",
    "AppendWriteStrategy",
    "IndexedRegionStrategy",
    "RegionWriteStrategy",
]
