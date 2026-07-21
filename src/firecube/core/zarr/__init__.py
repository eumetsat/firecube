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

"""Zarr utilities (validation, scrub, multires, state).

Public surface for CLI/API and plugins. Keep modules small and layered:
- `validation`: read-only Zarr inspection.
- `scrub`: mutating maintenance built on validation + ChunkManager.
- `multires`: multiresolution builders.
- `state`: shared Firecube state-array helpers.
"""

import warnings

# Zarr v3 warns about non-Zarr files (like our manifest) in the store directory.
try:
    from zarr.errors import ZarrUserWarning

    warnings.filterwarnings("ignore", category=ZarrUserWarning)
except ImportError:
    pass

from firecube.core.zarr.io import ZarrIO
from firecube.core.zarr.multires import MultiresConfig, ZarrMultiresBuilder
from firecube.core.zarr.region_writer import RegionZarrWriter, RegionZarrWriterProtocol
from firecube.core.zarr.scrub import ScrubResult, run_scrub
from firecube.core.zarr.state import (
    DEFAULT_STATE_MEANING,
    ensure_timestamp_state_array,
    expand_time_index_ranges_to_chunk_boundaries,
    update_timestamp_state,
)
from firecube.core.zarr.validation import (
    ZarrValidationReport,
    find_extra_chunks,
    group_exists,
    read_chunk_grid,
    validate_group_with_fs,
)

__all__ = [
    "DEFAULT_STATE_MEANING",
    "MultiresConfig",
    "RegionZarrWriter",
    "RegionZarrWriterProtocol",
    "ScrubResult",
    "ZarrIO",
    "ZarrMultiresBuilder",
    "ZarrValidationReport",
    "ensure_timestamp_state_array",
    "expand_time_index_ranges_to_chunk_boundaries",
    "find_extra_chunks",
    "group_exists",
    "read_chunk_grid",
    "run_scrub",
    "update_timestamp_state",
    "validate_group_with_fs",
]
