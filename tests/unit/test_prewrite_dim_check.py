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

import numpy as np
import pytest
import xarray as xr

from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.zarr.append_services import _verify_dataset_has_time_dim


@pytest.mark.unit
def test_raises_on_dim_mismatch():
    """Raise when the dataset lacks the configured time dimension."""
    ds = xr.Dataset(
        {"x": (("timestamp",), np.arange(3))},
        coords={"timestamp": [0, 1, 2]},
    )

    with pytest.raises(ConfigurationError) as exc:
        _verify_dataset_has_time_dim(ds, "time")

    assert "Plugin declared time_dim_name='time'" in str(exc.value)
    assert "does not contain that dimension" in str(exc.value)
    assert "timestamp" in str(exc.value)


@pytest.mark.unit
def test_passthrough_when_match():
    """Dataset with matching dim passes through without error."""
    ds = xr.Dataset({"x": (("time",), np.arange(3))}, coords={"time": [0, 1, 2]})

    _verify_dataset_has_time_dim(ds, "time")
