"""Runtime guard tests for calendar/cftime import behavior.

1. Documents that cftime is transitively guaranteed (via the hard netcdf4
   dependency), even though firecube never imports it directly.
2. Exercises decode_time_array's existing 360_day object-array output.
3. Checks whether resolving a plain Gregorian RegularTimeAxis avoids
   pulling in xarray.coding.times/cftime.
"""

from __future__ import annotations

import subprocess
import sys

import numpy as np
import pytest

from firecube.core.zarr.time_decode import decode_time_array

pytestmark = pytest.mark.unit


def test_cftime_import_succeeds():
    """firecube never imports cftime directly; this test documents the
    dependency chain that guarantees it is present at runtime: xarray's CF
    time coder needs cftime at call time to decode/encode non-standard
    calendars (360_day, noleap, etc.), and cftime itself arrives
    transitively through netcdf4, a hard firecube dependency -- so any
    environment that can run firecube already has cftime importable."""
    import cftime  # noqa: F401


def test_decode_time_array_360_day_third_element():
    values = np.array([0, 1, 2], dtype="int64")
    attrs = {"units": "days since 1850-02-28", "calendar": "360_day"}
    decoded = decode_time_array(values, attrs)
    third = decoded[2]
    assert third.day == 30
    assert third.calendar == "360_day"


def test_gregorian_regular_axis_does_not_import_xarray_coding_times():
    script = (
        "import sys\n"
        "from firecube.core.encoded_time import encode_coordinate\n"
        "encode_coordinate('2024-03-01T06:00:00Z', "
        "units='seconds since 2024-01-01 00:00:00', "
        "calendar='proleptic_gregorian')\n"
        "print('xarray.coding.times' in sys.modules, 'cftime' in sys.modules)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    xarray_times_imported, cftime_imported = result.stdout.strip().split()
    assert xarray_times_imported == "False"
    assert cftime_imported == "False"
