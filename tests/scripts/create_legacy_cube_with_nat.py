#!/usr/bin/env python3
"""Create a legacy Zarr cube with time coord chunks=(1,) and one NaT slot."""

from __future__ import annotations

import sys

import numpy as np
import zarr


def main() -> None:
    path, total = sys.argv[1], int(sys.argv[2])
    root = zarr.open_group(path, mode="w")
    group = root.require_group("data")
    epoch = np.datetime64("2024-01-01T00:00:00", "ns")
    values = epoch + np.arange(total, dtype=np.int64) * np.timedelta64(600_000_000_000, "ns")
    if total > 42:
        values[42] = np.datetime64("NaT", "ns")
    group.create_array(
        "time",
        data=values,
        chunks=(1,),
        overwrite=True,
        dimension_names=("time",),
    )


if __name__ == "__main__":
    main()
