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

"""GenericZarr fixture with a plugin-selectable CF calendar time axis.

Exercises the staged xarray-append path (``append_order.py`` /
``append_services.py``) against a time coordinate built with
``xr.date_range(..., calendar=..., use_cftime=True)``: a non-standard
calendar (for example ``360_day`` or ``noleap``) decodes to an object array
of calendar-valued (``cftime``) scalars rather than ``datetime64``, which is
the shape that previously crashed a second ``resume_existing`` ingest and
silently reported null coverage bounds (see ``append_order.py::AppendOrder``
and ``append_services.py::AppendCoverageBuilder``).

``calendar=None`` (the default) builds an ordinary ``datetime64`` Gregorian
time coordinate, giving a control case in the same fixture/harness.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import numpy as np
import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginConfig,
    PluginContext,
    register_ingestor,
)


@dataclass
class CalendarTimeConfig(PluginConfig):
    """Options controlling the synthetic time axis this fixture builds.

    Attributes:
        calendar: A CF calendar name (for example ``"360_day"`` or
            ``"noleap"``). ``None`` builds an ordinary Gregorian
            ``datetime64`` coordinate.
        start: ISO start date for the coordinate, interpreted in ``calendar``.
        count: Number of daily steps to generate.
        freq: Pandas/xarray frequency string for ``xr.date_range``.
    """

    calendar: str | None = None
    start: str = "2049-01-01"
    count: int = 5
    freq: str = "1D"


@register_ingestor("calendar_time_test_plugin")
class CalendarTimeIngestor(GenericZarrIngestor):
    """Synthetic single-variable cube whose time axis carries a declared calendar."""

    PRODUCT_NAME: ClassVar[str] = "calendar_time_test_plugin"
    time_dim_name: ClassVar[str] = "time"
    plugin_config_class = CalendarTimeConfig

    def build_dataset(
        self,
        group: str,
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        _ = group
        if not items:
            return None
        config = self.plugin_config
        assert isinstance(config, CalendarTimeConfig)

        calendar = config.calendar
        use_cftime = calendar is not None
        times = xr.date_range(
            start=config.start,
            periods=int(config.count),
            freq=config.freq,
            calendar=calendar or "standard",
            use_cftime=use_cftime,
        )
        values = np.arange(int(config.count), dtype=np.float32)

        return xr.Dataset(
            {
                "value": (
                    ["time"],
                    values,
                    {"units": "1", "long_name": "synthetic calendar-time value"},
                )
            },
            coords={"time": times},
            attrs={"Conventions": "CF-1.8", "title": "Calendar Time Test Cube"},
        )
