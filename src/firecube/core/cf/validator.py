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

"""Tier-1 CF-1.8 structural validator for xarray Datasets.

Checks structural compliance only (presence/shape/pattern).
No vocabulary validation (Tier 2) or UDUNITS checks (Tier 3).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator

import numpy as np
import xarray as xr

from firecube.core.cf.report import CFFinding, CFReport, CFSeverity

_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_CF_VER_RE = re.compile(r"CF-\d+\.\d+")
_TUNITS_RE = re.compile(
    r"^\s*(seconds?|minutes?|hours?|days?|months?|years?)\s+since\s+.+", re.IGNORECASE
)
_SINCE_RE = re.compile(r".*\bsince\b.*")
_LAT_UNITS = frozenset({"degrees_north", "degree_north", "degree_N", "degrees_N"})
_LON_UNITS = frozenset({"degrees_east", "degree_east", "degree_E", "degrees_E"})
_REF_ATTRS = ("coordinates", "bounds", "grid_mapping", "cell_measures", "ancillary_variables")
_ERR, _WARN, _INFO = CFSeverity.error, CFSeverity.warning, CFSeverity.info


def validate_cf18(ds: xr.Dataset, product: str, group: str) -> CFReport:
    """Run all Tier-1 CF-1.8 checks and return a structured report."""
    findings: list[CFFinding] = []
    for check in _ALL_CHECKS:
        findings.extend(check(ds))
    return CFReport(product=product, group=group, findings=findings)


def _is_time(var: xr.DataArray) -> bool:
    a = var.attrs
    if a.get("standard_name") == "time" or a.get("axis") == "T":
        return True
    return bool(_SINCE_RE.match(str(a.get("units", ""))))


def _time_coords(ds: xr.Dataset) -> Iterator[tuple[str, xr.DataArray]]:
    for n in ds.coords:
        c = ds.coords[n]
        if _is_time(c):
            yield str(n), c


def _has_geo_coord(ds: xr.Dataset, axis: str, units: frozenset[str], sn: str) -> bool:
    for n in ds.coords:
        a = ds.coords[n].attrs
        if a.get("standard_name") == sn or a.get("axis") == axis or a.get("units") in units:
            return True
    return False


def _is_firecube_internal(da: xr.DataArray) -> bool:
    """Arrays carrying `firecube_meaning` are control-plane state, not user data."""
    return "firecube_meaning" in da.attrs


def _user_data_vars(ds: xr.Dataset) -> Iterator[str]:
    for n in ds.data_vars:
        if not _is_firecube_internal(ds[n]):
            yield str(n)


def _grid_mapping_targets(ds: xr.Dataset) -> set[str]:
    """Variables referenced via a `grid_mapping` attribute.

    These are CF grid-mapping container variables: they carry the coordinate
    reference system in attributes and, per CF, have no data semantics and no
    `units`, so they are exempt from the data-variable units rule (CF010). A
    variable IS a grid-mapping container precisely because a data variable
    references it — there is no need for it to self-identify.
    """
    targets: set[str] = set()
    for n in list(ds.data_vars) + [str(c) for c in ds.coords]:
        ref = ds[n].attrs.get("grid_mapping")
        if ref:
            targets.update(str(ref).split())
    return targets


def _check_cf001(ds: xr.Dataset) -> list[CFFinding]:
    conv = ds.attrs.get("Conventions", "")
    if _CF_VER_RE.search(str(conv)):
        return []
    msg = f"Global 'Conventions' attr is missing or lacks a CF version token (got {conv!r})."
    fix = 'Add `ds.attrs["Conventions"] = "CF-1.8"` to your plugin\'s build_dataset.'
    return [CFFinding("CF001", _ERR, "/.zattrs", msg, fix)]


def _check_cf002(ds: xr.Dataset) -> list[CFFinding]:
    title = ds.attrs.get("title", "")
    if title and str(title).strip():
        return []
    return [
        CFFinding(
            "CF002",
            _WARN,
            "/.zattrs",
            "Global 'title' attribute is recommended.",
            'Add `ds.attrs["title"] = "<descriptive title>"`.',
        )
    ]


def _check_cf003(ds: xr.Dataset) -> list[CFFinding]:
    missing = [a for a in ("institution", "source", "history") if not ds.attrs.get(a)]
    if not missing:
        return []
    return [
        CFFinding(
            "CF003",
            _WARN,
            "/.zattrs",
            f"Recommended global attrs missing: {', '.join(missing)}.",
            "Set these attrs in your plugin's build_dataset.",
        )
    ]


def _check_cf004(ds: xr.Dataset) -> list[CFFinding]:
    for _ in _time_coords(ds):
        return []
    return [
        CFFinding(
            "CF004",
            _ERR,
            "/",
            "No coordinate identifiable as time (standard_name='time', axis='T', or units 'since').",
        )
    ]


def _check_cf005(ds: xr.Dataset) -> list[CFFinding]:
    return [
        CFFinding(
            "CF005",
            _ERR,
            n,
            f"Time coord '{n}' units {c.attrs.get('units', '')!r} "
            f"do not match '<unit> since <reference>'.",
        )
        for n, c in _time_coords(ds)
        if not _TUNITS_RE.match(str(c.attrs.get("units", "")))
    ]


def _check_cf006(ds: xr.Dataset) -> list[CFFinding]:
    return [
        CFFinding(
            "CF006",
            _WARN,
            n,
            f"Time coord '{n}' has no 'calendar' attribute.",
            f'Set `ds["{n}"].attrs["calendar"] = "standard"` (or appropriate).',
        )
        for n, c in _time_coords(ds)
        if "calendar" not in c.attrs
    ]


def _check_cf007(ds: xr.Dataset) -> list[CFFinding]:
    return [
        CFFinding(
            "CF007",
            _INFO,
            n,
            f"Time coord '{n}' has no axis='T' attribute (recommended for explicit identification).",
        )
        for n, c in _time_coords(ds)
        if c.attrs.get("axis") != "T"
    ]


def _check_cf008(ds: xr.Dataset) -> list[CFFinding]:
    if _has_geo_coord(ds, "Y", _LAT_UNITS, "latitude"):
        return []
    return [CFFinding("CF008", _WARN, "/", "No coordinate identifiable as latitude.")]


def _check_cf009(ds: xr.Dataset) -> list[CFFinding]:
    if _has_geo_coord(ds, "X", _LON_UNITS, "longitude"):
        return []
    return [CFFinding("CF009", _WARN, "/", "No coordinate identifiable as longitude.")]


def _check_cf010(ds: xr.Dataset) -> list[CFFinding]:
    grid_mapping = _grid_mapping_targets(ds)
    return [
        CFFinding(
            "CF010",
            _ERR,
            n,
            f"Data var '{n}' is missing 'units' attribute.",
            f'Set `ds["{n}"].attrs["units"] = "<unit>"`.',
        )
        for n in _user_data_vars(ds)
        if n not in grid_mapping and "units" not in ds[n].attrs
    ]


def _check_cf011(ds: xr.Dataset) -> list[CFFinding]:
    return [
        CFFinding("CF011", _WARN, n, f"Data var '{n}' has neither 'long_name' nor 'standard_name'.")
        for n in _user_data_vars(ds)
        if "long_name" not in ds[n].attrs and "standard_name" not in ds[n].attrs
    ]


def _check_cf012(ds: xr.Dataset) -> list[CFFinding]:
    out: list[CFFinding] = []
    known = {str(v) for v in ds.variables}
    targets = list(_user_data_vars(ds)) + [str(n) for n in ds.coords]
    for n in targets:
        for ref in _REF_ATTRS:
            v = ds[n].attrs.get(ref)
            if not v:
                continue
            out.extend(
                CFFinding(
                    "CF012",
                    _ERR,
                    str(n),
                    f"Var '{n}' attr '{ref}' references unknown variable {tok!r}.",
                )
                for tok in str(v).split()
                if tok not in known
            )
    return out


def _check_cf013(ds: xr.Dataset) -> list[CFFinding]:
    out: list[CFFinding] = []
    targets = list(_user_data_vars(ds)) + [str(n) for n in ds.coords]
    for n in targets:
        b = ds[n].attrs.get("bounds")
        if not b or b not in ds.variables:
            continue
        s = ds[b].shape
        if len(s) != 2 or s[-1] != 2:
            out.append(
                CFFinding(
                    "CF013", _ERR, str(b), f"Bounds var '{b}' has shape {s}, expected (N, 2)."
                )
            )
    return out


def _check_cf014(ds: xr.Dataset) -> list[CFFinding]:
    skip = {n for n in ds.data_vars if _is_firecube_internal(ds[n])}
    return [
        CFFinding(
            "CF014",
            _WARN,
            str(n),
            f"Variable name {str(n)!r} does not match CF naming pattern '[A-Za-z][A-Za-z0-9_]*'.",
        )
        for n in ds.variables
        if str(n) not in skip and not _NAME_RE.match(str(n))
    ]


def _check_cf015(ds: xr.Dataset) -> list[CFFinding]:
    out: list[CFFinding] = []
    for n in ds.coords:
        c = ds.coords[n]
        if c.ndim != 1 or str(c.dims[0]) != str(n):
            continue
        v = np.asarray(c.values)
        if v.size < 2 or v.dtype.kind not in "iuf":
            continue
        d = np.diff(v)
        if not (bool(np.all(d > 0)) or bool(np.all(d < 0))):
            out.append(CFFinding("CF015", _WARN, str(n), f"Coordinate '{n}' is not monotonic."))
    return out


_ALL_CHECKS: tuple[Callable[[xr.Dataset], list[CFFinding]], ...] = (
    _check_cf001,
    _check_cf002,
    _check_cf003,
    _check_cf004,
    _check_cf005,
    _check_cf006,
    _check_cf007,
    _check_cf008,
    _check_cf009,
    _check_cf010,
    _check_cf011,
    _check_cf012,
    _check_cf013,
    _check_cf014,
    _check_cf015,
)
