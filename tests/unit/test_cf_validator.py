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

# pyright: reportMissingImports=false

from __future__ import annotations

import numpy as np
import xarray as xr

from firecube.core.cf import CFReport, CFSeverity
from firecube.core.cf.validator import validate_cf18
from tests.fixtures.cf_dataset_fixtures import (
    make_broken_dataset,
    make_cf_compliant_dataset,
    make_legacy_timestamp_dataset,
)


def _ids(report: CFReport, check_id: str) -> list[str]:
    return [f.id for f in report.findings if f.id == check_id]


def _by_id(report: CFReport, check_id: str):
    return [f for f in report.findings if f.id == check_id]


def _make_2d_lat_lon_dataset() -> xr.Dataset:
    n_y, n_x = 4, 5
    yy, xx = np.meshgrid(np.arange(n_y), np.arange(n_x), indexing="ij")
    lat2d = -30 + 60 * yy / (n_y - 1)
    lon2d = -20 + 40 * xx / (n_x - 1)
    times = np.arange(2, dtype=np.float64)
    return xr.Dataset(
        {
            "temperature": (
                ("time", "y", "x"),
                np.zeros((2, n_y, n_x)),
                {"units": "K", "long_name": "T"},
            )
        },
        coords={
            "time": (
                "time",
                times,
                {
                    "units": "days since 2000-01-01",
                    "calendar": "standard",
                    "standard_name": "time",
                    "axis": "T",
                },
            ),
            "lat": (("y", "x"), lat2d, {"standard_name": "latitude", "units": "degrees_north"}),
            "lon": (("y", "x"), lon2d, {"standard_name": "longitude", "units": "degrees_east"}),
        },
        attrs={
            "Conventions": "CF-1.8",
            "title": "2D aux coords",
            "institution": "x",
            "source": "x",
            "history": "x",
        },
    )


def test_validate_returns_cfreport_with_product_group() -> None:
    ds = make_cf_compliant_dataset()
    report = validate_cf18(ds, "TEST_PRODUCT", "level1")

    assert isinstance(report, CFReport)
    assert report.product == "TEST_PRODUCT"
    assert report.group == "level1"


def test_compliant_dataset_zero_errors() -> None:
    ds = make_cf_compliant_dataset()
    report = validate_cf18(ds, "p", "g")

    assert report.summary.errors == 0


def test_cf001_missing_conventions_error() -> None:
    ds = make_broken_dataset("conventions")
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF001")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.error


def test_cf001_compliant_no_finding() -> None:
    ds = make_cf_compliant_dataset()
    report = validate_cf18(ds, "p", "g")

    assert _ids(report, "CF001") == []


def test_cf002_missing_title_warning() -> None:
    ds = make_cf_compliant_dataset()
    ds.attrs = {k: v for k, v in ds.attrs.items() if k != "title"}
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF002")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.warning


def test_cf003_missing_recommended_attrs_warning() -> None:
    ds = make_cf_compliant_dataset()
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF003")
    assert len(findings) == 1
    assert "institution" in findings[0].message
    assert "source" in findings[0].message
    assert "history" in findings[0].message


def test_cf004_no_time_coord_error() -> None:
    ds = xr.Dataset(
        {"x_var": (("x",), np.zeros(3), {"units": "K", "long_name": "x"})},
        coords={"x": ("x", np.arange(3), {"units": "m"})},
        attrs={"Conventions": "CF-1.8"},
    )
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF004")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.error


def test_cf004_legacy_timestamp_with_standard_name_passes() -> None:
    ds = make_legacy_timestamp_dataset()
    report = validate_cf18(ds, "p", "g")

    assert _ids(report, "CF004") == []
    assert _ids(report, "CF005") == []


def test_cf005_bad_time_units_error() -> None:
    ds = make_cf_compliant_dataset()
    ds["time"].attrs = dict(ds["time"].attrs)
    ds["time"].attrs["units"] = "foo bar baz"
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF005")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.error


def test_cf005_missing_time_units_error_via_fixture() -> None:
    ds = make_broken_dataset("time_units")
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF005")
    assert len(findings) >= 1


def test_cf006_missing_calendar_warning() -> None:
    ds = make_cf_compliant_dataset()
    ds["time"].attrs = {k: v for k, v in ds["time"].attrs.items() if k != "calendar"}
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF006")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.warning


def test_cf007_missing_axis_T_info() -> None:
    ds = make_cf_compliant_dataset()
    ds["time"].attrs = {k: v for k, v in ds["time"].attrs.items() if k != "axis"}
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF007")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.info


def test_cf008_no_latitude_warning() -> None:
    times = np.arange(2, dtype=np.float64)
    ds = xr.Dataset(
        {"v": (("time",), np.zeros(2), {"units": "K", "long_name": "v"})},
        coords={
            "time": (
                "time",
                times,
                {
                    "units": "days since 2000-01-01",
                    "standard_name": "time",
                    "axis": "T",
                    "calendar": "standard",
                },
            )
        },
        attrs={"Conventions": "CF-1.8"},
    )
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF008")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.warning


def test_cf008_2d_aux_lat_passes() -> None:
    ds = _make_2d_lat_lon_dataset()
    report = validate_cf18(ds, "p", "g")

    assert _ids(report, "CF008") == []
    assert _ids(report, "CF009") == []


def test_cf009_no_longitude_warning() -> None:
    times = np.arange(2, dtype=np.float64)
    ds = xr.Dataset(
        {"v": (("time",), np.zeros(2), {"units": "K", "long_name": "v"})},
        coords={
            "time": (
                "time",
                times,
                {
                    "units": "days since 2000-01-01",
                    "standard_name": "time",
                    "axis": "T",
                    "calendar": "standard",
                },
            )
        },
        attrs={"Conventions": "CF-1.8"},
    )
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF009")
    assert len(findings) == 1


def test_cf010_missing_var_units_error() -> None:
    ds = make_broken_dataset("var_units")
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF010")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.error
    assert "temperature" in findings[0].message


def test_cf010_exempts_grid_mapping_container() -> None:
    """A variable referenced via `grid_mapping` is a CF grid-mapping container —
    it carries the CRS in attributes and has no units by design, so CF010 must
    not flag it. A unitless var that is NOT referenced still errors (the
    exemption stays narrow)."""
    ds = xr.Dataset(
        {
            "temperature": (
                ("time", "y", "x"),
                np.zeros((1, 2, 2)),
                {"units": "K", "long_name": "T", "grid_mapping": "crs"},
            ),
            "crs": ((), np.int32(0), {"grid_mapping_name": "latitude_longitude"}),
            "no_units_var": (("time", "y", "x"), np.zeros((1, 2, 2)), {"long_name": "x"}),
        },
        coords={
            "time": (
                "time",
                np.array([0.0]),
                {"standard_name": "time", "units": "days since 2000-01-01"},
            ),
        },
        attrs={"Conventions": "CF-1.8"},
    )

    report = validate_cf18(ds, "p", "g")
    flagged = {f.target for f in _by_id(report, "CF010")}

    assert "crs" not in flagged, "grid-mapping container must be exempt from CF010"
    assert "no_units_var" in flagged, "non-referenced unitless var must still error"


def test_cf011_missing_long_or_standard_name_warning() -> None:
    ds = make_cf_compliant_dataset()
    ds["temperature"].attrs = {"units": "K"}
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF011")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.warning


def test_cf012_broken_reference_error() -> None:
    ds = make_broken_dataset("broken_reference")
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF012")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.error
    assert "nonexistent_var" in findings[0].message


def test_cf013_bad_bounds_shape_error() -> None:
    ds = make_cf_compliant_dataset()
    bad_bounds = xr.DataArray(np.zeros((3, 5)), dims=("time", "extra"))
    ds = ds.assign(time_bnds=bad_bounds)
    ds["time"].attrs = dict(ds["time"].attrs)
    ds["time"].attrs["bounds"] = "time_bnds"
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF013")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.error


def test_cf014_bad_variable_name_warning() -> None:
    ds = make_cf_compliant_dataset()
    ds = ds.rename({"temperature": "1bad-name"})
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF014")
    assert len(findings) == 1
    assert findings[0].severity is CFSeverity.warning


def test_cf015_non_monotonic_coord_warning() -> None:
    ds = xr.Dataset(
        {"v": (("y",), np.zeros(4), {"units": "K", "long_name": "v"})},
        coords={
            "y": (
                "y",
                np.array([10.0, 20.0, 5.0, 30.0]),
                {"units": "degrees_north", "standard_name": "latitude", "axis": "Y"},
            )
        },
        attrs={"Conventions": "CF-1.8"},
    )
    report = validate_cf18(ds, "p", "g")

    findings = _by_id(report, "CF015")
    assert any(f.target == "y" for f in findings)
    assert all(f.severity is CFSeverity.warning for f in findings)


def test_cf015_monotonic_coord_no_finding() -> None:
    ds = make_cf_compliant_dataset()
    report = validate_cf18(ds, "p", "g")

    assert _ids(report, "CF015") == []


def test_aggregation_multiple_findings() -> None:
    ds = make_cf_compliant_dataset()
    ds.attrs = {}
    ds["temperature"].attrs = {}
    report = validate_cf18(ds, "p", "g")

    assert report.summary.errors >= 1
    assert report.summary.warnings >= 1
    ids = {f.id for f in report.findings}
    assert "CF001" in ids
    assert "CF010" in ids
    assert "CF011" in ids
