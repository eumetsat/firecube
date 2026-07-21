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

from __future__ import annotations

import numpy as np
import xarray as xr

from firecube.core.formats.netcdf import (
    clean_netcdf_encoding,
    prepare_netcdf_for_zarr,
    rename_time_dim,
)


def _make_dataset(dims: dict[str, int], encoding: dict | None = None) -> xr.Dataset:
    data = np.zeros(tuple(dims.values()), dtype=np.float32)
    ds = xr.Dataset({"var": (list(dims), data)})
    if encoding:
        ds["var"].encoding.update(encoding)
    return ds


def test_clean_encoding_strips_hdf5_hints() -> None:
    ds = _make_dataset(
        {"x": 3},
        encoding={
            "chunks": (1, 2),
            "chunksizes": (4,),
            "preferred_chunks": {"x": 10},
            "dtype": "float32",
        },
    )
    result = clean_netcdf_encoding(ds)
    enc = result["var"].encoding
    assert "chunks" not in enc
    assert "chunksizes" not in enc
    assert "preferred_chunks" not in enc
    assert enc["dtype"] == "float32"


def test_clean_encoding_preserves_other_keys() -> None:
    ds = _make_dataset(
        {"x": 3},
        encoding={"dtype": "float32", "scale_factor": 0.1, "_FillValue": -999.0},
    )
    result = clean_netcdf_encoding(ds)
    enc = result["var"].encoding
    assert enc["dtype"] == "float32"
    assert enc["scale_factor"] == 0.1
    assert enc["_FillValue"] == -999.0


def test_clean_encoding_idempotent() -> None:
    ds = _make_dataset(
        {"x": 3},
        encoding={"chunks": (1,), "dtype": "float32"},
    )
    first = clean_netcdf_encoding(ds)
    second = clean_netcdf_encoding(first)
    enc = second["var"].encoding
    assert "chunks" not in enc
    assert enc["dtype"] == "float32"


def test_rename_time_dim_renames() -> None:
    ds = _make_dataset({"time": 5, "x": 3})
    result = rename_time_dim(ds)
    assert "timestamp" in result.dims
    assert "time" not in result.dims


def test_rename_time_dim_noop_when_missing() -> None:
    ds = _make_dataset({"x": 3, "y": 4})
    result = rename_time_dim(ds)
    assert dict(result.sizes) == {"x": 3, "y": 4}


def test_rename_time_dim_custom_names() -> None:
    ds = _make_dataset({"t": 5, "x": 3})
    result = rename_time_dim(ds, source="t", target="ts")
    assert "ts" in result.dims
    assert "t" not in result.dims


def test_prepare_combines_all() -> None:
    ds = _make_dataset(
        {"time": 5, "x": 3},
        encoding={"chunks": (1, 2), "chunksizes": (4,), "dtype": "float32"},
    )
    result = prepare_netcdf_for_zarr(ds)
    assert "timestamp" in result.dims
    assert "time" not in result.dims
    enc = result["var"].encoding
    assert "chunks" not in enc
    assert "chunksizes" not in enc
    assert enc["dtype"] == "float32"


# ─── _iso.py helpers ────────────────────────────────


def test_iso_parse_utc_z_suffix() -> None:
    from datetime import datetime

    from firecube.core.formats._iso import _parse_iso_utc

    result = _parse_iso_utc("2026-07-12T14:30:00Z")

    assert result == datetime(2026, 7, 12, 14, 30, 0)
    assert result.tzinfo is None


def test_iso_parse_utc_explicit_offset() -> None:
    from datetime import datetime

    from firecube.core.formats._iso import _parse_iso_utc

    result = _parse_iso_utc("2026-07-12T14:30:00+00:00")

    assert result == datetime(2026, 7, 12, 14, 30, 0)
    assert result.tzinfo is None


def test_iso_parse_preserves_microseconds() -> None:
    from firecube.core.formats._iso import _parse_iso_utc

    result = _parse_iso_utc("2026-07-12T14:30:00.123456Z")

    assert result.microsecond == 123456


def test_iso_parse_non_utc_raises() -> None:
    import pytest

    from firecube.core.formats._iso import _parse_iso_utc

    with pytest.raises(NotImplementedError):
        _parse_iso_utc("2026-07-12T14:30:00+05:00")


def test_iso_parse_empty_raises() -> None:
    import pytest

    from firecube.core.formats._iso import _parse_iso_utc

    with pytest.raises(ValueError):
        _parse_iso_utc("")


def test_iso_parse_invalid_raises() -> None:
    import pytest

    from firecube.core.formats._iso import _parse_iso_utc

    with pytest.raises(ValueError):
        _parse_iso_utc("not-a-date")


def test_iso_parse_nanosecond_raises() -> None:
    import pytest

    from firecube.core.formats._iso import _iso_strs_to_datetime64

    with pytest.raises(ValueError):
        _iso_strs_to_datetime64(["2026-07-12T14:30:00.1234567Z"])


def test_iso_parse_timezone_naive_raises() -> None:
    import pytest

    from firecube.core.formats._iso import _parse_iso_utc

    with pytest.raises(ValueError):
        _parse_iso_utc("2026-07-12T14:30:00")


def test_iso_strs_to_datetime64_seconds_batch() -> None:
    from firecube.core.formats._iso import _iso_strs_to_datetime64

    result = _iso_strs_to_datetime64(
        [
            "2026-07-12T14:30:00Z",
            "2026-07-12T14:31:00Z",
        ]
    )

    assert result.dtype == np.dtype("datetime64[s]")
    assert len(result) == 2


def test_iso_strs_to_datetime64_mixed_precision_batch() -> None:
    from firecube.core.formats._iso import _iso_strs_to_datetime64

    result = _iso_strs_to_datetime64(
        [
            "2026-07-12T14:30:00.5Z",
            "2026-07-12T14:31:00Z",
        ]
    )

    assert result.dtype == np.dtype("datetime64[us]")


def test_iso_strs_to_datetime64_empty_batch() -> None:
    from firecube.core.formats._iso import _iso_strs_to_datetime64

    result = _iso_strs_to_datetime64([])

    assert result.shape == (0,)


def _string_ds(name: str, values: np.ndarray, dim: str = "t") -> xr.Dataset:
    return xr.Dataset({name: (dim, values)})


# ─── normalize_string_vars ────────────────────────────


def test_normalize_fixed_length_utf_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype="<U20"))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_fixed_length_bytes_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array([b"2026-01-01T00:00:00Z"], dtype="S25"))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_object_dtype_python_str_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype=object))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_object_dtype_python_bytes_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array([b"2026-01-01T00:00:00Z"], dtype=object))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_object_dtype_numpy_str_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array([np.str_("2026-01-01T00:00:00Z")], dtype=object))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_object_dtype_numpy_bytes_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds(
        "ts",
        np.array([np.bytes_(b"2026-01-01T00:00:00Z")], dtype=object),
    )

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_post_concat_widening_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds_a = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype="<U20"), "t")
    ds_b = _string_ds("ts", np.array(["2026-01-01T00:01:00Z"], dtype=object), "t")

    combined = xr.concat(
        [ds_a, ds_b],
        dim="t",
        data_vars="all",
        coords="minimal",
        combine_attrs="override",
    )

    out = normalize_string_vars(combined, iso_targets={"ts"})

    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_already_datetime64_no_op() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array(["2026-01-01"], dtype="datetime64[s]"))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert out["ts"].dtype == np.dtype("datetime64[s]")


def test_normalize_bytes_utf8_decode_non_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("meta", np.array([b"hello"], dtype="S20"))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert out["meta"].dtype.kind == "U"


def test_normalize_object_str_non_iso_target() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("meta", np.array(["hello"], dtype=object))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert out["meta"].dtype.kind == "U"


def test_normalize_object_non_string_not_in_iso_targets_skipped(caplog) -> None:
    import logging

    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("meta", np.array([1, 2, 3], dtype=object))

    with caplog.at_level(logging.DEBUG):
        out = normalize_string_vars(ds, iso_targets={"other"}, logger=logging.getLogger("test"))

    assert out["meta"].dtype == object
    records = [record for record in caplog.records if "meta" in record.message]
    assert records, "expected DEBUG log mentioning 'meta'"


def test_normalize_object_non_string_in_iso_targets_raises() -> None:
    import pytest

    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array([1, 2, 3], dtype=object))

    with pytest.raises(ValueError):
        normalize_string_vars(ds, iso_targets={"ts"})


def test_normalize_object_empty_in_iso_targets_raises() -> None:
    import pytest

    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array([], dtype=object))

    with pytest.raises(ValueError):
        normalize_string_vars(ds, iso_targets={"ts"})


def test_normalize_object_empty_not_in_iso_targets_skipped() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("meta", np.array([], dtype=object))

    out = normalize_string_vars(ds, iso_targets={"ts"})

    assert out["meta"].dtype == object


def test_normalize_units_calendar_moved_to_encoding_by_default() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype="<U20"))
    ds["ts"].attrs["units"] = "ISO 8601"
    ds["ts"].attrs["calendar"] = "standard"
    out = normalize_string_vars(ds, iso_targets={"ts"})
    # Attrs: units/calendar gone
    assert "units" not in out["ts"].attrs
    assert "calendar" not in out["ts"].attrs
    # Encoding: units/calendar present (moved, not deleted)
    assert out["ts"].encoding.get("units") == "ISO 8601"
    assert out["ts"].encoding.get("calendar") == "standard"
    # Still converted to datetime64
    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_preserve_cf_time_attrs_keeps_them_in_attrs() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype="<U20"))
    ds["ts"].attrs["units"] = "ISO 8601"
    ds["ts"].attrs["calendar"] = "standard"
    out = normalize_string_vars(ds, iso_targets={"ts"}, preserve_cf_time_attrs=True)
    # Attrs: preserved verbatim
    assert out["ts"].attrs.get("units") == "ISO 8601"
    assert out["ts"].attrs.get("calendar") == "standard"
    # Encoding: units/calendar NOT written in opt-in mode
    assert "units" not in out["ts"].encoding
    assert "calendar" not in out["ts"].encoding
    assert str(out["ts"].dtype).startswith("datetime64")


def test_normalize_iso_target_does_not_copy_source_encoding() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    ds = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype="<U20"))
    # Pollute source encoding with stale/hostile keys
    ds["ts"].encoding = {
        "chunks": (1,),
        "dtype": "S20",
        "units": "days since 1900-01-01",  # should NOT survive
        "calendar": "julian",  # should NOT survive
        "_FillValue": "MISSING",
    }
    # Attrs win (correct units/calendar move from here)
    ds["ts"].attrs["units"] = "ISO 8601"
    ds["ts"].attrs["calendar"] = "standard"
    out = normalize_string_vars(ds, iso_targets={"ts"})
    # Stale source-encoding keys must not leak
    assert "chunks" not in out["ts"].encoding
    assert "dtype" not in out["ts"].encoding
    assert "_FillValue" not in out["ts"].encoding
    # Attrs won: encoding has attrs values, NOT the source-encoding values
    assert out["ts"].encoding.get("units") == "ISO 8601"
    assert out["ts"].encoding.get("calendar") == "standard"


def test_normalize_moves_only_present_cf_time_attrs() -> None:
    from firecube.core.formats.netcdf import normalize_string_vars

    # Case 1: only units
    ds_u = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype="<U20"))
    ds_u["ts"].attrs["units"] = "ISO 8601"
    out_u = normalize_string_vars(ds_u, iso_targets={"ts"})
    assert out_u["ts"].encoding.get("units") == "ISO 8601"
    assert "calendar" not in out_u["ts"].encoding
    assert "units" not in out_u["ts"].attrs
    # Case 2: only calendar
    ds_c = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype="<U20"))
    ds_c["ts"].attrs["calendar"] = "standard"
    out_c = normalize_string_vars(ds_c, iso_targets={"ts"})
    assert out_c["ts"].encoding.get("calendar") == "standard"
    assert "units" not in out_c["ts"].encoding
    assert "calendar" not in out_c["ts"].attrs
    # Case 3: neither — encoding stays empty for these keys
    ds_n = _string_ds("ts", np.array(["2026-01-01T00:00:00Z"], dtype="<U20"))
    out_n = normalize_string_vars(ds_n, iso_targets={"ts"})
    assert "units" not in out_n["ts"].encoding
    assert "calendar" not in out_n["ts"].encoding
