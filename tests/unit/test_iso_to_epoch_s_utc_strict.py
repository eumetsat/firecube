"""UTC-strictness of iso_to_epoch_s: what it accepts, rejects, and returns."""

from __future__ import annotations

import pytest

from firecube.core.slot_index import iso_to_epoch_s

pytestmark = pytest.mark.unit


_EXPECTED_EPOCH_SECONDS = 1704067200  # 2024-01-01T00:00:00 UTC.


def test_naive_iso_string_raises_value_error() -> None:
    with pytest.raises(ValueError, match="UTC-explicit ISO 8601 input"):
        iso_to_epoch_s("2024-01-01T00:00:00")


@pytest.mark.parametrize(
    "iso",
    [
        pytest.param("2024-01-01T00:00:00Z", id="z_suffix"),
        pytest.param("2024-01-01T00:00:00+00:00", id="plus_zero_offset"),
        pytest.param("2024-01-01T00:00:00-00:00", id="minus_zero_offset"),
        pytest.param("  2024-01-01T00:00:00Z  ", id="leading_and_trailing_whitespace"),
    ],
)
def test_utc_explicit_forms_produce_expected_epoch_seconds(iso: str) -> None:
    assert iso_to_epoch_s(iso) == _EXPECTED_EPOCH_SECONDS


@pytest.mark.parametrize(
    "iso",
    [
        pytest.param("2024-01-01T00:00:00+05:30", id="positive_non_utc_offset"),
        pytest.param("2024-01-01T00:00:00-07:00", id="negative_non_utc_offset"),
    ],
)
def test_non_utc_offset_rejected(iso: str) -> None:
    with pytest.raises(ValueError, match="UTC-explicit ISO 8601 input"):
        iso_to_epoch_s(iso)
