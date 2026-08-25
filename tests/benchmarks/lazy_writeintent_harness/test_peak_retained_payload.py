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

"""Peak-retained-payload regression harness (TEST_GAPS P2 §4).

Guards the DirectZarr eager-path retained memory floor so any later change to
``WriteIntent`` lifetime semantics (lazy payload thunks, streaming variants,
sub-slot batching) has a measurable regression signal instead of relying on
manual campaign runs. The primary metric is peak RSS (``max_rss_gib``) read
from ``/usr/bin/time -v``; wall time is recorded but is not an acceptance
metric (see the plan: cross-session wall drift ≥ 4.03 s is not reproducible,
cross-session memory drift is 0.0012 GiB and is reproducible).

Every test in this module is a benchmark; ``test_long_form_evidence`` is also
marked ``benchmark_evidence`` so long-form runs can be selected independently.

On hosts without ``/usr/bin/time -v``, the FCI smoke fixture, or the
``firecube-mtg-fci-l1c`` plugin, the actual-ingest tests skip with a reason
string containing one of the canonical tokens from
``conftest.KNOWN_SKIP_TOKENS``. ``test_environment_skip_documented`` runs
everywhere and asserts that the skip surface is coherent (the harness must
never silently pass by skipping without a documented token).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tests.benchmarks.lazy_writeintent_harness.conftest import (
    BASELINE_JSON,
    CAMPAIGN_BASELINE_MAX_RSS_GIB,
    KNOWN_SKIP_TOKENS,
    MEMRAY_REPORT,
    REGRESSION_TOLERANCE_FRACTION,
    TIME_V_BINARY,
    BaselineRecord,
    IngestMeasurement,
    _explain_environment_skips,
    make_measurement_record,
    memray_available,
    parse_time_v_output,
    read_baseline_record,
    run_ingest_under_time_v,
    skip_if_ingest_env_unavailable,
    time_v_available,
    write_baseline_record,
)


def _run_and_record(
    *,
    firecube_binary: str,
    fci_smoke_path: Path,
    tmp_path: Path,
    source: str,
) -> tuple[IngestMeasurement, BaselineRecord]:
    time_txt = tmp_path / "time.txt"
    target = tmp_path / "product.zarr"
    measurement = run_ingest_under_time_v(
        firecube_binary=firecube_binary,
        input_data=fci_smoke_path,
        target=target,
        product_name="mtg_fci_l1c_smoke",
        time_txt_path=time_txt,
    )
    if measurement.returncode != 0:
        raise AssertionError(
            f"firecube ingest returned {measurement.returncode}; "
            f"stderr tail: {measurement.stderr[-400:]!r}"
        )
    record = make_measurement_record(measurement, source=source)
    return measurement, record


@pytest.mark.benchmark
def test_environment_skip_documented(
    environment_skip_reasons: list[str], baseline_json_path: Path
) -> None:
    """Sanity gate: the harness never passes silently on an ineligible host.

    When ``environment_skip_reasons`` is non-empty this test still passes,
    but it asserts that every reason string contains a canonical token so
    downstream evidence parsing (and human review) can tell "skipped because
    the environment cannot host the measurement" from "skipped for an
    undocumented reason".
    """

    if not environment_skip_reasons:
        assert baseline_json_path.parent.exists()
        return

    for reason in environment_skip_reasons:
        assert any(token in reason for token in KNOWN_SKIP_TOKENS), (
            f"undocumented skip reason: {reason!r}; expected one of {KNOWN_SKIP_TOKENS!r}"
        )


@pytest.mark.benchmark
def test_baseline_capture(
    firecube_binary: str,
    fci_smoke_path: Path | None,
    baseline_json_path: Path,
    tmp_path: Path,
) -> None:
    """Runs the eager-path ingest and writes ``task-T0-baseline.json``.

    Emits the ``source="measured"`` record and stashes the raw ``time -v``
    text next to the JSON so the peak can be audited without re-running.
    """

    skip_if_ingest_env_unavailable()
    assert fci_smoke_path is not None

    measurement, record = _run_and_record(
        firecube_binary=firecube_binary,
        fci_smoke_path=fci_smoke_path,
        tmp_path=tmp_path,
        source="measured",
    )
    assert measurement.parsed.max_rss_gib > 0, "time -v reported zero max RSS; measurement invalid"
    write_baseline_record(baseline_json_path, record)
    (baseline_json_path.parent / "task-T0-baseline-time-v.txt").write_text(measurement.time_v_text)


@pytest.mark.benchmark
def test_regression_smoke_within_tolerance(
    firecube_binary: str,
    fci_smoke_path: Path | None,
    baseline_json_path: Path,
    tmp_path: Path,
) -> None:
    """Re-runs the eager ingest and asserts peak RSS is within ±5 % of baseline.

    Memory is the acceptance metric. Wall time is recorded in the emitted
    ``BaselineRecord`` for diagnostics but not gated — cross-session wall
    drift is known-unreliable.
    """

    skip_if_ingest_env_unavailable()
    assert fci_smoke_path is not None

    baseline = read_baseline_record(baseline_json_path)
    if baseline is None:
        pytest.skip(
            f"baseline JSON missing at {baseline_json_path}; run test_baseline_capture first"
        )

    measurement, _record = _run_and_record(
        firecube_binary=firecube_binary,
        fci_smoke_path=fci_smoke_path,
        tmp_path=tmp_path,
        source="regression-smoke",
    )
    measured = measurement.parsed.max_rss_gib
    band = baseline.max_rss_gib * REGRESSION_TOLERANCE_FRACTION
    lower = baseline.max_rss_gib - band
    upper = baseline.max_rss_gib + band
    assert lower <= measured <= upper, (
        f"peak RSS {measured:.4f} GiB outside tolerance band "
        f"[{lower:.4f}, {upper:.4f}] GiB around baseline "
        f"{baseline.max_rss_gib:.4f} GiB "
        f"(±{REGRESSION_TOLERANCE_FRACTION * 100:.0f}%)"
    )


@pytest.mark.benchmark_evidence
def test_long_form_evidence(
    firecube_binary: str,
    fci_smoke_path: Path | None,
    baseline_json_path: Path,
    tmp_path: Path,
) -> None:
    """Long-form evidence: runs the ingest under memray and writes a summary.

    Selected only via ``-m benchmark_evidence``; excluded from the fast
    behavioral lane. When memray is not installed the test skips with the
    ``memray not installed`` token so evidence parsers see a documented
    non-run instead of a silent pass.
    """

    skip_if_ingest_env_unavailable()
    if not memray_available():
        pytest.skip("memray not installed")
    assert fci_smoke_path is not None

    target = tmp_path / "product.zarr"
    memray_out = tmp_path / "memray.bin"
    memray_txt = MEMRAY_REPORT

    from tests.benchmarks.lazy_writeintent_harness.conftest import (
        build_ingest_command,
    )

    ingest_cmd = build_ingest_command(
        firecube_binary=firecube_binary,
        plugin="mtg_fci_l1c",
        input_data=fci_smoke_path,
        target=target,
        product_name="mtg_fci_l1c_smoke",
    )
    memray_cmd = [
        "memray",
        "run",
        "--output",
        str(memray_out),
        *ingest_cmd,
    ]
    completed = subprocess.run(memray_cmd, capture_output=True, text=True, timeout=1800)
    assert completed.returncode == 0, (
        f"memray-wrapped ingest failed rc={completed.returncode}; "
        f"stderr tail: {completed.stderr[-400:]!r}"
    )

    summary = subprocess.run(
        ["memray", "summary", str(memray_out)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    header = (
        "# T0 memray retained-root evidence\n"
        f"# firecube ingest {fci_smoke_path} -> {target}\n"
        f"# baseline provenance: {read_baseline_record(baseline_json_path)}\n"
        "\n"
    )
    memray_txt.write_text(header + summary.stdout + "\n---\n" + summary.stderr)
    assert "pixel_time" in memray_txt.read_text(), (
        "memray summary did not name 'pixel_time' as a retention root; unexpected retention profile"
    )


def _campaign_baseline_available() -> bool:
    return CAMPAIGN_BASELINE_MAX_RSS_GIB > 0


@pytest.mark.benchmark
def test_callable_plugin_retained_peak_smoke(
    firecube_binary: str,
    tmp_path: Path,
) -> None:
    """Synthetic retained-peak smoke using ``callable_payload_test_plugin``.

    Runs the tiny ``callable_payload_safe`` ingest (4x4 pixels, one region
    array and one static coord array, one synthetic time slot) under
    ``/usr/bin/time -v`` and asserts peak RSS stays below a generous
    ceiling. Unlike ``test_regression_smoke_within_tolerance``, this test
    does NOT require the ``firecube-mtg-fci-l1c`` plugin or the FCI smoke
    fixture — it uses a fixture plugin that is always installed in the
    test lane, so the harness produces an actually-executing quantitative
    signal in CI, not just source-inspection guards.

    The ceiling is intentionally loose (2.0 GiB). The fixture ingest is a
    handful of floats; any reasonable host will land far below 100 MiB. The
    acceptance property is that ``/usr/bin/time -v`` produced a finite peak
    for a real ingest through the callable-dispatch path, and that a
    catastrophic memory regression (e.g. accumulator-style materialization
    of every intent before write) would break the ceiling instead of
    passing silently. The tight cross-release bound is
    ``test_regression_smoke_within_tolerance`` on the FCI plugin; that lane
    is operator-run with the adopted build.
    """

    if not time_v_available():
        pytest.skip("/usr/bin/time -v not available")

    time_txt = tmp_path / "time.txt"
    target = tmp_path / "callable_smoke.zarr"

    cmd = [
        str(TIME_V_BINARY),
        "-v",
        "-o",
        str(time_txt),
        firecube_binary,
        "ingest",
        "callable_payload_safe",
        "--input-data",
        str(tmp_path),
        "--target",
        f"file://{target}",
        "--product-name",
        "callable_payload_safe",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--output-format",
        "zarr",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert completed.returncode == 0, (
        f"callable_payload_safe ingest failed rc={completed.returncode}; "
        f"stderr tail: {completed.stderr[-400:]!r}"
    )
    assert time_txt.exists() and time_txt.stat().st_size > 0, (
        f"/usr/bin/time -v produced no report at {time_txt}; measurement invalid"
    )

    parsed = parse_time_v_output(time_txt.read_text())
    ceiling_gib = 2.0
    assert parsed.max_rss_gib > 0, (
        "time -v reported zero max RSS for callable_payload_safe; measurement invalid"
    )
    assert parsed.max_rss_gib < ceiling_gib, (
        f"retained peak {parsed.max_rss_gib:.4f} GiB exceeded ceiling "
        f"{ceiling_gib} GiB — unexpected memory growth in callable dispatch"
    )


@pytest.mark.benchmark
def test_baseline_json_is_readable() -> None:
    """Structural check on the persisted baseline JSON.

    Passes when the file is missing (the ``test_baseline_capture`` path is
    the writer of record) — this test only enforces that when the file
    exists, its schema is intact and every required field is present.
    """

    if not BASELINE_JSON.is_file():
        pytest.skip(f"baseline JSON not yet written at {BASELINE_JSON}")

    record = read_baseline_record(BASELINE_JSON)
    assert record is not None
    assert record.max_rss_gib > 0, "recorded max_rss_gib must be positive"
    assert record.source in {"campaign-evidence", "measured", "regression-smoke"}
    assert record.host_id
    assert record.timestamp
    if not _explain_environment_skips() and record.source == "campaign-evidence":
        pytest.fail(
            "environment permits measurement but baseline is still "
            "campaign-seeded; run test_baseline_capture to refresh"
        )
    assert _campaign_baseline_available(), (
        "campaign baseline constant lost from conftest; "
        "downstream comparisons would silently degrade"
    )
