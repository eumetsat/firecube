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

"""Fixtures and measurement helpers for the peak-retained-payload harness.

The measurement instrument is ``/usr/bin/time -v`` (GNU time), invoked as a
Linux-only subprocess around ``firecube ingest``. Parsing logic is kept
intentionally small so the format regression surface is one file.

Three environmental prerequisites determine whether a benchmark test can
execute a real ingest or must skip with a clear reason:

1. Host is Linux (``/usr/bin/time -v`` reports ``Maximum resident set size``).
2. ``FIRECUBE_FCI_SMOKE_DATA`` (or the documented default path) resolves to a
   readable directory containing the FCI L1C smoke fixture.
3. The ``firecube-mtg-fci-l1c`` plugin is importable (its ingest is what the
   baseline measures).

When any of these fails, the affected test is skipped with a reason string
containing one of the canonical tokens documented in
``KNOWN_SKIP_TOKENS`` — the ``test_environment_skip_documented`` test relies
on those tokens as its stability contract.

Baseline provenance: the JSON schema records a ``source`` field —
``"campaign-evidence"`` when the file was seeded from the known baseline
number (14.5201 GiB) so downstream tests have something to compare against on
hosts without the plugin, or ``"measured"`` when written by a fresh baseline
capture. Both are valid; ``"measured"`` is preferred whenever the environment
allows an actual ingest.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from importlib.util import find_spec
from pathlib import Path
from typing import Final

import pytest

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
EVIDENCE_DIR: Final[Path] = REPO_ROOT / ".evidence"
BASELINE_JSON: Final[Path] = EVIDENCE_DIR / "task-T0-baseline.json"
MEMRAY_REPORT: Final[Path] = EVIDENCE_DIR / "task-T0-memray-eager-baseline.txt"

DEFAULT_FCI_SMOKE_PATH: Final[str] = str(Path.home() / "firecube-smoke-data")
FCI_SMOKE_ENV_VAR: Final[str] = "FIRECUBE_FCI_SMOKE_DATA"
MTG_FCI_PLUGIN_MODULE: Final[str] = "firecube_mtg_fci_l1c"
MTG_FCI_PLUGIN_NAME: Final[str] = "mtg_fci_l1c"
TIME_V_BINARY: Final[Path] = Path("/usr/bin/time")

CAMPAIGN_BASELINE_MAX_RSS_GIB: Final[float] = 14.5201
# Calibration source for REGRESSION_TOLERANCE_FRACTION (±5 % band).
# Cross-session memory drift observed in baseline measurements: 0.0012 GiB.
CAMPAIGN_MEMORY_DRIFT_GIB: Final[float] = 0.0012
REGRESSION_TOLERANCE_FRACTION: Final[float] = 0.05

KNOWN_SKIP_TOKENS: Final[tuple[str, ...]] = (
    "non-Linux host",
    "/usr/bin/time -v not available",
    "FCI smoke fixture not available",
    "firecube-mtg-fci-l1c plugin not installed",
    "memray not installed",
)


@dataclass(frozen=True)
class ParsedTimeV:
    """Fields extracted from a ``/usr/bin/time -v`` output block.

    ``max_rss_gib`` is the ``Maximum resident set size (kbytes)`` value
    converted to GiB (base 1024). ``vm_hwm_gib`` stays ``None`` because
    ``time -v`` does not report VmHWM directly; a live-sampler cross-check
    would produce it separately and is intentionally out of scope for the
    smoke harness.
    """

    wall_s: float
    max_rss_gib: float
    cpu_user_s: float
    cpu_sys_s: float
    minor_faults: int
    major_faults: int
    exit_status: int


@dataclass(frozen=True)
class IngestMeasurement:
    """One measurement of an ingest subprocess plus its ``time -v`` block."""

    parsed: ParsedTimeV
    command: list[str]
    time_v_text: str
    stdout: str
    stderr: str
    returncode: int


@dataclass
class BaselineRecord:
    """Persisted baseline JSON — see module docstring for provenance rules."""

    max_rss_gib: float
    vm_hwm_gib: float | None
    wall_s: float | None
    cpu_user_s: float | None
    cpu_sys_s: float | None
    minor_faults: int | None
    timestamp: str
    host_id: str
    source: str
    notes: list[str] = field(default_factory=list)


def parse_time_v_output(text: str) -> ParsedTimeV:
    """Extract the fields the harness gates on from ``/usr/bin/time -v`` text.

    Raises ``ValueError`` when the ``Maximum resident set size`` line is
    absent — the caller must not swallow that error, because it means the
    subprocess did not run under ``time -v`` at all and the "measurement"
    would be a silent zero.
    """

    def grab(pattern: str, default: str | None = None) -> str:
        match = re.search(pattern, text)
        if match is None:
            if default is None:
                raise ValueError(f"pattern {pattern!r} not found in time -v output")
            return default
        return match.group(1)

    raw_wall = grab(r"Elapsed \(wall clock\) time.*?:\s+(.+)").strip()
    parts = raw_wall.split(":")
    if len(parts) == 3:
        wall_s = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    elif len(parts) == 2:
        wall_s = int(parts[0]) * 60 + float(parts[1])
    else:
        wall_s = float(raw_wall or 0)

    rss_kb = int(grab(r"Maximum resident set size \(kbytes\):\s+(\d+)"))
    max_rss_gib = round(rss_kb / (1024 * 1024), 4)

    cpu_user_s = float(grab(r"User time \(seconds\):\s+([0-9.]+)", "0"))
    cpu_sys_s = float(grab(r"System time \(seconds\):\s+([0-9.]+)", "0"))
    minor_faults = int(grab(r"Minor \(reclaiming a frame\) page faults:\s+(\d+)", "0"))
    major_faults = int(grab(r"Major \(requiring I/O\) page faults:\s+(\d+)", "0"))
    exit_status = int(grab(r"Exit status:\s+(-?\d+)", "0"))

    return ParsedTimeV(
        wall_s=wall_s,
        max_rss_gib=max_rss_gib,
        cpu_user_s=cpu_user_s,
        cpu_sys_s=cpu_sys_s,
        minor_faults=minor_faults,
        major_faults=major_faults,
        exit_status=exit_status,
    )


def resolve_fci_smoke_path() -> Path | None:
    """Return the FCI smoke directory when available, else ``None``.

    Precedence: ``FIRECUBE_FCI_SMOKE_DATA`` env var overrides
    ``DEFAULT_FCI_SMOKE_PATH``. Both must resolve to a readable directory
    that is not empty; otherwise the caller must skip.
    """

    override = os.environ.get(FCI_SMOKE_ENV_VAR)
    candidate = Path(override) if override else Path(DEFAULT_FCI_SMOKE_PATH)
    if not candidate.is_dir():
        return None
    if not any(candidate.iterdir()):
        return None
    return candidate


def is_linux() -> bool:
    return platform.system() == "Linux"


def time_v_available() -> bool:
    if not is_linux():
        return False
    if not TIME_V_BINARY.exists():
        return False
    try:
        completed = subprocess.run(
            [str(TIME_V_BINARY), "-v", "true"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return "Maximum resident set size" in completed.stderr


def mtg_fci_plugin_available() -> bool:
    return find_spec(MTG_FCI_PLUGIN_MODULE) is not None


def memray_available() -> bool:
    return find_spec("memray") is not None


def _explain_environment_skips() -> list[str]:
    reasons: list[str] = []
    if not is_linux():
        reasons.append(f"non-Linux host (platform={platform.system()})")
    elif not time_v_available():
        reasons.append("/usr/bin/time -v not available")
    if resolve_fci_smoke_path() is None:
        override = os.environ.get(FCI_SMOKE_ENV_VAR)
        location = override or DEFAULT_FCI_SMOKE_PATH
        reasons.append(f"FCI smoke fixture not available (looked at {location})")
    if not mtg_fci_plugin_available():
        reasons.append(
            f"firecube-mtg-fci-l1c plugin not installed (module {MTG_FCI_PLUGIN_MODULE})"
        )
    return reasons


def skip_if_ingest_env_unavailable() -> None:
    """Skip the calling test unless every ingest-run precondition holds."""

    reasons = _explain_environment_skips()
    if reasons:
        pytest.skip("; ".join(reasons))


def build_ingest_command(
    *,
    firecube_binary: str,
    plugin: str,
    input_data: Path,
    target: Path,
    product_name: str,
) -> list[str]:
    """Explicit CLI invocation — mirrors AGENTS.md "no inference" contract."""

    return [
        firecube_binary,
        "ingest",
        plugin,
        "--input-data",
        str(input_data),
        "--target",
        f"file://{target}",
        "--product-name",
        product_name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--output-format",
        "zarr",
        "--write-mode",
        "direct",
    ]


def run_ingest_under_time_v(
    *,
    firecube_binary: str,
    input_data: Path,
    target: Path,
    product_name: str,
    time_txt_path: Path,
    timeout_s: float = 900.0,
) -> IngestMeasurement:
    """Run ``firecube ingest`` wrapped in ``/usr/bin/time -v``, parsed once.

    ``/usr/bin/time -v`` writes its report to stderr by default; the ``-o``
    option redirects that report to ``time_txt_path`` so the child's own
    stderr stays clean for diagnostics. If the file is empty when the child
    exits, the wrapper never captured a peak — the caller treats that as a
    hard failure rather than a "measurement of zero".
    """

    if not time_v_available():
        raise RuntimeError("/usr/bin/time -v not available on this host")

    command = build_ingest_command(
        firecube_binary=firecube_binary,
        plugin=MTG_FCI_PLUGIN_NAME,
        input_data=input_data,
        target=target,
        product_name=product_name,
    )
    wrapped: list[str] = [
        str(TIME_V_BINARY),
        "-v",
        "-o",
        str(time_txt_path),
        *command,
    ]

    completed = subprocess.run(
        wrapped,
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    if not time_txt_path.exists() or time_txt_path.stat().st_size == 0:
        raise RuntimeError(
            f"/usr/bin/time -v produced no report at {time_txt_path}; measurement invalid"
        )
    time_v_text = time_txt_path.read_text()
    parsed = parse_time_v_output(time_v_text)

    return IngestMeasurement(
        parsed=parsed,
        command=wrapped,
        time_v_text=time_v_text,
        stdout=completed.stdout,
        stderr=completed.stderr,
        returncode=completed.returncode,
    )


def write_baseline_record(path: Path, record: BaselineRecord) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(record)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def read_baseline_record(path: Path) -> BaselineRecord | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text())
    return BaselineRecord(
        max_rss_gib=float(payload["max_rss_gib"]),
        vm_hwm_gib=payload.get("vm_hwm_gib"),
        wall_s=payload.get("wall_s"),
        cpu_user_s=payload.get("cpu_user_s"),
        cpu_sys_s=payload.get("cpu_sys_s"),
        minor_faults=payload.get("minor_faults"),
        timestamp=payload["timestamp"],
        host_id=payload["host_id"],
        source=payload["source"],
        notes=list(payload.get("notes", [])),
    )


def nan_aware_equal(a, b) -> tuple[bool, str]:
    """NaN-aware bit-identical equality for the equivalence gate downstream."""

    import numpy as np

    if a.shape != b.shape:
        return False, f"shape {a.shape} vs {b.shape}"
    if a.dtype != b.dtype:
        return False, f"dtype {a.dtype} vs {b.dtype}"
    if a.dtype.kind == "f":
        eq = (a == b) | (np.isnan(a) & np.isnan(b))
    else:
        eq = a == b
    n_bad = int((~eq).sum())
    if n_bad:
        idx = np.argwhere(np.atleast_1d(~eq))[0]
        return False, f"{n_bad} elements differ; first at {tuple(idx)}"
    return True, "identical"


def make_measurement_record(measurement: IngestMeasurement, *, source: str) -> BaselineRecord:
    return BaselineRecord(
        max_rss_gib=measurement.parsed.max_rss_gib,
        vm_hwm_gib=None,
        wall_s=measurement.parsed.wall_s,
        cpu_user_s=measurement.parsed.cpu_user_s,
        cpu_sys_s=measurement.parsed.cpu_sys_s,
        minor_faults=measurement.parsed.minor_faults,
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
        host_id=socket.gethostname(),
        source=source,
    )


@pytest.fixture(scope="session")
def evidence_dir() -> Path:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    return EVIDENCE_DIR


@pytest.fixture(scope="session")
def baseline_json_path(evidence_dir: Path) -> Path:
    return evidence_dir / BASELINE_JSON.name


@pytest.fixture(scope="session")
def firecube_binary() -> str:
    override = os.environ.get("FIRECUBE_BINARY")
    if override:
        return override
    which = shutil.which("firecube")
    if which:
        return which
    return f"{sys.executable} -m firecube"


@pytest.fixture(scope="session")
def fci_smoke_path() -> Path | None:
    return resolve_fci_smoke_path()


@pytest.fixture(scope="session")
def environment_skip_reasons() -> list[str]:
    return _explain_environment_skips()
