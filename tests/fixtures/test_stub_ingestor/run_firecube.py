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

"""Subprocess entry point: registers `fc_test_stub` then runs the firecube CLI."""

from __future__ import annotations

import sys
from pathlib import Path

_FIXTURE_ROOT = Path(__file__).resolve().parent.parent
if str(_FIXTURE_ROOT) not in sys.path:
    sys.path.insert(0, str(_FIXTURE_ROOT))

import test_stub_ingestor  # noqa: F401,E402
from firecube.cli.main import cli  # noqa: E402

if __name__ == "__main__":
    cli()
