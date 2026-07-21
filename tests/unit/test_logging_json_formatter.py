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

"""RED test for ``JsonFormatter`` introduced by T20.

Until T20 lands, importing ``JsonFormatter`` raises ``ImportError``; once it
exists, every parametrized hostile-input case must produce a single line of
valid JSON whose ``message`` field round-trips via ``json.loads``.
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from firecube.core.observability.logging import JsonFormatter

HOSTILE_INPUTS = [
    ("plain_ascii", "hello world"),
    ("embedded_double_quote", 'User said "hi"'),
    ("embedded_backslash", r"path: C:\Users\data"),
    ("embedded_newline", "line1\nline2"),
    ("unicode", "naïve résumé 日本語"),
    ("control_char", "bell\x07tab\t"),
    ("percent_chars", "50%% off sale"),
    ("message_percent_with_no_args", "100% done"),
    ("exception_info", None),
]


@pytest.mark.parametrize("case_name,message", HOSTILE_INPUTS)
def test_json_formatter_hostile_inputs(case_name: str, message: str | None) -> None:
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(f"test_json.{case_name}")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    # propagate=False prevents the root handler installed by configure_logging
    # from double-formatting the record and corrupting the captured buffer.
    logger.propagate = False

    try:
        if case_name == "exception_info":
            try:
                raise ValueError('error with "quotes" and\nnewlines')
            except ValueError:
                logger.exception("caught error")
        else:
            logger.info(message)

        output = buf.getvalue().strip()
        assert output, "No output produced"

        for line in output.split("\n"):
            if not line.strip():
                continue
            parsed = json.loads(line)
            assert "message" in parsed or "level" in parsed
            if case_name != "exception_info":
                assert parsed.get("message") == message
    finally:
        logger.removeHandler(handler)
