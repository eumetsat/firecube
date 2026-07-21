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

import json
import logging
import os
import socket
import sys

from opentelemetry import trace


class JsonFormatter(logging.Formatter):
    """Emit log records as a single JSON line via :func:`json.dumps`.

    Replaces the previous hand-formatted JSON template which interpolated
    record fields directly into a format string and therefore failed to
    escape embedded ``"``, ``\\``, control characters, ``%``, and Unicode
    correctly.
    """

    def __init__(
        self,
        *,
        structured_fields: list[str] | None = None,
        datefmt: str = "%Y-%m-%dT%H:%M:%SZ",
    ) -> None:
        super().__init__(datefmt=datefmt)
        self._fields = structured_fields or [
            "asctime",
            "level",
            "name",
            "message",
        ]

    def format(self, record: logging.LogRecord) -> str:
        log_dict: dict[str, object] = {}
        for field in self._fields:
            if field == "level":
                log_dict["level"] = record.levelname
            elif field == "message":
                log_dict["message"] = record.getMessage()
            elif field == "asctime":
                log_dict["asctime"] = self.formatTime(record, self.datefmt)
            elif field in ("process", "thread", "lineno"):
                log_dict[field] = getattr(record, field)
            else:
                log_dict[field] = getattr(record, field, None)
        if record.exc_info:
            log_dict["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_dict, ensure_ascii=False, separators=(",", ":"))


def resolve_logging_env() -> tuple[str, str, list[str], bool]:
    """Resolve logging env vars from FIRECUBE_* names only."""
    log_format = (os.getenv("FIRECUBE_LOG_FORMAT") or "json").lower()
    log_level = (os.getenv("FIRECUBE_LOG_LEVEL") or "INFO").upper()

    firecube_debug = os.getenv("FIRECUBE_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}

    structured_fields_raw = os.getenv(
        "FIRECUBE_LOG_STRUCTURED_FIELDS",
        "asctime,hostname,process,thread,name,filename,lineno,level,trace_id,span_id,message",
    )

    structured_fields = structured_fields_raw.split(",")
    structured_fields = [f.strip() for f in structured_fields if f.strip()]
    structured_fields = list(dict.fromkeys(structured_fields))

    return log_format, log_level, structured_fields, firecube_debug


def configure_logging() -> None:
    """Configure structured logging (JSON or plain text).

    Convention: command outputs (including JSON emitted by CLI subcommands) go
    to stdout, while logs go to stderr.
    """
    hostname = socket.gethostname()
    log_format, log_level, structured_fields, firecube_debug = resolve_logging_env()

    class HostnameFilter(logging.Filter):
        def filter(self, record):
            record.hostname = hostname
            return True

    class TraceContextFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.trace_id = ""
            record.span_id = ""
            try:
                span = trace.get_current_span()
                span_context = span.get_span_context() if span else None
                if span_context and span_context.is_valid:
                    record.trace_id = f"{span_context.trace_id:032x}"
                    record.span_id = f"{span_context.span_id:016x}"
            except Exception:
                pass
            return True

    handler = logging.StreamHandler(sys.stderr)
    handler.addFilter(HostnameFilter())
    handler.addFilter(TraceContextFilter())

    formatter: logging.Formatter
    if log_format == "plain":
        fmt = "[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d] %(message)s"
        datefmt = "%H:%M:%S"
        formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    else:
        formatter = JsonFormatter(structured_fields=structured_fields)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    root.addHandler(handler)

    # By default, suppress noisy third-party libraries (unless root level is DEBUG)
    if log_level != "DEBUG":
        for noisy in ("aiobotocore", "botocore", "boto3", "s3fs", "fsspec", "urllib3"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    # When FIRECUBE_DEBUG=true, enable DEBUG for Firecube logs specifically
    if firecube_debug and log_level != "DEBUG":
        logging.getLogger("firecube").setLevel(logging.DEBUG)
        logging.getLogger("firecube.cli").setLevel(logging.DEBUG)
        logging.getLogger("firecube.ingestor").setLevel(logging.DEBUG)
        logging.getLogger("firecube.core").setLevel(logging.DEBUG)

    logging.debug(
        "Logging configured (format=%s, level=%s, fields=%s)",
        log_format,
        log_level,
        structured_fields,
    )
