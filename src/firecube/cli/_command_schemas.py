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

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click

from firecube.cli._uri_policy import SCHEME_TO_STORAGE_TYPE
from firecube.core.uris import infer_target_protocol


@dataclass(slots=True)
class IngestCommandConfig:
    """Validated ingest command configuration.

    Uses aggregate validation to report all missing/invalid command-level values at once.
    """

    plugin: str
    input_data: str | Path | None
    target: str
    write_mode: str | None
    storage_type: str | None
    storage_driver: str | None
    product_name: str | None = None
    output_format: str = "zarr"
    in_memory: bool = False
    options: dict[str, Any] = field(default_factory=dict)

    _VALID_WRITE_MODES = frozenset({"staged", "direct"})
    _VALID_STORAGE_TYPES = frozenset({"local", "s3"})
    _VALID_STORAGE_DRIVERS = frozenset({"fsspec", "obstore"})

    def __post_init__(self) -> None:
        """Aggregate all validation errors into a single Click usage error."""
        errors: list[str] = []

        if self.product_name is not None and self.product_name == "":
            errors.append(
                "--product-name was provided but is empty. Either omit it (use plugin "
                "PRODUCT_NAME or config default_product_name), or provide a non-empty value."
            )

        if not self.target:
            errors.append("--target is required.")

        if self.write_mode is None:
            errors.append(
                "--write-mode is required. No inference from target locality. "
                "Choose: staged (workspace-first then upload) or direct (stream to target)."
            )
        elif self.write_mode not in self._VALID_WRITE_MODES:
            errors.append(
                f"--write-mode must be one of: {', '.join(sorted(self._VALID_WRITE_MODES))} "
                f"(got '{self.write_mode}')"
            )

        if self.storage_type is not None and self.storage_type not in self._VALID_STORAGE_TYPES:
            errors.append(
                f"--storage-type must be one of: {', '.join(sorted(self._VALID_STORAGE_TYPES))} "
                f"(got '{self.storage_type}')"
            )
        if self.storage_type is not None and self.target:
            try:
                scheme = infer_target_protocol(self.target)
            except ValueError as exc:
                errors.append(str(exc))
            else:
                expected_storage_type = SCHEME_TO_STORAGE_TYPE.get(scheme)
                if expected_storage_type is None:
                    errors.append(
                        f"Target URI scheme '{scheme}' is not supported. Use a file:// URI for "
                        "--storage-type local, or an s3:// URI for --storage-type s3."
                    )
                elif expected_storage_type != self.storage_type:
                    alternate_target = (
                        "an s3://-compatible URI" if self.storage_type == "s3" else "a file:// URI"
                    )
                    errors.append(
                        f"Target URI scheme '{scheme}' is incompatible with --storage-type "
                        f"'{self.storage_type}'. Use --storage-type {expected_storage_type} "
                        f"for {scheme}:// targets, or change --target to {alternate_target}."
                    )

        if (
            self.storage_driver is not None
            and self.storage_driver not in self._VALID_STORAGE_DRIVERS
        ):
            errors.append(
                f"--storage-driver must be one of: {', '.join(sorted(self._VALID_STORAGE_DRIVERS))} "
                f"(got '{self.storage_driver}')"
            )

        if errors:
            raise click.UsageError(
                "Invalid ingest configuration:\n" + "\n".join(f"  - {error}" for error in errors)
            )
