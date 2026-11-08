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

from firecube.cli._errors import MissingProductNameError
from firecube.cli._input_filters import load_plugin_defaults
from firecube.cli._uri_policy import parse_and_validate_uri, validate_uri_storage_coherence
from firecube.core.controlplane import ChunkManager
from firecube.core.errors import Message, ProductLabelMismatchError
from firecube.core.storage.binding import StorageBinding


def check_recorded_label(binding: StorageBinding) -> None:
    """Refuse the bound label when the store records another, before the command works.

    Reads through a `ChunkManager` bound like the command's own. Writes nothing and creates no
    directory: reads resolve through the binding, so the manager gets the current directory as its
    workspace instead of a temporary one. The first control-plane write refuses the same label
    later, after the workspace and the source discovery.

    Raises:
        ProductLabelMismatchError: The store records another label.
        ManifestError: The stamp or a control-plane record cannot be read.
        ProductLabelConflictError: An unstamped store's records name several labels.
    """
    manager = ChunkManager(binding=binding, workspace=Path.cwd())
    try:
        recorded = manager.recorded_product_label()
    finally:
        manager.close()
    given = binding.identity.product_name
    if recorded is not None and recorded != given:
        raise ProductLabelMismatchError(
            Message(
                "Product label {given!r} is refused: the product at {product_uri} records "
                "the label {product!r}.",
                given=given,
                product=recorded,
                product_uri=binding.identity.product_uri.to_str(),
            ),
            given=given,
            recorded=recorded,
        )


def resolve_product_name(
    ctx: click.Context, plugin: str, ingestor_cls: type, given: str | None
) -> str:
    """Return the product label a write command offers to the store.

    Precedence: `--product-name`, the plugin's `default_product_name` config key, then the
    plugin class `PRODUCT_NAME`. The store refuses a label that differs from the one it records.

    Raises:
        click.UsageError: The flag is empty, no source names a label, or the config is unreadable.
    """
    if given == "":
        raise click.UsageError(
            "--product-name was provided but is empty. Either omit it (use plugin "
            "PRODUCT_NAME or config default_product_name), or provide a non-empty value."
        )
    name = (
        given
        or load_plugin_defaults(ctx, plugin).get("default_product_name")
        or getattr(ingestor_cls, "PRODUCT_NAME", None)
    )
    if not name:
        raise MissingProductNameError(plugin)
    return str(name)


@dataclass(slots=True)
class IngestCommandConfig:
    """Validated ingest command configuration.

    Uses aggregate validation to report all missing/invalid command-level values at once.
    """

    plugin: str
    input_data: tuple[str, ...] | str | Path | None
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
                "--write-mode is required (not inferred from the URI scheme).\n"
                "  --write-mode staged    write to workspace first, then upload to target\n"
                "  --write-mode direct    stream directly to target (no local copy)\n\n"
                "For local targets (file://): use --write-mode direct\n"
                "For S3 targets (s3://): use --write-mode staged (uploads after processing)"
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
        if self.target:
            try:
                uri = parse_and_validate_uri(self.target, param_hint="--target")
            except click.UsageError as exc:
                errors.append(exc.format_message())
            else:
                if self.storage_type is not None:
                    try:
                        validate_uri_storage_coherence(uri, self.storage_type)
                    except click.UsageError as exc:
                        errors.append(exc.format_message())

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
