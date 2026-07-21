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

"""TensogramWriteStrategy — batch dataset encoder to Tensogram .tgm files."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr

from firecube.core.tensogram._compat import require_tensogram
from firecube.core.tensogram.converter import _find_time_dim
from firecube.core.tensogram.metadata import (
    dataset_to_global_meta,
    prepare_array_for_encoding,
    variable_to_base_entry,
    variable_to_descriptor,
)


class TensogramWriteStrategy:
    """Write strategy that encodes xarray Datasets to a Tensogram .tgm file.

    One TensogramWriteStrategy instance = one .tgm file. Each write_groups()
    call appends a new message to the file. No append semantics — if the target
    already exists, it is overwritten on first write.
    """

    def __init__(
        self,
        *,
        target: str,
        compression: str = "zstd",
        source_uri: str = "",
        allow_nan: bool = True,
        allow_inf: bool = True,
        time_dim_name: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._target = Path(target)  # firecube: STORAGE-URI
        self._compression = compression
        self._source_uri = source_uri
        self._allow_nan = allow_nan
        self._allow_inf = allow_inf
        self._time_dim_name = time_dim_name
        self._logger = logger or logging.getLogger(__name__)
        self._file_context: Any = None
        self._file: Any = None

    def _get_or_create_file(self) -> Any:
        """Open or create the .tgm file on first write."""
        if self._file is None:
            require_tensogram("TensogramWriteStrategy")
            import tensogram  # pyright: ignore[reportMissingImports]

            self._target.parent.mkdir(parents=True, exist_ok=True)
            if self._target.exists():
                self._target.unlink()
            self._file_context = tensogram.TensogramFile.create(  # pyright: ignore[reportAttributeAccessIssue]
                str(self._target)
            )
            self._file = self._file_context.__enter__()
        return self._file

    def write(self, ds: xr.Dataset) -> dict[str, Any]:
        """Encode a single xarray Dataset as one Tensogram message."""
        require_tensogram("TensogramWriteStrategy.write")

        tensogram_file = self._get_or_create_file()

        objects: list[tuple[dict[str, Any], np.ndarray]] = []
        base: list[dict[str, Any]] = []
        for var_name in ds.data_vars:
            variable = ds[var_name].variable
            base.append(variable_to_base_entry(str(var_name), variable))
            objects.append(
                (
                    variable_to_descriptor(
                        str(var_name),
                        variable,
                        compression=self._compression,
                    ),
                    prepare_array_for_encoding(variable),
                )
            )

        for coord_name in ds.coords:
            coordinate = ds.coords[coord_name].variable
            base.append(variable_to_base_entry(str(coord_name), coordinate))
            objects.append(
                (
                    variable_to_descriptor(
                        str(coord_name),
                        coordinate,
                        compression="none",
                    ),
                    prepare_array_for_encoding(coordinate),
                )
            )

        global_meta = dataset_to_global_meta(
            ds,
            source_uri=self._source_uri,
            compression=self._compression,
            base=base,
        )
        global_meta["firecube"]["coordinates"] = list(ds.coords)
        time_dim = _find_time_dim(ds, preferred_time_dim=self._time_dim_name)
        if time_dim is not None:
            global_meta["firecube"]["time_dim"] = time_dim

        tensogram_file.append(
            global_meta, objects, allow_nan=self._allow_nan, allow_inf=self._allow_inf
        )
        self._logger.debug("Appended message to %s: %s", self._target, list(ds.data_vars))

        return {"variables": list(ds.data_vars), "message_appended": True}

    def write_groups(
        self,
        *,
        group_to_timestamps: Mapping[str, Sequence[Any]],
        dataset_for_batch: Callable[[str, Sequence[Any]], xr.Dataset | None],
        batch_size: int,
        claim_for_group: Callable[[str], Any] | None = None,
    ) -> dict[str, Any]:
        """Write all groups from the batch to the .tgm file."""
        metrics: dict[str, Any] = {"messages_written": 0, "variables_written": 0}
        for group_name, timestamps in group_to_timestamps.items():
            ds = dataset_for_batch(group_name, list(timestamps))
            if ds is None:
                continue

            result = self.write(ds)
            metrics["messages_written"] += 1
            metrics["variables_written"] += len(result.get("variables", []))

        return metrics

    def close(self) -> None:
        """Close the .tgm file handle."""
        if self._file is not None:
            self._file_context.__exit__(None, None, None)
            self._file_context = None
            self._file = None

    def __del__(self) -> None:
        self.close()
