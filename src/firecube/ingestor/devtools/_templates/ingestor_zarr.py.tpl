# FIRECUBE_TEMPLATE_LICENSE_HEADER_BEGIN
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
# FIRECUBE_TEMPLATE_LICENSE_HEADER_END
{copyright_header}

"""Generic Zarr ingestor for {plugin_name}.

Set ``TIME_DIM`` and implement ``read_dataset``. Only the reader knows the source
format: ``build_dataset`` reads each source item of a batch with it and appends
the batch to the store along ``TIME_DIM``.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any, ClassVar

import xarray as xr
from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginContext,
    ZarrTemplateConfig,
    register_ingestor,
)

# The name of the time dimension in the datasets read_dataset() returns, for
# example "time". Firecube appends each batch to the store along it, and the
# store keeps this name. Ingestion stops with NotImplementedError until it is set.
TIME_DIM = ""

# Reading your source data, by situation:
#   Which files Firecube finds under --input-data, and adding other formats:
#     https://eumetsat.github.io/firecube/latest/guides/plugins/source-discovery/
#   Data inside .zip archives:
#     https://eumetsat.github.io/firecube/latest/guides/plugins/discover-zipped-data/
#   Times or other values that exist only in the file name:
#     https://eumetsat.github.io/firecube/latest/guides/plugins/parse-filename-fields/
#   Two files per source item, such as data plus metadata, passed as a tuple:
#     https://eumetsat.github.io/firecube/latest/guides/plugins/paired-source-files/
#   The whole GenericZarrIngestor workflow, from reader to verified store:
#     https://eumetsat.github.io/firecube/latest/guides/plugins/generic-zarr/


def read_dataset(path: Path) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension.

    ``path`` is a local file: ``build_dataset`` downloads remote items first. Load
    the data into memory before the file closes, for example with ``.load()``.
    For other formats, add the library that reads them to ``dependencies`` in
    ``pyproject.toml``.
    """
    # For NetCDF files the reader can be this small; Firecube already installs
    # the NetCDF libraries.
    # with xr.open_dataset(path) as dataset:
    #     return dataset.load()
    raise NotImplementedError(
        f"read_dataset() is not implemented (called for {{path}}). Open the file and "
        "return an xarray.Dataset with a TIME_DIM dimension."
    )


@dataclasses.dataclass
class ZarrStorageConfig(ZarrTemplateConfig):
    """Storage defaults for the Zarr store this plugin writes.

    Nothing is overridden, so Firecube's defaults apply: Zarr picks the chunk
    shape, arrays are compressed with zstd, and nothing is sharded. Uncomment a
    setting to make it this plugin's default. An operator's ``--option`` or config
    file still wins. Decide chunking before the first real run, because later
    appends must match the chunk shape already on disk. See the Firecube guides
    "Configure Zarr Chunking", "Configure Zarr Compression", and "Configure Zarr
    Sharding".
    """

    # Chunk length per dimension of the dataset build_dataset returns.
    # zarr_chunk_shape: dict[str, int] | None = dataclasses.field(
    #     default_factory=lambda: {{"timestamp": 64, "lat": 180, "lon": 360}}
    # )

    # Store arrays uncompressed:
    # zarr_compression: bool = False

    # Or keep compression and choose the codecs. Every codec needs both "name" and
    # "configuration"; use {{}} for the codec's own defaults.
    # zarr_codecs: list[dict] | None = dataclasses.field(
    #     default_factory=lambda: [{{"name": "zstd", "configuration": {{"level": 3}}}}]
    # )

    # Store many chunks in one file. Set zarr_chunk_shape too: every shard
    # length must be a whole number of chunks.
    # zarr_sharding: bool = True
    # zarr_shard_shape: dict[str, int] | None = dataclasses.field(
    #     default_factory=lambda: {{"timestamp": 512, "lat": 180, "lon": 360}}
    # )


@register_ingestor("{plugin_name}")
class {class_name}(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    time_dim_name: ClassVar[str] = TIME_DIM
    template_config_class = ZarrStorageConfig
    # To accept your own ``--option key=value`` flags, attach a PluginConfig
    # subclass; see the Firecube "Add Plugin Configuration Options" guide.

    def build_dataset(
        self,
        group: str,  # Called once per output group; most plugins ignore this.
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        """Return one batch of source files as a single dataset, or ``None`` to skip it.

        Firecube calls this once per batch, in input order, and appends the
        returned dataset to the store before asking for the next batch. ``items``
        holds up to ``pipeline_batch_size`` files (10 by default). If this raises,
        the run stops at this batch; batches appended before it stay written.
        The dataset must be sorted along ``TIME_DIM`` with no repeated timestamps.
        When the reader needs more than a local path, change only the
        ``read_dataset(...)`` call below; the guides linked above show how.
        """
        _ = group
        if not items:
            return None
        if not self.time_dim_name:
            raise NotImplementedError(
                "TIME_DIM is not set. Set it at the top of ingestor.py to the time "
                "dimension of the datasets read_dataset() returns, for example 'time'."
            )

        datasets = [read_dataset(ctx.materialize(item)) for item in items]
        for dataset in datasets:
            if self.time_dim_name not in dataset.dims:
                raise ValueError(
                    f"read_dataset() returned dimensions {{sorted(map(str, dataset.dims))}} "
                    f"without TIME_DIM {{self.time_dim_name!r}}. Set TIME_DIM to the time "
                    "dimension the files use, or rename it in read_dataset()."
                )
        dataset = xr.concat(
            datasets,
            dim=self.time_dim_name,
            # variables without TIME_DIM are stored once, not copied per step
            data_vars="minimal",
            # same for coordinates like bounds that get decoded as coords
            coords="minimal",
            # static values must match across files in one batch; use "override" to keep the first without checking
            compat="equals",
            # grids must match; use "override" to take the first file's grid or "outer" for a padded union
            join="exact",
        )
        return dataset.sortby(self.time_dim_name)
