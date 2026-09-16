# Configure Zarr Chunking

## Goal

Set the chunk shape of the arrays your plugin writes. Chunking decides how
much data one read has to fetch. Decide it before the first real run: a
written cube keeps its chunk shape, and appends must match it, so changing
it later means writing a new cube. Without a setting Zarr picks a shape
itself (for a `(2, 720, 1440)` array it chose `(1, 180, 720)`).

| Template | Where the chunk shape comes from |
|---|---|
| `GenericZarrIngestor` | the `zarr_chunk_shape` option |
| `DirectZarrIngestor` | `chunks=` in each `ZarrArraySpec`; the option has no effect |

## On The Command Line

The value is a JSON object keyed by dimension name, so quote the option:

```bash
firecube ingest my_plugin \
  --input-data /data/raw \
  --target "file://$PWD/out.zarr" \
  --product-name my_product \
  --write-mode direct \
  --option 'zarr_chunk_shape={"time":5,"lat":90,"lon":180}'
```

## In A Config File

```toml
[plugins.my_plugin]
zarr_chunk_shape = { time = 5, lat = 90, lon = 180 }
```

```bash
firecube --config-file firecube.toml ingest my_plugin ...
```

A `--option` on the command line overrides the file.

## As The Plugin's Default

Subclass the template config and attach it. An operator's `--option` still
wins:

```python
from dataclasses import dataclass, field

from firecube.ingestor.api import GenericZarrIngestor, ZarrTemplateConfig


@dataclass
class MyZarrConfig(ZarrTemplateConfig):
    zarr_chunk_shape: dict[str, int] | None = field(
        default_factory=lambda: {"time": 64, "lat": 180, "lon": 360}
    )


class MyPlugin(GenericZarrIngestor):
    template_config_class = MyZarrConfig
    ...
```

## Chosen At Run Time

`GenericZarrIngestor` only. When the plugin offers more than one layout,
override `get_zarr_config`. Start from the operator's options and fill only
what they did not set:

```python
LAYOUTS = {
    "maps": {"chunk_shape": {"time": 64, "lat": 180, "lon": 360}},
    "timeseries": {"chunk_shape": {"time": 512, "lat": 15, "lon": 15}},
}


class MyPlugin(GenericZarrIngestor):
    def get_zarr_config(self, ctx):
        zarr_config = super().get_zarr_config(ctx)  # the operator's zarr_* options
        for key, value in LAYOUTS[self.plugin_config.layout].items():
            if f"zarr_{key}" not in ctx.options:
                zarr_config[key] = value
        return zarr_config
```

The returned key is the writer's name, `chunk_shape`, not the option name.
Returning `{**super().get_zarr_config(ctx), **preset}` would silently discard
what the operator passed.

## In A `DirectZarrIngestor` Schema

```python
ZarrArraySpec(
    name="value",
    shape=(n_times, 4),
    dtype=np.float32,
    chunks=(24, 4),
    dimension_names=("timestamp", "sample"),
)
```

## Verify

```python
import zarr

array = zarr.open_group("out.zarr", mode="r", use_consolidated=False)["default"]["precipitation"]
print(array.chunks)
```

```text
(5, 90, 180)
```

## Common Mistakes

| Mistake | Fix |
|---|---|
| `--option zarr_chunk_shape={"time":5}` without quotes | The shell eats the braces. Quote the whole option. |
| A dimension name the dataset does not have | Use the dataset's own names, for example `latitude` rather than `lat`. |
| `zarr_chunk_shape` on a `DirectZarrIngestor` plugin | It has no effect there. Set `chunks=` in the `ZarrArraySpec`. |
| `get_zarr_config` returns the preset over the operator's options | Fill only the keys absent from `ctx.options`, as above. |

## Next Steps

- **[Configure Zarr Sharding](configure-zarr-sharding.md)** — many chunks in one file
- **[Configure Zarr Compression](configure-zarr-compression.md)** — codecs and levels
- **[Performance Tuning](../../concepts/performance.md)** — choosing chunk sizes for the reads you expect
