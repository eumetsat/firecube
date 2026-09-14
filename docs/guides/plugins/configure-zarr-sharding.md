# Configure Zarr Sharding

## Goal

Store many small chunks inside one larger file. Sharding keeps small chunks
for reads while keeping the number of files down. It needs a chunk shape and
a shard shape; the shard shape must be a multiple of the chunk shape.

| Template | Where the shard shape comes from |
|---|---|
| `GenericZarrIngestor` | the `zarr_sharding` and `zarr_shard_shape` options |
| `DirectZarrIngestor` | `shards=` in each `ZarrArraySpec`; the options have no effect |

## On The Command Line

```bash
firecube ingest my_plugin \
  --input-data /data/raw \
  --target "file://$PWD/out.zarr" \
  --product-name my_product \
  --write-mode direct \
  --option zarr_sharding=true \
  --option 'zarr_chunk_shape={"time":5,"lat":90,"lon":180}' \
  --option 'zarr_shard_shape={"time":10,"lat":180,"lon":360}'
```

`zarr_sharding=true` with a chunk shape and no shard shape is refused before
anything is written:

```text
Error: zarr_sharding=true requires zarr_shard_shape when zarr_chunk_shape is set.
```

## In A Config File

```toml
[plugins.my_plugin]
zarr_sharding = true
zarr_chunk_shape = { time = 5, lat = 90, lon = 180 }
zarr_shard_shape = { time = 10, lat = 180, lon = 360 }
```

## As The Plugin's Default

```python
from dataclasses import dataclass, field

from firecube.ingestor.api import GenericZarrIngestor, ZarrTemplateConfig


@dataclass
class MyZarrConfig(ZarrTemplateConfig):
    zarr_sharding: bool = True
    zarr_chunk_shape: dict[str, int] | None = field(
        default_factory=lambda: {"time": 512, "lat": 15, "lon": 15}
    )
    zarr_shard_shape: dict[str, int] | None = field(
        default_factory=lambda: {"time": 4096, "lat": 15, "lon": 15}
    )


class MyPlugin(GenericZarrIngestor):
    template_config_class = MyZarrConfig
    ...
```

## In A `DirectZarrIngestor` Schema

```python
ZarrArraySpec(
    name="value",
    shape=(n_times, 4),
    dtype=np.float32,
    chunks=(24, 4),
    shards=(240, 4),
    dimension_names=("timestamp", "sample"),
)
```

## Verify

```python
import zarr

array = zarr.open_group("out.zarr", mode="r", use_consolidated=False)["default"]["precipitation"]
print(array.chunks, array.shards)
```

```text
(5, 90, 180) (10, 180, 360)
```

`array.shards` is `None` when the array is not sharded.

## Common Mistakes

| Mistake | Fix |
|---|---|
| `zarr_sharding=true` without `zarr_shard_shape` | Pass both shapes; the run is refused before writing. |
| Shard shape not a multiple of the chunk shape | Make every shard dimension a whole number of chunks. |
| Sharding options on a `DirectZarrIngestor` plugin | They have no effect there. Set `shards=` in the `ZarrArraySpec`. |

## Next Steps

- **[Configure Zarr Chunking](configure-zarr-chunking.md)** — the chunk shape sharding builds on
- **[Configure Zarr Compression](configure-zarr-compression.md)** — codecs and levels
