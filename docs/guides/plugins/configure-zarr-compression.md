# Configure Zarr Compression

## Goal

Choose whether and how the arrays are compressed. The default is `zstd`. The
same options reach `GenericZarrIngestor` and `DirectZarrIngestor`.

## Turn Compression Off

```bash
firecube ingest my_plugin \
  --input-data /data/raw \
  --target "file://$PWD/out.zarr" \
  --product-name my_product \
  --write-mode direct \
  --option zarr_compression=false
```

## Choose A Codec And Level

```bash
firecube ingest my_plugin ... \
  --option 'zarr_codecs=[{"name":"zstd","configuration":{"level":3}}]'
```

The value is a JSON list of Zarr v3 codec objects, each with a `name` and an
optional `configuration`.

## In A Config File

```toml
[plugins.my_plugin]
zarr_compression = false
```

or

```toml
[plugins.my_plugin]
zarr_codecs = [{ name = "zstd", configuration = { level = 3 } }]
```

## As The Plugin's Default

```python
from dataclasses import dataclass

from firecube.ingestor.api import GenericZarrIngestor, ZarrTemplateConfig


@dataclass
class MyZarrConfig(ZarrTemplateConfig):
    zarr_compression: bool = False


class MyPlugin(GenericZarrIngestor):
    template_config_class = MyZarrConfig
    ...
```

## Per Array In A `DirectZarrIngestor` Schema

The options above set the default for every array. One array can override
them in its spec:

```python
ZarrArraySpec(
    name="mask",
    shape=(n_times, 4),
    dtype=np.uint8,
    chunks=(24, 4),
    dimension_names=("timestamp", "sample"),
    compressors=(),  # this array uncompressed
)
```

```python
ZarrArraySpec(
    name="value",
    shape=(n_times, 4),
    dtype=np.float32,
    chunks=(24, 4),
    dimension_names=("timestamp", "sample"),
    compressors=({"name": "zstd", "configuration": {"level": 3}},),
)
```

`compressors=None` (the default) inherits the options.

## Verify

```python
import zarr

array = zarr.open_group("out.zarr", mode="r", use_consolidated=False)["default"]["precipitation"]
print([c.to_dict() for c in array.metadata.codecs])
```

Default:

```text
[{'name': 'bytes', 'configuration': {'endian': 'little'}}, {'name': 'zstd', 'configuration': {'level': 0, 'checksum': False}}]
```

With `zarr_compression=false`:

```text
[{'name': 'bytes', 'configuration': {'endian': 'little'}}]
```

## Common Mistakes

| Mistake | Fix |
|---|---|
| `zarr_codecs=zstd` | The value is a JSON list of objects; see above. |
| `--option zarr_codecs=[...]` without quotes | The shell eats the brackets. Quote the whole option. |
| Both `zarr_compression=false` and `zarr_codecs` | Refused: `zarr_compression=False conflicts with zarr_codecs`. Pick one. |

## Next Steps

- **[Configure Zarr Chunking](configure-zarr-chunking.md)** — the chunk shape
- **[Configure Zarr Sharding](configure-zarr-sharding.md)** — many chunks in one file
- **[`ZarrTemplateConfig`](../../reference/config.md#zarrtemplateconfig)** — every Zarr option and its default
