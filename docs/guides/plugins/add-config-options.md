# Add Plugin Configuration Options

## Goal

Declare product-specific options that Firecube validates before calling plugin
hooks.

## Add Configuration to an Existing Plugin

Define the configuration next to an already working plugin:

```python
from dataclasses import dataclass

from firecube.ingestor.api import PluginConfig


@dataclass
class MyConfig(PluginConfig):
    scale_factor: float = 1.0
```

Then add the configuration class to the ingestor:

```python
plugin_config_class = MyConfig
```

Inside a plugin hook, read the validated instance:

```python
config = self.plugin_config
assert isinstance(config, MyConfig)
scale_factor = config.scale_factor
```

`@dataclass` is required when a `PluginConfig` subclass adds fields.

Use `self.plugin_config` for declared product fields. Do not treat
`ctx.option()` as a separate CLI-only configuration channel.

Use `ctx.option(key, default)` only for effective engine settings or
experimental `x_*` values. `ctx.options` exposes the same effective values as a
read-only mapping. See the
[Configuration Reference](../../reference/config.md#pluginconfig) for the
public configuration types.

## Full Example

One complete plugin with one declared option. The config class declares
`scale_factor`, the ingestor attaches it, and the hook applies the validated
value:

```python
from dataclasses import dataclass
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginConfig,
    PluginContext,
    register_ingestor,
)


@dataclass
class MyConfig(PluginConfig):
    scale_factor: float = 1.0


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"
    time_dim_name: ClassVar[str] = "time"
    plugin_config_class = MyConfig

    def build_dataset(
        self, group: str, items: list[Any], ctx: PluginContext
    ) -> xr.Dataset | None:
        if not items:
            return None

        config = self.plugin_config
        assert isinstance(config, MyConfig)

        paths = [ctx.materialize(item) for item in items]
        dataset = xr.open_mfdataset(paths, combine="by_coords")
        return (dataset * config.scale_factor).sortby(self.time_dim_name)
```

This is the [GenericZarrIngestor guide's example](generic-zarr.md#implement-build_dataset)
plus the three configuration pieces: the `MyConfig` declaration, the
`plugin_config_class` attachment, and the validated read inside the hook. The
same three pieces work unchanged on any other ingestor template.

## Set Configuration Values

Set defaults in the plugin section of a config file:

```toml
[plugins.my_plugin]
scale_factor = 0.01
```

Override declared fields for one run with repeatable `--option` flags:

```bash
firecube ingest my_plugin \
  --input-data ./sample-input \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option scale_factor=0.01
```

Unknown keys fail during configuration. Options in the `x_*` namespace bypass
the declared tiers and are intended for experimental plugin behavior.

Keep product identity, target, storage driver, output format, and write mode in
their dedicated command flags. Use `--option` only for declared plugin or engine
settings.

## Verify

```bash
firecube plugins describe my_plugin
firecube ingest my_plugin --show-options
```

Confirm that the declared fields and defaults appear before running a small
ingestion with one overridden value.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Adding fields without `@dataclass` | Decorate the `PluginConfig` subclass. |
| Reading a declared field only from `ctx.option()` | Read it from the validated `self.plugin_config`. |
| Using `batch_size` | Use the engine option `pipeline_batch_size`. |
| Parsing `ctx.target` to choose a storage driver | Let the runtime resolve the storage binding. |

## Next Steps

- **[Configuration Model](../../concepts/configuration.md)** — understand how configuration is resolved
- **[Configuration Reference](../../reference/config.md)** — look up supported keys and precedence
- **[CLI Reference](../../reference/cli.md)** — inspect the complete command surface
