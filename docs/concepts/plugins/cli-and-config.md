# Plugin CLI And Config

## Goal

Explain how the operator's CLI flags and config file settings reach your plugin.
After reading this page you'll know how to declare typed plugin config, read
per-run options from the context, and understand why your plugin must never
inspect the target URI to decide how to write.

## Minimal Example

A plugin that declares typed config and reads a per-run option:

```python
from typing import ClassVar

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginConfig,
    PluginContext,
    register_ingestor,
)


class MyConfig(PluginConfig):
    region: str = "Euro"


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"
    plugin_config_class: ClassVar[type[PluginConfig]] = MyConfig

    def build_dataset(self, group: str, items: list[object], ctx: PluginContext):
        region = self.plugin_config.region          # from [plugins.my_plugin] in config.toml
        batch_size = ctx.option("batch_size", default=10)  # from --option batch_size=N
        ...
```

The engine validates `MyConfig` against `[plugins.my_plugin]` in `config.toml`,
then sets `self.plugin_config` before calling any hook. `ctx.option()` reads
values passed via `--option key=value` on the command line.

## Required API

| API | Kind | Purpose |
|-----|------|---------|
| `PRODUCT_NAME` | `ClassVar[str]` | Logical product identity. Required on every concrete ingestor. |
| `plugin_config_class` | `ClassVar[type[PluginConfig]]` | Declares the typed config class. Engine validates and injects it. |
| `self.plugin_config` | Instance attr | Populated by the engine before hooks run. Type matches your `plugin_config_class`. |
| `ctx.options` | `dict[str, Any]` | All `--option key=value` pairs for this run, as a read-only mapping. |
| `ctx.option(key, default)` | Method | Reads one option with a fallback. Prefer this over `ctx.options[key]`. |
| `--option key=value` | CLI flag | Passes a per-run option. Repeatable. Maps into `ctx.options`. |

## Configuration

### Required CLI flags

Every `firecube ingest` call passes these flags explicitly. The engine does not
infer them from the target URI or file path:

```bash
firecube ingest my_plugin \
  --source /data/input \
  --target s3://bucket/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type s3 \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged \
  --option batch_size=20
```

| Flag | What it controls |
|------|-----------------|
| `--source` | Input path or URI the plugin reads from. |
| `--target` | Output URI where the product is written. |
| `--product-name` | Logical product name for bookkeeping and telemetry. |
| `--storage-type` | `local` or `s3`. Never inferred from the URI scheme. |
| `--storage-driver` | `fsspec` or `obstore`. Never inferred. |
| `--output-format` | `zarr`, `parquet`, or plugin-specific format. |
| `--write-mode` | `staged` or `direct`. Never inferred from locality. |
| `--option key=value` | Per-run plugin or engine option. Repeatable. |

### Product name precedence

The engine resolves the product name in this order and fails if none is found:

1. `--product-name` on the CLI
2. `default_product_name` in `[plugins.<name>]` in `config.toml`
3. `PRODUCT_NAME` declared on the plugin class
4. Hard fail

### Typed plugin config

Declare a `PluginConfig` subclass and assign it to `plugin_config_class`. The
engine reads `[plugins.<name>]` from `config.toml`, validates the values against
your class, and sets `self.plugin_config` before any hook runs:

```toml
# ~/.config/firecube/config.toml
[plugins.my_plugin]
region = "Euro"
default_product_name = "MY_PRODUCT"
```

```python
class MyConfig(PluginConfig):
    region: str = "Euro"

class MyPlugin(GenericZarrIngestor):
    plugin_config_class: ClassVar[type[PluginConfig]] = MyConfig

    def build_dataset(self, group, items, ctx):
        print(self.plugin_config.region)  # "Euro" or whatever the operator set
```

If you don't declare `plugin_config_class`, `self.plugin_config` is `None`.

### Per-run options

`--option key=value` passes ephemeral options for a single run. Read them
through the context, not through `self.plugin_config`:

```python
def build_dataset(self, group, items, ctx):
    batch_size = ctx.option("batch_size", default=10)
    all_opts = ctx.options  # read-only dict of all --option pairs
```

Options passed via `--option` are validated against the active option tiers.
Unknown keys fail immediately.

Options with the `x_` prefix (e.g. `--option x_foo=1`) bypass tier validation
and are passed through to plugins untyped. Use them only for in-development
plugin features not yet promoted to a declared option tier.

## Test It

Run locally with a minimal config to confirm your plugin reads config and options
correctly:

```bash
firecube ingest my_plugin \
  --source /tmp/test-input \
  --target /tmp/test-output/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option batch_size=5
```

Inspect the effective option set before running:

```bash
firecube ingest my_plugin --show-options
firecube plugins describe my_plugin
```

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Parsing `ctx.target` to detect S3 and choose a write path | Don't. The operator controls storage via `--storage-type`, `--storage-driver`, and `--write-mode`. Your plugin must not inspect the URI. |
| Hardcoding `PRODUCT_NAME` and ignoring the CLI | The engine resolves precedence: CLI `--product-name` wins. Declare `PRODUCT_NAME` as the fallback, not the authority. |
| Storing plugin options in a plain dict instead of `PluginConfig` | Declare `plugin_config_class` so the engine validates types and rejects unknown keys at startup. |
| Calling a `.config.plugin` property on the context | No such attribute exists on `PluginContext`. Use `self.plugin_config` for typed config values and `ctx.option(key, default)` for per-run options. |
| Omitting `plugin_config_class` and reading `self.plugin_config` | `self.plugin_config` is `None` unless you declare `plugin_config_class`. |

## Next Steps

- **[Plugin Contract](contract.md)** — required methods, typed results, and storage access rules
- **[Configuration Model](../configuration.md)** — full config tier model and option precedence
- **[CLI Reference](../../reference/cli.md)** — complete flag list for `firecube ingest`
- **[Config Reference](../../reference/config.md)** — generated schema for `config.toml`
