# Plugin Registry

Published Firecube plugins. Each plugin is a separately installed Python
package; see its repository for installation and usage.

| Plugin | Repository | Description |
|---|---|---|
| `firecube-quickstart-plugin` | [eumetsat/firecube-quickstart-plugin](https://github.com/eumetsat/firecube-quickstart-plugin) | Minimal worked example that converts time-indexed NetCDF files into a Zarr product. Ships its own sample-data generator. Used in the [Quickstart](../quickstart/index.md). |
| `firecube-mtg-fci-l1c` | [eumetsat/firecube-mtg-fci-l1c](https://github.com/eumetsat/firecube-mtg-fci-l1c) | Ingests MTG FCI Level 1C FDHSI and HRFI products into direct-region Zarr stores. |

## Add Your Plugin

To list a plugin here, open a pull request that adds a row to this table.
See [Package and Register](../guides/plugins/contract.md) for how to publish a
plugin.
