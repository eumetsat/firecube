# Create and Install the Example Plugin

## When To Use This

A Firecube plugin is a separately installed Python package that tells Firecube
how to discover, read, and convert a particular dataset. Firecube runs the
ingestion workflow and manages the output product.

This page creates a local `weather_netcdf` plugin for the quickstart. It reads
the example NetCDF files and returns one ordered `xarray.Dataset`; Firecube
writes that dataset to Zarr.

## Prerequisites

- Firecube installed in a Python environment.
- The environment activated with `source .venv/bin/activate`.
- The current directory is `firecube-quickstart/`.

## Steps

### Create The Plugin

Create a plugin project from Firecube's Zarr template:

```bash
mkdir -p plugins_dev
firecube plugins create weather-netcdf \
  --template zarr \
  --target-dir plugins_dev \
  --non-interactive
```

Expected output starts with:

```text
✨ Created plugin project: plugins_dev/firecube-weather-netcdf
```

The generated package registers the plugin as `weather_netcdf`. Its Zarr
template handles discovery, batching, and output writes, but its dataset
conversion deliberately fails until you implement it.

### Implement The Plugin

Open
`plugins_dev/firecube-weather-netcdf/src/firecube_weather_netcdf/ingestor.py`.
Replace the generated `build_dataset` method with:

```python
    def build_dataset(
        self,
        group: str,
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        _ = group
        if not items:
            return None

        datasets: list[xr.Dataset] = []
        for item in items:
            path = ctx.materialize(item)
            with xr.open_dataset(path) as source:
                datasets.append(source.load())

        return xr.concat(datasets, dim="timestamp").sortby("timestamp")
```

`ctx.materialize(item)` gives `xarray` a local path for each source item.
`source.load()` keeps the values available after each NetCDF file closes. The
final line combines the files and orders them by timestamp.

### Install The Plugin

Install the generated project into the active environment in editable mode:

```bash
firecube plugins install --editable plugins_dev/firecube-weather-netcdf
```

Editable installation means later changes to the plugin source take effect
without reinstalling it.

Inspect the registered plugin and its options:

```bash
firecube plugins list
firecube plugins describe weather_netcdf
firecube ingest weather_netcdf --show-options
```

`plugins list` should contain `weather_netcdf`. `plugins describe` should show
`weather_netcdf` as both the plugin and product name.

## Verify

Run the plugin inspection commands above. They should complete without an
import error, and `--show-options` should list the engine, storage, and Zarr
options accepted by the plugin.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Plugin is missing from `plugins list` | The package was installed in a different environment. | Run `source .venv/bin/activate`, then repeat the editable install. |
| `build_dataset` raises `NotImplementedError` | The generated stub is still present. | Replace the method with the implementation above, then run the command again. |
| Plugin import fails | The method was pasted outside `WeatherNetcdfIngestor` or has invalid indentation. | Put the method inside the generated class and keep its four-space class indentation. |
| `uv` cannot find a Python environment | The quickstart environment is not active. | Run `source .venv/bin/activate` from `firecube-quickstart/`. |

## Next Steps

- **[Prepare Source Data](source-data.md)**: create the NetCDF files used by the
  local ingestion example.
- **[Plugin Development](../guides/plugins/index.md)**: choose a template and
  develop a plugin for your own dataset after completing the quickstart.
