# NetCDF To Zarr: Observability

## Goal

Add one domain-specific metric to the plugin from the first tutorial, run
ingestion with a Prometheus Pushgateway, and inspect the exported metric.

Continue from [NetCDF To Zarr](weather-netcdf.md). You will keep the same
plugin and add one metric through `ctx.telemetry`.

## Prerequisites

- The completed [Quickstart](../quickstart/index.md) project in the current
  directory.
- Docker available with local port `9091` free.

## Start A Pushgateway

This tutorial uses Docker to run a temporary local Pushgateway:

```bash
docker run --rm -d \
  --name firecube-tutorial-pushgateway \
  -p 9091:9091 \
  prom/pushgateway:v1.11.1
```

Set its URL for the remaining commands:

```bash
export FIRECUBE_PUSHGATEWAY_URL="http://localhost:9091"
```

Confirm that it is ready:

```bash
curl -s "$FIRECUBE_PUSHGATEWAY_URL/-/ready"
```

Expected output:

```text
OK
```

## Add One Metric

In `plugins_dev/firecube-weather-netcdf/src/firecube_weather_netcdf/ingestor.py`,
add this block near the end of `build_dataset`, just before `return result`:

```python
        if ctx.telemetry is not None:
            ctx.telemetry.emit(
                "weather_netcdf_files",
                len(items),
                kind="counter",
                meta={"group": group, "status": "success"},
            )
```

`counter` is the right kind here because the value is a count that increases
during the run. Firecube exports it as:

```text
firecube_weather_netcdf_files_total
```

## Run The Plugin

```bash
PRODUCT_URI="file://$PWD/quickstart-output/weather_netcdf_observed.zarr"

firecube ingest weather_netcdf \
  --input-data quickstart-data/weather-netcdf \
  --target "$PRODUCT_URI" \
  --product-name weather_netcdf_observed \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

Expected logs on stderr include:

```text
"message":"Found 4 files"
```

Expected command output on stdout includes:

```text
"plugin": "weather_netcdf"
...
"files_processed": 4
...
"count": 4
"product": "weather_netcdf_observed"
```

## Verify The Product

```bash
python - <<'PY'
import xarray as xr

ds = xr.open_zarr(
    "quickstart-output/weather_netcdf_observed.zarr",
    group="default",
    consolidated=False,
)
assert ds.sizes["timestamp"] == 4
print("ok")
PY
```

Expected output:

```text
ok
```

## Verify The Metric

Firecube flushes buffered metrics at the end of the run. Query the Pushgateway
and select the metric added above:

```bash
curl -s "$FIRECUBE_PUSHGATEWAY_URL/metrics" \
  | grep "firecube_weather_netcdf_files_total"
```

Expected output includes a sample with the value `4`:

```text
firecube_weather_netcdf_files_total{group="default",status="success",...} 4
```

If the metric is absent, check the ingestion logs for `Failed to push metrics
to Pushgateway`, then confirm the configured URL is reachable from the Firecube
process.

Stop the tutorial Pushgateway when verification is complete:

```bash
docker stop firecube-tutorial-pushgateway
```

## Next Steps

- **[Metrics](../concepts/observability/metrics.md)** — understand metric kinds, labels, and Pushgateway behavior
- **[Add Plugin Telemetry](../guides/plugins/observability.md)** — plugin telemetry rules
- **[Observability Reference](../reference/observability.md)** — configure and inspect telemetry backends
- **[Sentinel-3 FRP To Parquet](sentinel3-frp.md)** — download and ingest a real EUMETSAT product
- **[DirectZarrIngestor (Region)](direct-zarr-parallel.md)** — continue to the advanced write path
