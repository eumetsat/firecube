# Weather CSV: Observability

## Goal

Add one domain-specific metric to the Weather CSV plugin and verify the plugin
still writes a Zarr product.

Continue from [Weather CSV Plugin](weather-csv.md). You will keep the same
plugin and add one metric through `ctx.telemetry`.

## Add One Metric

In `plugins_dev/firecube-weather-csv/src/firecube_weather_csv/ingestor.py`, add
this block near the end of `build_dataset`, just before `return ds`:

```python
        if ctx.telemetry is not None:
            ctx.telemetry.emit(
                "weather_csv_files",
                len(items),
                kind="counter",
                meta={"group": group, "status": "success"},
            )
```

`counter` is the right kind here because the value is a count that increases
during the run. Firecube exports it as:

```text
firecube_weather_csv_files_total
```

## Run The Plugin

```bash
PRODUCT_URI="file://$PWD/tutorial-output/weather_csv_observed.zarr"

uv run firecube ingest weather_csv \
  --input-data tutorial-data/weather-csv \
  --target "$PRODUCT_URI" \
  --product-name weather_csv_observed \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option 'include_patterns=["*.csv"]'
```

Expected logs on stderr include:

```text
"message":"Found 4 files"
```

Expected command output on stdout includes:

```text
"plugin": "weather_csv"
...
"files_processed": 4
...
"count": 4
"product": "weather_csv_observed"
```

## Verify The Product

```bash
uv run python - <<'PY'
import xarray as xr

ds = xr.open_zarr(
    "tutorial-output/weather_csv_observed.zarr",
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

## Optional: Push Metrics

When a Prometheus Pushgateway is available, set its URL before running
ingestion:

```bash
export FIRECUBE_PUSHGATEWAY_URL="http://localhost:9091"
```

Rerun ingestion, then query the pushed metric:

```bash
curl -s "$FIRECUBE_PUSHGATEWAY_URL/metrics" \
  | grep "firecube_weather_csv_files_total"
```

Expected output includes:

```text
firecube_weather_csv_files_total{group="default",status="success",...} 4.0
```

Pushgateway grouping, labels, and environment variables are listed in
[Observability Reference](../reference/observability.md).

## Next Steps

- **[Metrics](../concepts/observability/metrics.md)** — metric kinds, labels, and Pushgateway behavior
- **[Plugin Observability](../concepts/plugins/observability.md)** — plugin telemetry rules
- **[Sentinel-3 FRP Plugin](sentinel3-frp.md)** — build a Parquet plugin from downloaded data
- **[Direct Parallel Zarr](direct-zarr-parallel.md)** — continue to the advanced write path
