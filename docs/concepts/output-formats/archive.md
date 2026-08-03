# Tensogram

Firecube uses [Tensogram](https://github.com/ecmwf/tensogram) to represent Zarr
data as a single `.tgm` file. The file is useful when a chunked Zarr product
needs to be transferred, downloaded, or kept in archival storage.

Zarr remains the normal cube layout for ingestion and analysis. The `.tgm` file
is the package Firecube creates from an existing Zarr product.

<figure markdown="span">
  ![A Zarr product is packaged into Tensogram data messages plus a final ChunkManager message, then restored back to Zarr.](../../assets/images/firecube-tensogram-flow.svg){ width="820" }
  <figcaption markdown="span">Firecube packages selected Zarr data into `.tgm` messages and stores ChunkManager records as the final message, so restore keeps the cube lifecycle inspectable.</figcaption>
</figure>

## Tensogram And Zarr

Zarr is the normal layout for incremental writes and partial reads. Tensogram
is the portable representation: it packages selected Zarr data into messages
inside one file. Restoring the file recreates a Zarr product.

When Firecube packages one of its Zarr products, it also includes the associated
ChunkManager records. A restored product therefore retains the run and
coordination history needed by Firecube operations.

## Open With Xarray

Consumers with `tensogram-xarray` installed can also open `.tgm` files through
Xarray:

```python
import xarray as xr

ds = xr.open_dataset("product.tgm", engine="tensogram")
print(ds)
```

## Next Steps

- **[Archive Operations](../../operations/archive/index.md)** — create, inspect, validate, or restore `.tgm` files
- **[Zarr](zarr/index.md)** — understand the normal chunked product layout
