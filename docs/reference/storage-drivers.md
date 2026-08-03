# Storage Drivers

A storage driver selects the filesystem implementation Firecube uses for
product data and the product-local `.firecube/` control plane. It does not
select the product location; the product URI supplies that location.

For the storage model and driver-selection guidance, see
[Product Storage](../concepts/storage.md).

## Driver Values

| Value | Availability | Storage types | Default |
|---|---|---|---|
| `fsspec` | Included with Firecube | `local`, `s3` | Yes |
| `obstore` | Requires the `obstore` extra | `local`, `s3` | No |

Install the optional driver with:

```bash
uv pip install 'firecube[obstore]'
```

Selecting `obstore` without the extra installed fails with an error containing
the same installation command.

## Configuration

Choose the driver with one of these equivalent configuration surfaces:

| Surface | Example |
|---|---|
| CLI | `--storage-driver obstore` |
| Environment | `FIRECUBE_STORAGE_DRIVER=obstore` |
| `config.toml` | `driver = "obstore"` under `[storage]` |

CLI values override environment values, which override `config.toml`. The
complete generated command surface is in the [CLI Reference](cli.md), and the
generated `StorageConfig` schema is in the
[Configuration Reference](config.md#storageconfig).

`--storage-type` and the product URI describe the storage location. They are
separate from `--storage-driver`, which selects the implementation used to
access that location.

## Runtime Contract

A run uses one driver for product writes, control-plane records, and staged
uploads. Firecube does not switch between `fsspec` and `obstore` within one
write domain.

Both drivers implement the common filesystem operations used by Firecube,
including reads, writes, listing, deletion, and atomic claim creation. Optional
capabilities advertised by the current backend implementations are generated
below.

{{ render_storage_driver_capabilities() }}

These are filesystem capabilities, not output-format restrictions. Zarr,
Parquet, archive, and maintenance commands may impose additional requirements;
use each command's generated CLI reference for its accepted options.

## Next Steps

- **[Product Storage](../concepts/storage.md)** — choose a storage type, driver, and write mode
- **[Configuration Reference](config.md)** — inspect storage fields and precedence
- **[CLI Reference](cli.md)** — inspect command-specific storage options
