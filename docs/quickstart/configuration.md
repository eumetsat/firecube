# Configuration

Use environment variables or a TOML file to provide S3 credentials, storage
driver defaults, and plugin-specific defaults.

For the mental model behind CLI flags, config files, environment variables, and plugin options, see the
[Configuration Model](../concepts/configuration.md).

### Storage Types

Product locations come from full CLI product URIs such as `file:///tmp/demo.zarr`
or `s3://bucket/path/demo.zarr`. Every `firecube ingest` call also includes
`--storage-type` and `--storage-driver`.

### Environment Variables

Use environment variables for S3 credentials and connection settings:

```bash
export FIRECUBE_ENDPOINT_URL="https://your-s3-endpoint.example.com"
export FIRECUBE_REGION="your-region"
export FIRECUBE_ACCESS_KEY="your-access-key"
export FIRECUBE_SECRET_KEY="your-secret-key"
```

Environment variables have higher precedence than `config.toml`. CLI flags and
explicit command overrides still win over environment variables.

### The Config File

By default, Firecube looks for a configuration at `~/.config/firecube/config.toml`.

#### Minimal `config.toml`

Use a config file for settings you want to keep between runs, such as a storage
endpoint or plugin defaults. If you store credentials in this file, keep it
private.

```toml
[storage]
endpoint_url = "https://your-s3-endpoint.example.com"
region = "your-region"
access_key = "your-access-key"
secret_key = "your-secret-key"

[plugins.my_plugin]
default_product_name = "my_product"
custom_option = "value"
```
See the [Configuration Reference](../reference/config.md#storageconfig) for the
full list and precedence.

## Next Steps

- **[Run Ingestion](ingestion.md)** — return to local and S3 examples
- **[Intake Catalogs](catalogs.md)** — continue after ingestion succeeds
