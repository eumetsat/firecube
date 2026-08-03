# Configure S3 Access

Provide the credentials and connection settings needed by the optional S3
ingestion path. Use environment variables for the current shell or
`config.toml` for settings that should persist.

## Set Values For The Current Shell

Replace the example values with those supplied by your storage provider. The
endpoint is needed for S3-compatible services with a custom endpoint; omit it
when the provider uses its standard endpoint.

```bash
export FIRECUBE_ENDPOINT_URL="https://your-s3-endpoint.example.com"
export FIRECUBE_REGION="your-region"
export FIRECUBE_ACCESS_KEY="your-access-key"
export FIRECUBE_SECRET_KEY="your-secret-key"
```

Set `FIRECUBE_REGION` only when your provider requires a region.

Environment variables have higher precedence than `config.toml`. CLI flags and
explicit command overrides still win over environment variables.

## Store Values In A Config File

By default, Firecube looks for a configuration at `~/.config/firecube/config.toml`.
If you store credentials in this file, keep it private.

```toml
[storage]
endpoint_url = "https://your-s3-endpoint.example.com"
region = "your-region"
access_key = "your-access-key"
secret_key = "your-secret-key"
```

The ingest commands still pass the full product URI, `--storage-type`,
`--storage-driver`, and `--write-mode` explicitly.

## Verify The Configuration

Return to [Run Ingestion](ingestion.md) and run the S3 command. A successful run
prints a JSON summary whose `stored_at` value is the requested S3 product URI.

See the [Configuration Reference](../reference/config.md#storageconfig) for all
storage fields and the [Configuration Model](../concepts/configuration.md) for
precedence.

## Next Steps

- **[Run Ingestion](ingestion.md)** — run the S3 ingestion command
- **[Create an Intake Catalog](catalogs.md)** — catalog the completed product
