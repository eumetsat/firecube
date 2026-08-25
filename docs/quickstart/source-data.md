# Prepare Source Data

Firecube expects you to provide source datasets. In normal use, these files
may come from a data provider, an existing local source, or object storage.
You are responsible for staging source files that match what your installed
plugin accepts, then passing their directory or storage prefix to
`--input-data`. The installed plugin decides which formats, filenames, and
dataset structure it accepts; nothing about that is generic.

The `quickstart_plugin` plugin from the previous page expects time-indexed
NetCDF files. It ships a script that generates four matching example files, so
this quickstart does not require you to source real data.

## Create The Example Files

Run this command from the `firecube-quickstart-plugin/` directory, with the
quickstart virtual environment active:

```bash
python scripts/generate_sample_data.py
```

## Verify The Source Data

List the generated files:

```bash
ls sample_data
```

Expected output:

```text
sample01.nc  sample02.nc  sample03.nc  sample04.nc
```

Each file contains one timestamp of `temperature_c` and `humidity_pct` on the
same 2 by 3 latitude-longitude grid. The ingestion command passes their parent
directory to the plugin.

## Next Steps

- **[Run Ingestion](ingestion.md)**: convert the four NetCDF files into one
  local Zarr product and inspect it.
