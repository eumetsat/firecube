# Install the Example Plugin

## When To Use This

A Firecube plugin is a separately installed Python package that tells Firecube
how to discover, read, and convert a particular dataset. Firecube runs the
ingestion workflow and manages the output product.

This page installs the published `firecube-quickstart-plugin` example. It
already implements dataset conversion for a small set of time-indexed NetCDF
files, so this quickstart does not require you to write any plugin code.

## Prerequisites

- Firecube installed in a Python environment.
- The environment activated with `source .venv/bin/activate`.
- `git` installed.

## Steps

### Clone The Plugin

```bash
git clone https://github.com/eumetsat/firecube-quickstart-plugin.git
cd firecube-quickstart-plugin
```

Stay in this directory for the rest of the quickstart. The virtual environment
you activated earlier stays active regardless of your current directory.

### Install The Plugin

Install the cloned project into the active environment in editable mode:

```bash
firecube plugins install .
```

Expected output ends with:

```text
Detected plugins: quickstart_plugin
```


### Validate The Installation

```bash
firecube plugins describe quickstart_plugin

```



## Next Steps

- **[Prepare Source Data](source-data.md)**: generate the NetCDF files this
  plugin expects.
- **[Plugin Development](../guides/plugins/index.md)**: build a plugin for
  your own dataset instead of the example.
