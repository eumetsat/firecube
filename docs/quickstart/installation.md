# Installation

Firecube requires Python 3.12 or later. This quickstart uses `uv` to create an
isolated environment and install the released package from PyPI.

## Create The Environment

Create a working directory and a Python 3.12 environment:

```bash
mkdir firecube-quickstart
cd firecube-quickstart
uv venv --python 3.12
source .venv/bin/activate
```

Keep this shell open for the rest of the quickstart. If you open a new shell,
return to `firecube-quickstart/` and run `source .venv/bin/activate` again.

## Install Firecube

Install Firecube from PyPI:

```bash
uv pip install firecube
```

The next step creates and installs a local plugin in this environment so the
Firecube CLI can discover it.

## Verify The Installation

```bash
firecube --version
```

Expected output:

```text
Firecube {{ firecube_version() }}
```

To change Firecube itself, use the source setup in
[Contributing To Firecube](../contributing/firecube-contributors.md) instead.

## Next Steps

Firecube works with plugins to convert data to data cubes. Continue to **[Install the Quickstart Plugin](plugins.md)**. 

If you want to learn more about creating your own plugin, visit the [Plugin Development Overview](../guides/plugins/index.md).
