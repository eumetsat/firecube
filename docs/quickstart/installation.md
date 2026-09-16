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

Keep this environment active and run the remaining commands from
`firecube-quickstart/`. In a new shell, return to that directory and run
`source .venv/bin/activate` again.

Alternatively, stay in that directory and use `uv run firecube` and
`uv run python` without activating the environment. The examples below use activation.

## Install Firecube

Install Firecube from PyPI:

```bash
uv pip install firecube
```

The next step installs the existing Quickstart plugin in this environment so
the Firecube CLI can discover it.

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
