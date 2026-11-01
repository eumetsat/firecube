# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import copy
import pickle

import pytest
from click.testing import CliRunner

import firecube.ingestor.errors as errors
from firecube.cli.main import cli
from firecube.ingestor.runtime.configure import TierConfigurator
from firecube.ingestor.types.context import IngestContext

pytestmark = pytest.mark.contract


def test_output_name_option_is_rejected_with_migration_hint() -> None:
    configurator = TierConfigurator(None, None, plugin_name="cli_test_plugin")
    ctx = IngestContext(source="/tmp/source", options={"output_name": "legacy"})

    with pytest.raises(errors.RemovedConfigKeyError) as exc_info:
        configurator.configure(ctx)
    assert exc_info.value.key == "output_name"


@pytest.mark.parametrize("how", ["pickle", "copy"])
def test_removed_config_key_error_survives_pickle_and_copy(how) -> None:
    err = errors.RemovedConfigKeyError("output_name")
    setattr(err, "sentinel", "kept")  # noqa: B010
    clone = pickle.loads(pickle.dumps(err)) if how == "pickle" else copy.copy(err)
    assert type(clone) is type(err) and str(clone) == str(err) and clone.args == err.args
    assert (clone.key, getattr(clone, "sentinel", None)) == ("output_name", "kept")


@pytest.mark.parametrize("cmd", ["ingest", "zarr preallocate --option output_name=x"])
def test_cli_exits_2_with_output_name_migration_text(tmp_path, monkeypatch, cmd) -> None:
    (tmp_path / "c.toml").write_text("[plugins.direct_zarr_capable_test_plugin]\noutput_name=1")
    monkeypatch.setattr("firecube.cli.zarr._common.coerce_options_for_plugin", lambda _n, o: o)
    args = f"--config-file {tmp_path}/c.toml {cmd} direct_zarr_capable_test_plugin"
    args += f" --target {tmp_path.as_uri()}/q.zarr --product-name qa --write-mode direct"
    result = CliRunner().invoke(cli, f"{args} --input-data file:///tmp".split())
    assert result.exit_code == 2
    assert result.output.endswith(
        "Error: Config key 'output_name' is no longer accepted. Use '--product-name' or "
        "'default_product_name' instead.\nEdit the CLI invocation or the config file passed via "
        "--config-file.\n"
    )
