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

"""Unit tests for Phase 2 — GenericTensogramIngestor and TensogramWriteStrategy."""

from __future__ import annotations


class TestTensogramTemplateConfig:
    """TensogramTemplateConfig defaults and from_options parsing."""

    def test_tensogram_template_config_defaults(self):
        from firecube.ingestor.templates.config import TensogramTemplateConfig

        cfg = TensogramTemplateConfig()
        assert cfg.tensogram_compression == "zstd"
        assert cfg.tensogram_message_granularity == "per_variable"
        assert cfg.tensogram_allow_nan is True
        assert cfg.tensogram_allow_inf is True

    def test_tensogram_template_config_from_options(self):
        from firecube.ingestor.templates.config import TensogramTemplateConfig

        cfg = TensogramTemplateConfig.from_options({"tensogram_compression": "zstd"})
        assert cfg.tensogram_compression == "zstd"


class TestTensogramWriteStrategy:
    """TensogramWriteStrategy encodes xarray Datasets to .tgm files."""

    def test_write_strategy_creates_tgm_file(self, tmp_path):
        """write() produces a non-empty .tgm file."""
        import numpy as np
        import xarray as xr

        from firecube.ingestor.runtime.tensogram.strategy import TensogramWriteStrategy

        ds = xr.Dataset(
            {"temp": (["t", "y"], np.ones((3, 4), dtype="float32"))},
            coords={"t": [0, 1, 2], "y": [1.0, 2.0, 3.0, 4.0]},
        )
        tgm = str(tmp_path / "test.tgm")
        strategy = TensogramWriteStrategy(target=tgm, compression="blosc2")
        strategy.write(ds)
        strategy.close()
        assert (tmp_path / "test.tgm").exists()
        assert (tmp_path / "test.tgm").stat().st_size > 0

    def test_write_strategy_output_is_xarray_openable(self, tmp_path):
        """Output .tgm is openable with xr.open_dataset(engine="tensogram")."""
        import numpy as np
        import xarray as xr

        from firecube.ingestor.runtime.tensogram.strategy import TensogramWriteStrategy

        ds = xr.Dataset({"FWI": (["t"], np.array([1.0, 2.0, 3.0], dtype="float32"))})
        tgm = str(tmp_path / "test.tgm")
        strategy = TensogramWriteStrategy(target=tgm)
        strategy.write(ds)
        strategy.close()
        result = xr.open_dataset(tgm, engine="tensogram")
        assert "FWI" in result.data_vars

    def test_write_strategy_overwrites_existing_target(self, tmp_path):
        """write() overwrites the target if called again (no append)."""
        import numpy as np
        import tensogram
        import xarray as xr

        from firecube.ingestor.runtime.tensogram.strategy import TensogramWriteStrategy

        stale = xr.Dataset({"stale": (["t"], np.array([99.0], dtype="float32"))})
        fresh = xr.Dataset({"fresh": (["t"], np.array([1.0, 2.0, 3.0], dtype="float32"))})
        tgm = str(tmp_path / "test.tgm")
        s1 = TensogramWriteStrategy(target=tgm)
        s1.write(stale)
        s1.close()

        s2 = TensogramWriteStrategy(target=tgm)
        s2.write(fresh)
        s2.close()

        loaded = xr.open_dataset(tgm, engine="tensogram")
        assert list(loaded.data_vars) == ["fresh"]
        np.testing.assert_array_equal(loaded["fresh"].values, fresh["fresh"].values)
        with tensogram.TensogramFile.open(tgm) as archive:  # type: ignore[attr-defined]
            assert archive.message_count() == 1

    def test_write_groups_processes_all_groups(self, tmp_path):
        """write_groups() calls dataset_for_batch for each group and returns metrics."""
        import numpy as np
        import xarray as xr

        from firecube.ingestor.runtime.tensogram.strategy import TensogramWriteStrategy

        ds = xr.Dataset({"T": (["x"], np.array([1.0, 2.0, 3.0], dtype="float32"))})
        tgm = str(tmp_path / "test.tgm")
        strategy = TensogramWriteStrategy(target=tgm)
        call_count = 0

        def get_ds(group, timestamps):
            nonlocal call_count
            call_count += 1
            return ds

        metrics = strategy.write_groups(
            group_to_timestamps={"group_a": [1, 2, 3], "group_b": [4, 5, 6]},
            dataset_for_batch=get_ds,
            batch_size=3,
        )
        strategy.close()
        assert call_count == 2
        assert metrics["messages_written"] == 2


class TestOutputFormatRouting:
    """Output format routing and public API import checks."""

    def test_generic_tensogram_ingestor_importable(self):
        """GenericTensogramIngestor is importable from the public api."""
        from firecube.ingestor.api import GenericTensogramIngestor

        assert GenericTensogramIngestor is not None
