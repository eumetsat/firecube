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

"""Execution-mode selection is decided by ``pipeline_workers`` alone; fields moved to ``ExecutionParams`` are refused at ``EngineConfig`` construction and on a bare config, and forwarded with a ``DeprecationWarning`` on a configured one; ``write_mode`` defaults to direct."""

from __future__ import annotations

import copy
import dataclasses
import inspect
import pickle
from typing import Any

import pytest

from firecube.core.errors import ConfigurationError
from firecube.core.product.resolver import resolve_dataset_target
from firecube.ingestor.config.engine import EngineConfig, config_keys
from firecube.ingestor.runtime.configure import (
    ExecutionMode,
    TierConfigurator,
    determine_execution_mode,
)
from firecube.sdk import BaseIngestor, ExecutionParams, IngestContext

pytestmark = pytest.mark.unit


def test_default_config_runs_sequentially() -> None:
    assert determine_execution_mode(EngineConfig()) is ExecutionMode.SEQUENTIAL


def test_one_worker_runs_sequentially() -> None:
    config = EngineConfig(pipeline_workers=1)
    assert determine_execution_mode(config) is ExecutionMode.SEQUENTIAL


def test_two_or_more_workers_select_pipeline() -> None:
    assert determine_execution_mode(EngineConfig(pipeline_workers=2)) is ExecutionMode.PIPELINE
    assert determine_execution_mode(EngineConfig(pipeline_workers=8)) is ExecutionMode.PIPELINE


@pytest.mark.parametrize("workers", [0, -1])
def test_invalid_pipeline_workers_rejected_at_construction(workers: int) -> None:
    with pytest.raises(ValueError, match="pipeline_workers must be >= 1"):
        EngineConfig(pipeline_workers=workers)


def test_invalid_pipeline_batch_size_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="pipeline_batch_size must be >= 1"):
        EngineConfig(pipeline_batch_size=0)


def test_invalid_extract_workers_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="extract_workers must be >= 1"):
        EngineConfig(extract_workers=0)


_MOVED_FIELDS = {
    "write_mode": "direct",
    "dry_run": True,
    "slot_start": 3,
    "slot_end": 5,
    "slot_size": 10,
    "slot_group": "g",
    "static_owner_slot_start": 0,
    "suppress_static_emission_for_non_owner": True,
}


@pytest.mark.parametrize(
    ("field", "value"),
    [*_MOVED_FIELDS.items(), ("write_mode", None)],
    ids=[*_MOVED_FIELDS, "write_mode-none"],
)
def test_engine_config_refuses_every_field_moved_to_execution_params(
    field: str, value: object
) -> None:
    with pytest.raises(ConfigurationError, match=rf"ExecutionParams\({field}="):
        EngineConfig(**{field: value})  # type: ignore[arg-type]


@pytest.mark.parametrize("field", _MOVED_FIELDS)
def test_reading_a_moved_field_off_a_bare_engine_config_names_the_new_home(field: str) -> None:
    with pytest.raises(ConfigurationError, match=rf"ctx\.execution\.{field}"):
        getattr(EngineConfig(), field)


@pytest.mark.parametrize("field", _MOVED_FIELDS)
def test_reading_a_moved_field_off_a_configured_engine_config_forwards_with_a_warning(
    field: str,
) -> None:
    execution = ExecutionParams(**_MOVED_FIELDS)
    ctx = IngestContext(source="input", options={}, execution=execution)
    engine_config, _, _ = TierConfigurator(None, None, plugin_name="cli_test_plugin").configure(ctx)

    with pytest.warns(DeprecationWarning, match=rf"ctx\.execution\.{field}\b") as record:
        value = getattr(engine_config, field)

    assert value == _MOVED_FIELDS[field]
    assert "v0.2.0" in str(record[0].message)


def test_configure_attaches_the_context_execution_params_itself_not_a_copy() -> None:
    ctx = IngestContext(source="input", options={}, execution=ExecutionParams(write_mode="staged"))
    engine_config, _, _ = TierConfigurator(None, None, plugin_name="cli_test_plugin").configure(ctx)

    assert getattr(engine_config, "_execution", None) is ctx.execution
    assert getattr(EngineConfig(), "_execution", None) is None


def _configured() -> tuple[EngineConfig, ExecutionParams]:
    execution = ExecutionParams(write_mode="staged")
    ctx = IngestContext(source="input", options={}, execution=execution)
    engine_config, _, _ = TierConfigurator(None, None, plugin_name="cli_test_plugin").configure(ctx)
    return engine_config, execution


def test_copy_of_a_configured_engine_config_keeps_the_same_attachment() -> None:
    engine_config, execution = _configured()

    duplicate = copy.copy(engine_config)

    assert getattr(duplicate, "_execution", None) is execution
    assert duplicate == engine_config


@pytest.mark.parametrize(
    "duplicate",
    [copy.deepcopy, lambda c: pickle.loads(pickle.dumps(c))],
    ids=["deepcopy", "pickle"],
)
def test_deepcopy_and_pickle_of_a_configured_engine_config_keep_an_equal_attachment(
    duplicate: Any,
) -> None:
    engine_config, execution = _configured()

    result = duplicate(engine_config)

    assert result == engine_config
    assert getattr(result, "_execution", None) == execution
    with pytest.warns(DeprecationWarning, match=r"ctx\.execution\.write_mode"):
        assert result.write_mode == "staged"


def test_replace_of_a_configured_engine_config_drops_the_attachment() -> None:
    engine_config, _ = _configured()

    replaced = dataclasses.replace(engine_config, pipeline_workers=2)

    assert getattr(replaced, "_execution", None) is None
    with pytest.raises(ConfigurationError, match=r"ctx\.execution\.write_mode"):
        getattr(replaced, "write_mode")  # noqa: B009


def test_the_attached_execution_params_is_not_an_engine_option_key() -> None:
    ctx = IngestContext(source="input", options={}, execution=ExecutionParams())
    engine_config, _, _ = TierConfigurator(None, None, plugin_name="cli_test_plugin").configure(ctx)
    engine_keys = config_keys(EngineConfig)

    assert engine_keys.isdisjoint(_MOVED_FIELDS)
    assert not any("execution" in key for key in engine_keys)
    assert "execution" not in repr(engine_config)
    assert engine_config == EngineConfig()


@pytest.mark.parametrize("field", _MOVED_FIELDS)
def test_hasattr_of_a_moved_field_raises_instead_of_answering_false(field: str) -> None:
    with pytest.raises(ConfigurationError):
        hasattr(EngineConfig(), field)


def test_execution_params_and_path_resolvers_default_to_direct() -> None:
    assert ExecutionParams().write_mode == "direct"
    assert inspect.signature(resolve_dataset_target).parameters["write_mode"].default == "direct"
    resolve_output_uri = inspect.signature(BaseIngestor.resolve_output_uri)
    assert resolve_output_uri.parameters["write_mode"].default == "direct"


def test_extract_workers_does_not_affect_execution_mode() -> None:
    config = EngineConfig(extract_workers=8)
    assert determine_execution_mode(config) is ExecutionMode.SEQUENTIAL
