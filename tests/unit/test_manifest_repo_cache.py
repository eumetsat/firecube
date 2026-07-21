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

import pytest

from firecube.core.controlplane.repo import ManifestRepository
from tests.helpers.storage import make_test_binding


def _repo(temp_workspace):
    return ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)


def test_run_entries_cache_scope_sets_cache(temp_workspace):
    repo = _repo(temp_workspace)

    with repo.run_entries_cache_scope():
        cache = repo._run_entries_cache

        assert cache is not None
        assert cache.entries_by_product == {}


def test_run_entries_cache_scope_restores_none(temp_workspace):
    repo = _repo(temp_workspace)

    with repo.run_entries_cache_scope():
        assert repo._run_entries_cache is not None

    assert repo._run_entries_cache is None


def test_run_entries_cache_scope_restores_nested_scopes(temp_workspace):
    repo = _repo(temp_workspace)

    with repo.run_entries_cache_scope():
        outer = repo._run_entries_cache

        assert outer is not None

        with repo.run_entries_cache_scope():
            inner = repo._run_entries_cache

            assert inner is not None
            assert inner is not outer

        assert repo._run_entries_cache is outer

    assert repo._run_entries_cache is None


def test_run_entries_cache_scope_restores_after_exception(temp_workspace):
    repo = _repo(temp_workspace)

    with pytest.raises(RuntimeError), repo.run_entries_cache_scope():
        assert repo._run_entries_cache is not None
        raise RuntimeError("boom")

    assert repo._run_entries_cache is None
