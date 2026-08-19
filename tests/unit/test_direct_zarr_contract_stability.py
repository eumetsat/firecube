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

import pytest


@pytest.mark.unit
def test_directzarr_writeintent_has_timestamp_val_field():
    """The 'timestamp_val' field on WriteIntent is part of the stable plugin contract."""
    from firecube.ingestor.api import WriteIntent

    fields_by_name = {f.name for f in WriteIntent.__dataclass_fields__.values()}
    assert "timestamp_val" in fields_by_name


@pytest.mark.unit
def test_directzarr_has_index_spec_method():
    """The 'index_spec' method is part of the stable DirectZarrIngestor contract."""
    from firecube.ingestor.api import DirectZarrIngestor

    assert hasattr(DirectZarrIngestor, "index_spec")
    assert callable(DirectZarrIngestor.index_spec)


@pytest.mark.unit
def test_writeintent_kind_timestamp_token():
    """The intent kind 'timestamp' is a stable dispatch token (not the dim name)."""
    from firecube.ingestor.api import WriteIntent

    intent = WriteIntent(
        group="g",
        array="a",
        ts_index=0,
        data=None,
        kind="timestamp",
        timestamp_val="2026-01-01T00:00:00Z",
    )
    assert intent.kind == "timestamp"
