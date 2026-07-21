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

from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import IngestContext, StorageContext


def test_context_carries_runtime_config():
    """Verify IngestContext carries typed output storage and run identity.

    Plugins should use ctx.storage.output instead of legacy config/session/target fields.
    """
    run_id = "run-12345"
    product_uri = StorageUri.parse("s3://my-bucket/data/product.zarr")
    session = StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="test_product"),
            driver=StorageDriverConfig(),
        )
    )

    ctx = IngestContext(
        source="/dev/null",
        storage=StorageContext(output=session),
        run_id=run_id,
    )

    # Assert fields are available
    assert ctx.storage is not None
    assert ctx.storage.output is session
    output = ctx.storage.output
    assert output is not None
    assert output.product.product_uri == product_uri
    assert output.product.product_uri.parent().to_str() == "s3://my-bucket/data"
    assert ctx.run_id == run_id
