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

"""Per-batch closeable-resource registry utilities."""

from __future__ import annotations

import logging
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Protocol, TypeVar

log = logging.getLogger("firecube.core.batch_registry")


class _Closeable(Protocol):
    def close(self) -> None: ...


_ResourceT = TypeVar("_ResourceT", bound=_Closeable)


@dataclass(frozen=True)
class _IdentityRef:
    """Hashable identity wrapper for resources with value equality."""

    value: object

    def __hash__(self) -> int:
        return id(self.value)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _IdentityRef) and self.value is other.value


class BatchResourceRegistry:
    """Track closeable resources by batch and close each batch idempotently.

    The registry is intentionally small: plugin mixins may register per-batch
    resources during ``batch_setup`` and ask the registry to close everything
    for the same batch id during ``batch_teardown``. Teardown pops the batch
    before closing resources, so repeated teardown calls for the same batch are
    no-ops even when an earlier close raises.

    Examples:
        Register and close a per-batch resource:

            >>> class Resource:
            ...     def __init__(self):
            ...         self.closed = False
            ...     def close(self):
            ...         self.closed = True
            >>> registry = BatchResourceRegistry()
            >>> resource = registry.register("batch-1", Resource())
            >>> registry.teardown("batch-1")
            >>> resource.closed
            True
    """

    def __init__(self) -> None:
        self._resources: dict[Hashable, dict[_IdentityRef, _Closeable]] = {}

    def register(self, batch_id: Hashable, resource: _ResourceT) -> _ResourceT:
        """Register a closeable resource for a batch id.

        Args:
            batch_id: Hashable batch identifier. The same value must be passed
                to ``teardown`` to close the registered resources.
            resource: Object exposing ``close()``.

        Returns:
            The same ``resource`` object, so callers can create and register in
            one expression.

        Raises:
            TypeError: If ``batch_id`` is not hashable or ``resource`` does not
                expose a callable ``close`` method.

        Examples:
            Register a file-like object and keep using it:

                >>> registry = BatchResourceRegistry()
                >>> handle = registry.register("batch-1", open(__file__))
                >>> callable(handle.close)
                True
                >>> registry.teardown("batch-1")
        """
        hash(batch_id)
        close = getattr(resource, "close", None)
        if not callable(close):
            raise TypeError("BatchResourceRegistry resources must expose callable close()")
        self._resources.setdefault(batch_id, {})[_IdentityRef(resource)] = resource
        return resource

    def teardown(self, batch_id: Hashable) -> None:
        """Pop and close all resources registered for one batch id.

        Args:
            batch_id: Hashable batch identifier to close and remove.

        Returns:
            ``None``.

        Raises:
            Exception: Re-raises the first exception raised by a resource's
                ``close()`` after attempting to close every resource. Later
                close exceptions are logged and suppressed behind the first.

        Examples:
            Repeated teardown is safe and closes the resource once:

                >>> registry = BatchResourceRegistry()
                >>> resource = registry.register("batch-1", open(__file__))
                >>> registry.teardown("batch-1")
                >>> registry.teardown("batch-1")
        """
        resources = self._resources.pop(batch_id, {})
        first_error: Exception | None = None
        for resource in resources.values():
            try:
                resource.close()
            except Exception as exc:
                log.warning(
                    "Failed to close batch resource for batch_id=%r: %s",
                    batch_id,
                    exc,
                    exc_info=True,
                )
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def teardown_all(self) -> None:
        """Tear down every registered batch id.

        Intended for pipeline-start drains of batches whose per-batch
        teardown never ran (crash paths). Like ``teardown``, every close is
        attempted and the first exception is re-raised at the end.

        Examples:
            >>> registry = BatchResourceRegistry()
            >>> registry.register("batch-1", open(__file__))
            >>> registry.register("batch-2", open(__file__))
            >>> registry.teardown_all()
        """
        first_error: Exception | None = None
        for batch_id in list(self._resources):
            try:
                self.teardown(batch_id)
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error


__all__ = ["BatchResourceRegistry"]
