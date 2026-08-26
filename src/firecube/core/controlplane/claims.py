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

"""Write-domain claim service for ChunkManager v2."""

from __future__ import annotations

import json
import logging
import threading
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Protocol

from firecube.core.controlplane.types import CLAIMS_DIRNAME, ClaimInfo, WriteDomain
from firecube.core.errors import ClaimConflictError, ControlPlaneCorruptionError
from firecube.core.filesystem import StorageFilesystemFull
from firecube.core.storage.uri import StorageUri

DEFAULT_HEARTBEAT_INTERVAL_S = 30
DEFAULT_STALE_THRESHOLD_S = 120

log = logging.getLogger(__name__)


class ClaimService(Protocol):
    """Pluggable claim backend interface."""

    def acquire(self, *, product: str, domain: WriteDomain, owner_id: str) -> ClaimHandle:
        """Acquire an exclusive write claim for the given domain."""
        ...

    def list_claims(self, *, product: str | None = None) -> list[ClaimInfo]:
        """List active claims, optionally filtered by product."""
        ...

    def list_stale_claims(self, *, product: str) -> list[ClaimInfo]:
        """List stale claims for one product."""
        ...

    def clear_claim(self, *, product: str, domain_id: str, force: bool = False) -> bool:
        """Release a claim by domain ID; force=True overrides stale checks."""
        ...


def _entry_to_uri(entry: Any, base_uri: StorageUri) -> StorageUri:
    """Convert an `ls(detail=False)` entry back to a StorageUri using base context."""
    name = entry.get("name") if isinstance(entry, dict) else entry
    if isinstance(name, StorageUri):
        return name
    raw = str(name or "")
    if "://" in raw:
        return StorageUri.parse(raw)
    if base_uri.protocol == "file" and raw.startswith("/"):
        return StorageUri.from_local_path(raw)
    path = raw
    if base_uri.authority and path.startswith(f"{base_uri.authority}/"):
        path = path[len(base_uri.authority) + 1 :]
    return StorageUri(protocol=base_uri.protocol, authority=base_uri.authority, path=path)


@dataclass(slots=True)
class ClaimHandle:
    """Exclusive claim with background heartbeat."""

    fs: StorageFilesystemFull
    claim_path: StorageUri
    info: ClaimInfo
    heartbeat_interval_s: int
    stale_threshold_s: int
    _stop: threading.Event = field(init=False, repr=False)
    _thread: threading.Thread | None = field(init=False, default=None, repr=False)

    def __post_init__(self) -> None:
        self._stop = threading.Event()
        self._start_heartbeat()

    def __enter__(self) -> ClaimHandle:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.release()
        return False

    def release(self) -> None:
        """Stop the heartbeat thread and delete the claim file."""
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        with suppress(FileNotFoundError):
            self.fs.rm(self.claim_path, recursive=False)

    def _start_heartbeat(self) -> None:
        def _loop() -> None:
            while not self._stop.wait(self.heartbeat_interval_s):
                self.info.last_heartbeat_at = time.time()
                payload = {
                    "product": self.info.product,
                    "domain": self.info.domain,
                    "owner_id": self.info.owner_id,
                    "claim_path": self.info.claim_path,
                    "acquired_at": self.info.acquired_at,
                    "last_heartbeat_at": self.info.last_heartbeat_at,
                    "heartbeat_interval_s": self.heartbeat_interval_s,
                    "stale_threshold_s": self.stale_threshold_s,
                }
                with suppress(Exception), self.fs.open(self.claim_path, "w") as handle:
                    json.dump(payload, handle, separators=(",", ":"))

        self._thread = threading.Thread(
            target=_loop,
            name=f"firecube-claim-{self.info.owner_id}",
            daemon=True,
        )
        self._thread.start()


class FilesystemClaimService:
    """Filesystem/object-store backed claim implementation."""

    def __init__(
        self,
        *,
        fs: StorageFilesystemFull,
        control_root_resolver: Any,
        heartbeat_interval_s: int = DEFAULT_HEARTBEAT_INTERVAL_S,
        stale_threshold_s: int = DEFAULT_STALE_THRESHOLD_S,
    ) -> None:
        self._fs = fs
        self._control_root_resolver = control_root_resolver
        self._heartbeat_interval_s = int(heartbeat_interval_s)
        self._stale_threshold_s = int(stale_threshold_s)
        self._log = log

    def acquire(self, *, product: str, domain: WriteDomain, owner_id: str) -> ClaimHandle:
        """Acquire an exclusive write claim, raising ClaimConflictError on conflict."""
        control_path, _control_uri = self._control_root_resolver(product)
        claims_dir = control_path.join(CLAIMS_DIRNAME)
        with suppress(Exception):
            self._fs.makedirs(claims_dir, exist_ok=True)
        claim_path = claims_dir.join(domain.claim_name)
        claim_path_str = claim_path.to_str()
        now = time.time()
        payload = {
            "product": product,
            "domain": domain.identifier,
            "owner_id": owner_id,
            "claim_path": claim_path_str,
            "acquired_at": now,
            "last_heartbeat_at": now,
            "heartbeat_interval_s": self._heartbeat_interval_s,
            "stale_threshold_s": self._stale_threshold_s,
        }

        payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            self._fs.atomic_writer.write_atomic(claim_path, payload_bytes)
        except FileExistsError as exc:
            info = self._read_claim(product=product, claim_path=claim_path)
            stale = info.stale if info is not None else True
            owner = info.owner_id if info is not None else "unknown"
            raise ClaimConflictError(
                f"Write-domain claim conflict for {domain.identifier} "
                f"(owner={owner}, stale={str(stale).lower()})"
            ) from exc

        info = ClaimInfo(
            product=product,
            domain=domain.identifier,
            owner_id=owner_id,
            claim_path=claim_path_str,
            acquired_at=now,
            last_heartbeat_at=now,
            heartbeat_interval_s=self._heartbeat_interval_s,
            stale_threshold_s=self._stale_threshold_s,
        )
        return ClaimHandle(
            fs=self._fs,
            claim_path=claim_path,
            info=info,
            heartbeat_interval_s=self._heartbeat_interval_s,
            stale_threshold_s=self._stale_threshold_s,
        )

    def list_claims(self, *, product: str | None = None) -> list[ClaimInfo]:
        """Scan claim files and return active claims, sorted by (product, domain)."""
        products = [product] if product else list(self._list_products())
        claims: list[ClaimInfo] = []
        for item in products:
            control_path, _control_uri = self._control_root_resolver(item)
            claims_dir = control_path.join(CLAIMS_DIRNAME)
            try:
                entries = self._fs.ls(claims_dir, detail=False)  # type: ignore[attr-defined]
            except Exception:
                continue
            for entry in entries:
                claim_path = _entry_to_uri(entry, claims_dir)
                info = self._read_claim(product=item, claim_path=claim_path)
                if info is not None:
                    claims.append(info)
        claims.sort(key=lambda item: (item.product, item.domain))
        return claims

    def list_stale_claims(self, *, product: str) -> list[ClaimInfo]:
        """List stale claims for one product."""
        return [claim for claim in self.list_claims(product=product) if claim.stale]

    def read_claim_by_domain(self, *, product: str, domain: str) -> ClaimInfo | None:
        """Return the claim for a canonical write-domain identifier, if present.

        Uses targeted claim-name derivation to avoid listing the claims directory.
        Raises ControlPlaneCorruptionError on malformed JSON.
        """
        domain_product, category, name = domain.split(":", 2)
        if domain_product != product:
            msg = f"claim domain {domain!r} does not belong to product {product!r}"
            raise ValueError(msg)
        control_path, _control_uri = self._control_root_resolver(product)
        claims_dir = control_path.join(CLAIMS_DIRNAME)
        claim_name = WriteDomain(product=domain_product, category=category, name=name).claim_name
        return self._read_claim(product=product, claim_path=claims_dir.join(claim_name))

    def clear_claim(self, *, product: str, domain_id: str, force: bool = False) -> bool:
        """Delete a claim file; refuses non-stale claims unless force=True."""
        domain_product, category, name = domain_id.split(":", 2)
        if domain_product != product:
            msg = f"claim domain {domain_id!r} does not belong to product {product!r}"
            raise ValueError(msg)
        control_path, _control_uri = self._control_root_resolver(product)
        claims_dir = control_path.join(CLAIMS_DIRNAME)
        claim_path = claims_dir.join(
            WriteDomain(product=domain_product, category=category, name=name).claim_name
        )
        info = self._read_claim(product=product, claim_path=claim_path)
        if info is None:
            return False
        if not force and not info.stale:
            raise ClaimConflictError(
                f"Refusing to clear active claim {domain_id}; rerun with --force to override."
            )
        with suppress(FileNotFoundError):
            self._fs.rm(claim_path, recursive=False)
        return True

    def _list_products(self) -> set[str]:
        products: set[str] = set()
        base_uri = self._control_root_resolver.base_uri
        try:
            entries = self._fs.ls(base_uri, detail=False)  # type: ignore[attr-defined]
        except Exception:
            return products
        for entry in entries:
            uri = _entry_to_uri(entry, base_uri)
            parts = uri.path.rstrip("/").split("/")
            if parts and parts[-1]:
                products.add(parts[-1])
        return products

    def _list_claim_files(self, product: str) -> list[StorageUri]:
        control_path, _control_uri = self._control_root_resolver(product)
        claims_dir = control_path.join(CLAIMS_DIRNAME)
        try:
            entries = self._fs.ls(claims_dir, detail=False)  # type: ignore[attr-defined]
        except Exception:
            return []
        return [_entry_to_uri(entry, claims_dir) for entry in entries]

    def _read_claim(self, *, product: str, claim_path: StorageUri) -> ClaimInfo | None:
        try:
            with self._fs.open(claim_path, "r") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            return None
        except json.JSONDecodeError as exc:
            msg = f"Corrupt claim file: {claim_path.to_str()}"
            raise ControlPlaneCorruptionError(msg) from exc
        except OSError as exc:
            msg = f"Unable to read claim file: {claim_path.to_str()}"
            raise ControlPlaneCorruptionError(msg) from exc
        except TypeError as exc:
            msg = f"Corrupt claim file: {claim_path.to_str()}"
            raise ControlPlaneCorruptionError(msg) from exc
        except ValueError as exc:
            msg = f"Corrupt claim file: {claim_path.to_str()}"
            raise ControlPlaneCorruptionError(msg) from exc
        return ClaimInfo(
            product=product,
            domain=str(payload.get("domain", "")),
            owner_id=str(payload.get("owner_id", "")),
            claim_path=claim_path.to_str(),
            acquired_at=float(payload.get("acquired_at", 0.0) or 0.0),
            last_heartbeat_at=float(payload.get("last_heartbeat_at", 0.0) or 0.0),
            heartbeat_interval_s=int(
                payload.get("heartbeat_interval_s", self._heartbeat_interval_s)
            ),
            stale_threshold_s=int(payload.get("stale_threshold_s", self._stale_threshold_s)),
        )
