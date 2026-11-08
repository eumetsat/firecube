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

"""The product label a store records once, in `.firecube/product.json`.

The first control-plane write of a store creates the stamp, and nothing rewrites it.
A store without a stamp takes the label that all its run records and held claims
agree on. Writers and claim reads refuse any other label.
"""

from __future__ import annotations

import json
import time
from contextlib import suppress
from typing import Any

from firecube.core.controlplane._wal_reader import WalReader
from firecube.core.controlplane.types import (
    CLAIM_STATE_HELD,
    CLAIMS_DIRNAME,
    CONTROL_DIRNAME,
    PRODUCT_LABEL_FILENAME,
    RUNS_DIRNAME,
)
from firecube.core.errors import (
    ManifestError,
    Message,
    ProductLabelConflictError,
    ProductLabelMismatchError,
)
from firecube.core.filesystem import StorageFilesystemFull
from firecube.core.storage.uri import StorageUri


def recorded_product_label(fs: StorageFilesystemFull, root: StorageUri) -> str | None:
    """Return the label the product at `root` records, or `None` when it records none.

    The stamp decides. Without one, the label every readable run record and held claim
    names decides.

    Raises:
        ManifestError: The stamp, a run or claim record, or the `runs/` or `claims/`
            listing cannot be read, or the stamp is malformed.
        ProductLabelConflictError: The run records and held claims name more than one label.
    """
    control_root = root.join(CONTROL_DIRNAME)
    stamped = _read_stamp(fs, control_root)
    return stamped if stamped is not None else _label_from_records(fs, control_root)


def check_product_label(
    fs: StorageFilesystemFull, control_root: StorageUri, product: str
) -> str | None:
    """Refuse `product` unless the store under `control_root` records it or records no label.

    Reads only. Returns the stamped label, or `None` when the store has no stamp.

    Raises:
        ProductLabelMismatchError: The store records another label.
        ManifestError: The stamp, a run or claim record, or the `runs/` or `claims/`
            listing cannot be read, or the stamp is malformed.
        ProductLabelConflictError: The run records and held claims name more than one label.
    """
    stamped = _read_stamp(fs, control_root)
    recorded = stamped if stamped is not None else _label_from_records(fs, control_root)
    if recorded is not None and recorded != product:
        raise _mismatch(control_root, given=product, recorded=recorded)
    return stamped


def stamp_product_label(fs: StorageFilesystemFull, control_root: StorageUri, product: str) -> str:
    """Refuse a label the store does not record, then create the stamp if the store has none.

    A creator that loses the race re-reads the stamp and proceeds only when it records
    `product`. Returns `product`, which the stamp now records.

    Raises:
        ProductLabelMismatchError: The store records another label.
        ManifestError: The stamp, a run or claim record, or the `runs/` or `claims/`
            listing cannot be read, or the stamp is malformed.
        ProductLabelConflictError: The run records and held claims name more than one label.
    """
    if check_product_label(fs, control_root, product) is not None:
        return product
    stamp_uri = control_root.join(PRODUCT_LABEL_FILENAME)
    payload = {"product": product, "stamped_at": time.time()}
    with suppress(Exception):
        # A local create-only write needs the directory; an object store has none to make.
        fs.makedirs(control_root, exist_ok=True)
    try:
        fs.atomic_writer.write_atomic(stamp_uri, json.dumps(payload).encode("utf-8"))
    except FileExistsError:
        winner = _read_stamp(fs, control_root)
        if winner is None:
            raise ManifestError(
                f"product label stamp {stamp_uri.to_str()} existed at the create and is gone now"
            ) from None
        if winner != product:
            raise _mismatch(control_root, given=product, recorded=winner) from None
    return product


def _read_stamp(fs: StorageFilesystemFull, control_root: StorageUri) -> str | None:
    """Return the label of the stamp under `control_root`, or `None` when no stamp exists.

    Raises:
        ManifestError: The stamp cannot be read, is not JSON, or names no label.
    """
    uri = control_root.join(PRODUCT_LABEL_FILENAME)
    try:
        # Probe first: a failed read of an absent stamp would count as a storage client error.
        if not fs.exists(uri):
            return None
        raw = fs.read_bytes(uri)
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise ManifestError(f"product label stamp {uri.to_str()} cannot be read: {exc}") from exc
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise ManifestError(f"product label stamp {uri.to_str()} is not JSON: {exc}") from exc
    label = payload.get("product") if isinstance(payload, dict) else None
    if not isinstance(label, str) or not label:
        raise ManifestError(
            f"product label stamp {uri.to_str()} names no product label: {raw[:200]!r}"
        )
    return label


def _label_from_records(fs: StorageFilesystemFull, control_root: StorageUri) -> str | None:
    """Return the label every run record and held claim under `control_root` names, or `None`.

    Raises:
        ManifestError: A run or claim record that exists, or its directory, cannot be read.
        ProductLabelConflictError: The records name more than one label.
    """
    labels = _run_labels(fs, control_root) | _held_claim_labels(fs, control_root)
    if len(labels) > 1:
        ordered = sorted(labels)
        raise ProductLabelConflictError(
            Message(
                "The runs and claims of the product at {product_uri} name more than one "
                "label, {labels}, and the product has no label stamp.",
                product_uri=control_root.parent().to_str(),
                labels=", ".join(repr(label) for label in ordered),
            ),
            labels=ordered,
        )
    return next(iter(labels), None)


def _run_labels(fs: StorageFilesystemFull, control_root: StorageUri) -> set[str]:
    """Return the labels the readable `runs/*/run.json` under `control_root` name.

    Reads the raw records, because the WAL reader replaces their label with the caller's.
    A run without a readable `run.json` object, or whose record names no label, is left out.

    Raises:
        ManifestError: `runs/` cannot be listed, or a `run.json` that exists cannot be read.
    """
    runs_dir = control_root.join(RUNS_DIRNAME)
    labels: set[str] = set()
    for entry in _listing(fs, runs_dir):
        payload = _read_json(fs, WalReader._entry_to_uri(entry, runs_dir).join("run.json"))
        label = payload.get("product") if payload is not None else None
        if isinstance(label, str) and label:
            labels.add(label)
    return labels


def _held_claim_labels(fs: StorageFilesystemFull, control_root: StorageUri) -> set[str]:
    """Return the labels the held records in `claims/` under `control_root` name.

    A record's label begins its domain, `<label>:<category>:<name>`. A record without
    `state` is held. An unparsable record, or one that names no domain, is left out.

    Raises:
        ManifestError: `claims/` cannot be listed, or a record that exists cannot be read.
    """
    claims_dir = control_root.join(CLAIMS_DIRNAME)
    labels: set[str] = set()
    for entry in _listing(fs, claims_dir):
        payload = _read_json(fs, WalReader._entry_to_uri(entry, claims_dir))
        if payload is None or payload.get("state", CLAIM_STATE_HELD) != CLAIM_STATE_HELD:
            continue
        domain = payload.get("domain")
        if not isinstance(domain, str) or ":" not in domain:
            continue
        product = payload.get("product")
        # The product field disambiguates a label that itself holds a colon.
        if isinstance(product, str) and product and domain.startswith(f"{product}:"):
            labels.add(product)
        else:
            labels.add(domain.split(":", 1)[0])
    return labels


def _listing(fs: StorageFilesystemFull, directory: StorageUri) -> list[Any]:
    """List `directory` freshly; an absent directory lists as empty.

    Raises:
        ManifestError: The directory exists and cannot be listed.
    """
    if not fs.exists(directory):
        return []
    try:
        return list(fs.ls(directory, detail=False, refresh=True))  # type: ignore[attr-defined]
    except FileNotFoundError:
        return []
    except Exception as exc:
        raise ManifestError(
            f"control-plane directory {directory.to_str()} cannot be listed: {exc}"
        ) from exc


def _read_json(fs: StorageFilesystemFull, uri: StorageUri) -> dict[str, Any] | None:
    """Return the JSON object at `uri`, or `None` when it is absent or not a JSON object.

    Raises:
        ManifestError: The object exists and cannot be read.
    """
    try:
        raw = fs.read_bytes(uri)
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise ManifestError(f"control-plane record {uri.to_str()} cannot be read: {exc}") from exc
    try:
        payload = json.loads(raw)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _mismatch(control_root: StorageUri, *, given: str, recorded: str) -> ProductLabelMismatchError:
    """Build the refusal of label `given` by a store that records `recorded`."""
    return ProductLabelMismatchError(
        Message(
            "Product label {given!r} is refused: the product at {product_uri} records "
            "the label {product!r}.",
            given=given,
            product=recorded,
            product_uri=control_root.parent().to_str(),
        ),
        given=given,
        recorded=recorded,
    )
