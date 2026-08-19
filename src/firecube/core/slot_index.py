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

"""Slot-index model for firecube product time axes.

The slot-index model describes how a product's time axis is partitioned into
discrete slots: one or more named groups, each with its own cadence and
rounding mode, anchored on a shared UTC epoch. The model is content-addressed
via a canonical-JSON SHA-256 (``identity_hash``) so independent writers can
verify they agree on the partitioning scheme without coordinating beforehand.

Stability rules:

 ``canonical_bytes()`` is deterministic: groups are emitted in alphabetical
  order, JSON keys are sorted, and the separators are tight. ``time_unit=None``
  is always serialised as ``"time_unit":null``.
 ``identity_hash`` is never embedded inside the hashed payload.
 Epoch normalisation is the explicit responsibility of
  :func:`normalize_epoch_iso`; ``canonical_bytes()`` does NOT silently mutate
  the stored epoch string. ``"Z"`` and ``"+00:00"`` therefore deliberately
  produce different hashes -- callers that want them to converge must
  normalise the epoch before constructing the model.
"""

from __future__ import annotations

import hashlib
import json
import numbers
from dataclasses import dataclass
from typing import Literal

SLOT_INDEX_MODEL_ATTR = "firecube_slot_index_model"
SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR = "firecube_slot_index_model_identity_hash"


@dataclass(frozen=True, slots=True)
class SlotAxis:
    """A single time-axis partitioning rule for one group.

    Attributes:
        cadence_s: Slot width in seconds; must be strictly positive.
        mode: ``"exact"`` means timestamps must align exactly on a slot
            boundary; ``"floor"`` means timestamps are floored to the
            preceding boundary.
    """

    cadence_s: int
    mode: Literal["exact", "floor"]

    def __post_init__(self) -> None:
        if isinstance(self.cadence_s, bool) or not isinstance(self.cadence_s, numbers.Integral):
            raise TypeError(
                f"cadence_s must be an integral type "
                f"(Python int or numpy.integer subclass); bool is explicitly rejected. "
                f"Got: {type(self.cadence_s).__name__}({self.cadence_s!r})"
            )
        # Normalize numpy.integer (e.g. np.int64) to Python int so json.dumps in
        # canonical_bytes() doesn't crash. Byte output unchanged for equal values.
        object.__setattr__(self, "cadence_s", int(self.cadence_s))
        if self.cadence_s <= 0:
            raise ValueError(f"cadence_s must be > 0, got {self.cadence_s!r}")
        if self.mode not in {"exact", "floor"}:
            raise ValueError(f"mode must be 'exact' or 'floor', got {self.mode!r}")


@dataclass(frozen=True, slots=True)
class SlotIndexModel:
    """Content-addressable description of a product's slot-index partitioning.

    Attributes:
        name: Human-readable identifier for the model (e.g. ``"opera_v1"``).
        epoch: ISO-8601 UTC anchor (e.g. ``"2026-01-01T00:00:00Z"``); used as
            the origin from which slot indices are counted.
        groups: Per-group :class:`SlotAxis` mapping; at least one entry.
        time_unit: Optional storage-precision hint
            (e.g. ``"seconds"``, ``"milliseconds"``); ``None`` is permitted
            and is serialised as JSON ``null``.
    """

    name: str
    epoch: str
    groups: dict[str, SlotAxis]
    time_unit: str | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must be non-empty")
        if not self.epoch:
            raise ValueError("epoch must be non-empty")
        if not self.groups:
            raise ValueError("groups must be non-empty")
        for k in self.groups:
            if not k:
                raise ValueError("group keys must be non-empty strings")

    def canonical_bytes(self) -> bytes:
        """Return the canonical UTF-8 JSON encoding of this model.

        Determinism rules:

        * ``groups`` are emitted in alphabetical key order.
        * ``json.dumps(..., sort_keys=True, separators=(",", ":"))`` is used so
          that whitespace and key order are stable across runs and platforms.
        * ``time_unit=None`` is serialised as JSON ``null``.
        * The output does NOT contain :attr:`identity_hash` -- the hash is
          computed over the canonical bytes, not stored inside them.
        """

        payload = {
            "schema_version": "v1",
            "name": self.name,
            "epoch": self.epoch,
            "time_unit": self.time_unit,
            "groups": {
                group: {"cadence_s": axis.cadence_s, "mode": axis.mode}
                for group, axis in sorted(self.groups.items())
            },
        }
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")

    @property
    def identity_hash(self) -> str:
        """SHA-256 hex digest of :meth:`canonical_bytes`.

        Two :class:`SlotIndexModel` instances are considered equivalent
        partitioning schemes iff their ``identity_hash`` values match.
        """

        return hashlib.sha256(self.canonical_bytes()).hexdigest()


def iso_to_epoch_s(iso: str) -> int:
    """Convert an ISO-8601 UTC timestamp to whole seconds since Unix epoch.

    Only UTC inputs are accepted. The trailing ``"Z"`` or the explicit
    ``"+00:00"`` offset are both honoured; any other offset raises
    :class:`ValueError`. Numpy is imported lazily so that ``slot_index`` stays
    cheap to import for code paths that never touch the epoch helpers.
    """

    import numpy as np

    if not iso:
        raise ValueError("iso must be a non-empty string")

    text = iso.strip()
    if text.endswith("Z"):
        bare = text[:-1]
    elif text.endswith("+00:00"):
        bare = text[: -len("+00:00")]
    elif text.endswith("-00:00"):
        bare = text[: -len("-00:00")]
    else:
        raise ValueError(
            f"iso_to_epoch_s requires UTC-explicit ISO 8601 input "
            f"(end with 'Z', '+00:00', or '-00:00'). Got: {iso!r}. "
            f"To fix: use 'YYYY-MM-DDTHH:MM:SSZ'."
        )

    try:
        when = np.datetime64(bare, "s")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"could not parse ISO-8601 timestamp {iso!r}: {exc}") from exc

    epoch = np.datetime64("1970-01-01T00:00:00", "s")
    return int((when - epoch).astype("int64"))


def epoch_s_to_iso(seconds: int) -> str:
    """Convert whole seconds-since-epoch to a canonical ``"YYYY-MM-DDTHH:MM:SSZ"``."""

    import numpy as np

    when = np.datetime64("1970-01-01T00:00:00", "s") + np.timedelta64(int(seconds), "s")
    # str(np.datetime64(..., 's')) renders as "YYYY-MM-DDTHH:MM:SS"; append the
    # UTC marker so that the round-trip with iso_to_epoch_s is exact.
    return f"{when!s}Z"


def normalize_epoch_iso(iso: str) -> str:
    """Round-trip an ISO-8601 UTC string through ``epoch_s`` and back to ``"...Z"``.

    Equivalent to ``epoch_s_to_iso(iso_to_epoch_s(iso))``; raises
    :class:`ValueError` for non-UTC inputs via :func:`iso_to_epoch_s`.
    """

    return epoch_s_to_iso(iso_to_epoch_s(iso))


__all__ = [
    "SLOT_INDEX_MODEL_ATTR",
    "SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR",
    "SlotAxis",
    "SlotIndexModel",
    "epoch_s_to_iso",
    "iso_to_epoch_s",
    "normalize_epoch_iso",
]
