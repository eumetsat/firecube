"""Open-product handle; a new module because no test covers ``core/product/open.py``."""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path

import pytest

from firecube.core.controlplane.manager import ChunkManager
from firecube.core.errors import ProductLabelMismatchError, ProductLabelMissingError
from firecube.core.product.open import open_product
from tests.helpers.storage import stamp_test_label

pytestmark = pytest.mark.unit


def _store(base: Path, label: str | None = "p") -> str:
    store = base / "p.zarr"
    store.mkdir()
    (store / "zarr.json").write_text("{}")
    if label is not None:
        stamp_test_label(store, label)
    return store.as_uri()


def _tree(root: Path) -> dict[str, bytes | None]:
    return {
        p.relative_to(root).as_posix(): p.read_bytes() if p.is_file() else None
        for p in root.rglob("*")
    }


@pytest.mark.parametrize("fmt", ["zarr", "parquet"])
def test_open_product_reads_back_the_recorded_label_and_keeps_the_format(
    tmp_path: Path, fmt: str
) -> None:
    with open_product(_store(tmp_path, "recorded"), format=fmt) as product:
        assert (product.label, product.identity.format) == ("recorded", fmt)


@pytest.mark.parametrize(
    ("recorded", "name", "error", "match"),
    [
        ("recorded", "other", ProductLabelMismatchError, None),
        (None, None, ProductLabelMissingError, None),
        ("recorded", "", ValueError, "product_name"),
    ],
)
def test_open_product_refuses_a_bad_label_and_writes_nothing(
    tmp_path: Path,
    recorded: str | None,
    name: str | None,
    error: type[Exception],
    match: str | None,
) -> None:
    uri = _store(tmp_path, recorded)
    before = _tree(tmp_path / "p.zarr")
    with pytest.raises(error, match=match), open_product(uri, format="zarr", product_name=name):
        pytest.fail("the block must not run")
    assert _tree(tmp_path / "p.zarr") == before


@pytest.mark.parametrize("given", [None, "chosen"])
def test_open_product_opens_an_unlabelled_store_under_the_given_name_or_the_uri(
    tmp_path: Path, given: str | None
) -> None:
    uri = _store(tmp_path, None)
    with open_product(uri, format="zarr", product_name=given, require_label=False) as product:
        assert product.label == (given or uri)


@pytest.mark.parametrize(("touch", "fails"), [(False, False), (True, False), (True, True)])
def test_open_product_closes_only_the_manager_it_built_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, touch: bool, fails: bool
) -> None:
    closed: list[ChunkManager] = []
    real = ChunkManager.close
    monkeypatch.setattr(ChunkManager, "close", lambda self: (closed.append(self), real(self)))
    manager = None
    with suppress(KeyError), open_product(_store(tmp_path), format="zarr") as product:
        manager = product.manager if touch else None
        if fails:
            raise KeyError("boom")
    assert closed == ([manager] if touch else [])
    with pytest.raises(RuntimeError):
        _ = product.manager


def test_open_product_binds_local_and_leaves_the_environment_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uri = _store(tmp_path)
    monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "s3")
    before = dict(os.environ)
    with open_product(uri, format="zarr") as product:
        assert product.identity.product_uri.to_str() == uri
        assert getattr(product.manager.storage_config, "storage_type", None) == "local"
    assert dict(os.environ) == before
