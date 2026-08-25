from firecube.core.api import FIRECUBE_STATIC_WRITTEN_ATTR as core_static_written_attr
from firecube.core.api import RESERVED_ARRAY_ATTRS as core_reserved_array_attrs
from firecube.core.api import ExtentUnknownError as CoreExtentUnknownError
from firecube.core.api import assert_attrs_safe as core_assert_attrs_safe
from firecube.core.api import resolve_index_spec as core_resolve_index_spec
from firecube.ingestor.api import FIRECUBE_STATIC_WRITTEN_ATTR as ingestor_static_written_attr
from firecube.ingestor.api import RESERVED_ARRAY_ATTRS as ingestor_reserved_array_attrs
from firecube.ingestor.api import ExtentUnknownError as IngestorExtentUnknownError
from firecube.ingestor.api import assert_attrs_safe as ingestor_assert_attrs_safe
from firecube.ingestor.api import resolve_index_spec as ingestor_resolve_index_spec


def test_extent_unknown_error_exported_from_both_facades() -> None:
    assert CoreExtentUnknownError is IngestorExtentUnknownError


def test_reserved_attrs_exported_from_both_facades() -> None:
    assert core_static_written_attr == "firecube_static_written"
    assert ingestor_static_written_attr == "firecube_static_written"
    assert core_static_written_attr is ingestor_static_written_attr
    assert core_static_written_attr in core_reserved_array_attrs
    assert ingestor_static_written_attr in ingestor_reserved_array_attrs
    assert core_reserved_array_attrs is ingestor_reserved_array_attrs

    for assert_attrs_safe in (core_assert_attrs_safe, ingestor_assert_attrs_safe):
        assert_attrs_safe({"my_custom_attr": 42})

        try:
            assert_attrs_safe({"firecube_static_written": True})
        except ValueError:
            pass
        else:
            raise AssertionError("reserved attrs must be rejected")


def test_resolve_index_spec_reexported_from_ingestor_api() -> None:
    assert core_resolve_index_spec is ingestor_resolve_index_spec
