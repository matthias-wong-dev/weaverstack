"""The authoring surface: what a developer writes and what it delegates to."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from weaver import Folder, Table, View, WeaverObject
from weaver.errors import LoadError
from weaver.objects import _active_resolver


@dataclass
class FakeContext:
    """Stands in for what Weaver injects per step."""

    spark: Any = "spark-session"
    object_path: Any = "/srv/.local/Sales/Tables/Sales/Order"
    schema: tuple = ()
    primary_key: tuple = ("Order id",)
    is_incremental: bool = False
    staged: Any = "/tmp/staging"
    frame: Any = None
    calls: list = field(default_factory=list)

    def current_dataframe(self):
        self.calls.append("current_dataframe")
        return self.frame

    def empty_frame(self):
        self.calls.append("empty_frame")
        return "empty"

    def staging_folder(self):
        self.calls.append("staging_folder")
        return self.staged


class Sales__Order(Table):
    def read(self):
        return [], []


class Sales__Export(Folder):
    def read(self):
        return self.staging_folder(), []


# --- depending on another object -------------------------------------------


def test_a_dependency_resolves_through_the_running_workflow():
    resolved = []

    def resolver(cls, accessor):
        resolved.append((cls, accessor))
        return "the dataframe"

    token = _active_resolver.set(resolver)
    try:
        assert Sales__Order.dataframe() == "the dataframe"
    finally:
        _active_resolver.reset(token)
    assert resolved == [(Sales__Order, "dataframe")]


def test_a_folder_dependency_resolves_a_path():
    token = _active_resolver.set(lambda cls, accessor: f"{cls.__name__}:{accessor}")
    try:
        assert Sales__Export.folder_path() == "Sales__Export:path"
    finally:
        _active_resolver.reset(token)


def test_a_folders_own_path_is_not_shadowed_by_the_accessor():
    """A classmethod named `path` would replace the inherited property —
    silently, because a bound method is truthy."""
    context = FakeContext(object_path="/srv/.local/Sales/Files/Sales/Export")
    assert Sales__Export(context).path == "/srv/.local/Sales/Files/Sales/Export"


def test_the_two_path_concepts_have_different_names():
    assert "path" in vars(WeaverObject)
    assert "path" not in vars(Folder)
    assert "folder_path" in vars(Folder)


def test_accessors_explain_themselves_outside_a_workflow():
    with pytest.raises(LoadError, match="only available while Weaver is executing"):
        Sales__Order.dataframe()


def test_views_are_readable_as_dependencies():
    token = _active_resolver.set(lambda cls, accessor: accessor)
    try:
        class Sales__OrderView(View):
            pass

        assert Sales__OrderView.dataframe() == "dataframe"
    finally:
        _active_resolver.reset(token)


def test_concurrent_steps_do_not_share_a_resolver():
    """A ContextVar, so one step's dependencies are invisible to another."""
    token = _active_resolver.set(lambda cls, accessor: "outer")
    try:
        assert Sales__Order.dataframe() == "outer"
    finally:
        _active_resolver.reset(token)
    with pytest.raises(LoadError):
        Sales__Order.dataframe()


# --- an object's own context ------------------------------------------------


def test_the_object_surface_delegates_to_the_context():
    context = FakeContext(schema=(("Order id", "string"),))
    order = Sales__Order(context)
    assert order.spark == "spark-session"
    assert order.path == "/srv/.local/Sales/Tables/Sales/Order"
    assert order.schema == (("Order id", "string"),)
    assert order.primary_key == ("Order id",)
    assert order.is_incremental is False


def test_table_accessors_call_through():
    context = FakeContext(frame="persisted")
    order = Sales__Order(context)
    assert order.current_dataframe == "persisted"
    assert order.empty_frame() == "empty"
    assert context.calls == ["current_dataframe", "empty_frame"]


def test_a_folder_stages_through_the_context():
    context = FakeContext()
    export = Sales__Export(context)
    staged, deletes = export.read()
    assert staged == "/tmp/staging"
    assert deletes == []
    assert context.calls == ["staging_folder"]


def test_an_object_without_a_context_says_so():
    with pytest.raises(LoadError, match="constructed by Weaver"):
        Sales__Order().path


def test_read_must_be_implemented():
    class Sales__Unfinished(Table):
        pass

    with pytest.raises(NotImplementedError, match="must implement read"):
        Sales__Unfinished(FakeContext()).read()


# --- the module stays light -------------------------------------------------


def test_the_authoring_module_imports_without_spark():
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c",
         "import sys, weaver.objects; print('pyspark' in sys.modules)"],
        capture_output=True, text=True, check=True,
    )
    assert result.stdout.strip() == "False"


def test_the_base_classes_are_registered_by_kind():
    from weaver.objects import BASE_CLASSES, BASE_CLASS_NAMES

    assert BASE_CLASSES == {"Folder": Folder, "Table": Table, "View": View}
    assert BASE_CLASS_NAMES == {"Folder", "Table", "View"}


def test_every_authored_object_shares_one_base():
    assert issubclass(Folder, WeaverObject)
    assert issubclass(Table, WeaverObject)
    assert issubclass(View, WeaverObject)
