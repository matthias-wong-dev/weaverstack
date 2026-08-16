"""Two absences a bootstrap must tell apart.

A catalogue that is not there yet can be missing in two different ways, and they
call for opposite responses:

.. code-block:: text

    no Warehouse at all       provision one
    a Warehouse without `_`   build into it

Conflating them is how a shared catalogue host goes wrong. If "no `_` tables"
were read as "no Warehouse", initialisation would try to create a Warehouse that
exists — and if a missing Warehouse were read as an empty catalogue, the first
build would fail somewhere far from the cause.
"""

from __future__ import annotations

import pytest

from weaver.catalogue.tables import REGISTRY
from weaver.errors import CommandError
from weaver.initialise import prepare_catalogue
from weaver.workspaces import Workspace

WORKSPACE = Workspace(workspace="Demo", catalogue="Warehouse/Weaver")


class _Workspace:
    id = "ws-1"
    name = "Demo"


@pytest.fixture
def fabric(monkeypatch):
    """Fabric's item lookup and Warehouse creation, recorded rather than made."""

    from weaver.fabric import resources

    created: list[str] = []
    present: set[str] = set()

    def find_workspace(name, *, client=None):
        return _Workspace()

    def find_item(workspace, name, *, item_type, client=None):
        if name in present:
            return object()
        raise resources.ItemNotFoundError(f"{item_type} {name!r} was not found")

    def create_warehouse(workspace, name, *, client=None):
        created.append(name)
        present.add(name)
        return object()

    monkeypatch.setattr(resources, "find_workspace", find_workspace)
    monkeypatch.setattr(resources, "find_item", find_item)
    monkeypatch.setattr(resources, "create_warehouse", create_warehouse)
    return type("Fabric", (), {"created": created, "present": present})


def test_a_missing_warehouse_is_provisioned(fabric):
    prepared = prepare_catalogue(WORKSPACE)

    assert prepared.created is True
    assert fabric.created == ["Weaver"]


def test_an_existing_warehouse_is_used_rather_than_refused(fabric):
    """A Warehouse already holding a user's schemas is an ordinary host.

    Weaver owns `_` there and nothing else, so finding one is the shared-host
    arrangement working rather than a collision to report.
    """

    fabric.present.add("Weaver")

    prepared = prepare_catalogue(WORKSPACE)

    assert prepared.created is False
    assert fabric.created == []


def test_preparing_twice_provisions_once(fabric):
    first = prepare_catalogue(WORKSPACE)
    second = prepare_catalogue(WORKSPACE)

    assert first.created is True
    assert second.created is False
    assert fabric.created == ["Weaver"]


def test_a_workspace_naming_no_catalogue_says_so(fabric):
    with pytest.raises(CommandError, match="configured Weaver catalogue"):
        prepare_catalogue(Workspace(workspace="Demo"))


# --- the other absence: a Warehouse whose `_` is not built yet ----------------


class _Empty:
    """A catalogue connection to a Warehouse that holds no `_` at all."""

    def shape(self):
        return {}

    def columns_of(self, table):
        return None

    def rows(self, statement):
        raise AssertionError("nothing should be read from a table that is absent")


def test_an_absent_table_reads_as_no_rows_rather_than_a_failure():
    """Bootstrap: the build that writes the catalogue is the build that creates it."""

    from weaver.catalogue.reader import read_table

    assert read_table(_Empty(), REGISTRY) == ()


def test_an_empty_catalogue_is_not_a_missing_warehouse():
    """The distinction, stated as the two answers a caller gets.

    `prepare_catalogue` says whether the *Warehouse* had to be made. Whether its
    `_` tables are there is a different question, answered by reading them — and
    a Warehouse that exists with no `_` gives "not created" and "no rows".
    """

    from weaver.catalogue.reader import read_table

    assert _Empty().columns_of(REGISTRY) is None
    assert read_table(_Empty(), REGISTRY) == ()
